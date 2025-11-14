"""Build and refresh lineage for BigQuery scheduled queries.

We read the existing lineage and report files, parse the BigQuery transfer
config JSON, pull out input/output tables from the SQL, and write the updated
lineage back out.
"""

import json
import re
from datetime import datetime
from typing import List, Tuple

import pandas as pd

from mlutils import dataset  # Walmart Package

# Handy CLI command to download BigQuery transfer configs as pretty-printed JSON.

cli_command = r'''
cd {BQ_Directory}
bq ls --transfer_config --project_id={project_id} --transfer_location=US --format=prettyjson > bq_download.json
'''

def extract_lineage(sql_query: str) -> Tuple[List[str], List[str]]:
    """Given a BigQuery SQL string, pull out input and output table names.

    We strip out lines that start with `--`, then use a couple of regexes to
    find tables after FROM/JOIN clauses (inputs) and CREATE/REPLACE TABLE
    statements (outputs).

    We only keep fully-qualified tables that start with our internal dataset
    prefix (for example: '{WM_Dataset Idenfifier}.project.dataset').
    """
    # Flatten the query into one string and drop any full-line `--` comments.
    query_lines = sql_query.splitlines()
    cleaned_query = ""
    for line in query_lines:
        # Ignore lines that are just SQL comments.
        if not line.strip().startswith("--"):
            cleaned_query += f" {line} "

    input_tables: List[str] = []
    output_tables: List[str] = []

    # Regex patterns to find table references in the query.
    input_pattern = r"(FROM|JOIN|INNER JOIN|LEFT JOIN|RIGHT JOIN|FULL OUTER JOIN|CROSS JOIN)\s+(\S+)"
    output_pattern = r"(CREATE TABLE|REPLACE TABLE|CREATE OR REPLACE TABLE)\s+(\S+)"

    input_matches = re.findall(input_pattern, cleaned_query, re.IGNORECASE)
    output_matches = re.findall(output_pattern, cleaned_query, re.IGNORECASE)

    # Clean up and filter the input tables we found.
    for match in input_matches:
        table = (
            match[1]
            .strip()
            .replace("'", "")
            .replace("`", "")
            .replace("(", "")
            .replace(")", "")
            .replace(";", "")
            .replace(",", "")
        )
        table_parts = table.split(".")
        if table.startswith("{WM_Dataset Idenfifier}") and len(table_parts) == 3:
            input_tables.append(table)

    # Clean up and filter the output tables we found.
    for output_match in output_matches:
        table = (
            output_match[1]
            .strip()
            .replace("'", "")
            .replace("`", "")
            .replace("(", "")
            .replace(")", "")
            .replace(";", "")
            .replace(",", "")
        )
        table_parts = table.split(".")
        if table.startswith("{WM_Dataset Idenfifier}") and len(table_parts) == 3:
            output_tables.append(table)

    # De-duplicate results but keep the return types as simple lists.
    unique_input_tables = list(set(input_tables))
    unique_output_tables = list(set(output_tables))

    return unique_input_tables, unique_output_tables

def main() -> None:
    """Read transfer configs, rebuild lineage rows, and push updates out."""
    # Start from the existing lineage file and strip out any rows for GCP
    # scheduled queries so we can rebuild them from scratch.
    df = pd.read_csv("files/Lineage.csv").astype(str)

    temp_df = df[
        df["Pipeline_Layer_Platform"].str.lower().str.strip()
        == "gcp scheduled query"
    ]
    index_list = temp_df.index.tolist()
    df = df.drop(index_list).reset_index(drop=True)

    # Keep a simple list of scheduled query names and links (nice for debugging).
    bq_queries_found: List[str] = []
    unique_bq_links: List[str] = []

    # Load the report-level metadata that goes alongside the lineage.
    report_df = pd.read_csv("files/report_df.csv").astype(str)

    # Read the transfer config JSON we pulled down from BigQuery via the CLI.
    with open("bq_download.json") as json_file:
        data = json.load(json_file)

    # The JSON export should be a list of transfer config objects.
    if isinstance(data, list):
        for item in data:
            query = item["params"]["query"]

            bq_queries_found.append(item["displayName"])

            # Build a direct link back to this scheduled query in the GCP console.
            link_string = (
                "https://console.cloud.google.com/bigquery/scheduled-queries/locations/us/configs/"
            )
            substring = item["name"].split("transferConfigs/")[-1]
            link_string += substring
            unique_bq_links.append(link_string)

            # Stamp the lineage with "last updated" as of today.
            today = datetime.today()
            formatted_date = today.strftime("%Y-%m-%d")

            # Pull the refresh cadence off the config if it exists.
            if "schedule" in item:
                refresh_cadence = item["schedule"]
            else:
                refresh_cadence = ""

            # Add or update the report-level row for this scheduled query.
            new_row = {
                "Name": item["displayName"],
                "Platform": "GCP Scheduled Query",
                "Link": link_string,
                "Refresh Cadence": refresh_cadence,
                "Modified Date": formatted_date,
            }

            new_row_df = pd.DataFrame([new_row])
            report_df = pd.concat([report_df, new_row_df], ignore_index=True)

            # Use the query SQL to figure out which tables feed into and out of it.
            input_tables, output_tables = extract_lineage(query)

            # Write one lineage row for every (input, output) combo we find.
            for input_table in input_tables:
                for output_table in output_tables:
                    lineage_row = {
                        "Input_Layer": input_table,
                        "Input_Layer_Platform": "BQ",
                        "Pipeline_Layer": item["displayName"],
                        "Pipeline_Layer_Platform": "GCP Scheduled Query",
                        "Pipeline_Layer_Link": link_string,
                        "Output_Layer": output_table,
                        "Report_Name": "",
                        "Report_Platform": "",
                        "Report_Link": "",
                        "Output_Layer_Type": "BQ",
                    }

                    lineage_row_df = pd.DataFrame([lineage_row])
                    df = pd.concat([df, lineage_row_df], ignore_index=True)

    # Save the updated lineage back to the connector and also to the CSV on disk.
    dataset.save(df, name="{Connector_Name}", table_name="oa_Lineage")
    df.to_csv("files/Lineage.csv", index=False)


if __name__ == "__main__":
    main()