import Networkx as Network
import pandas as pd
import streamlit as st

def build_graph(node, link):
    """Build a knowledge graph HTML visualization for a given lineage node.

    Parameters
    ----------
    node : str
        Name of an input table, pipeline, or report selected by the user.
    link : str
        URL associated with a report or pipeline, used to resolve the node
        if the raw name is not found directly in the lineage file.

    Returns
    -------
    str
        "success" if the HTML graph was built, otherwise "failure".
    """

    # Spin up the PyVis network that will render the knowledge graph UI.
    # Dark background helps the colored nodes pop.
    kg = Network(
        height="600px",
        width="100%",
        neighborhood_highlight=True,
        bgcolor="#000000",
        cdn_resources="remote",
        select_menu=False,
        filter_menu=False,
    )

    def create_knowledge_graph(df: pd.DataFrame) -> nx.DiGraph:
        """Construct a directed graph from lineage dataframe.

        Edges are added for:
        - Input_Layer -> Pipeline_Layer
        - Pipeline_Layer -> Output_Layer
        - Input_Layer -> Report_Name
        """
        G = nx.DiGraph()
        for _, row in df.iterrows():
            if row["Input_Layer"] != "nan" and row["Pipeline_Layer"] != "nan":
                G.add_edge(row["Input_Layer"], row["Pipeline_Layer"])
            if row["Pipeline_Layer"] != "nan" and row["Output_Layer"] != "nan":
                G.add_edge(row["Pipeline_Layer"], row["Output_Layer"])
            if row["Input_Layer"] != "nan" and row["Report_Name"] != "nan":
                G.add_edge(row["Input_Layer"], row["Report_Name"])
        return G

    def find_data_sources(G: nx.DiGraph, center_node: str):
        """Return all upstream (ancestors) and downstream (descendants) nodes."""
        sources_in = list(nx.ancestors(G, center_node))
        sources_out = list(nx.descendants(G, center_node))
        return sources_in, sources_out

    # Pull in the lineage CSV and clean up pipeline/report labels so they read nicely.
    df = pd.read_csv("files/Lineage.csv").astype(str)
    for index, row in df.iterrows():
        if row["Pipeline_Layer"] not in ["nan", ""]:
            df.at[index, "Pipeline_Layer"] = (
                f"{row['Pipeline_Layer']} ({row['Pipeline_Layer_Platform']})"
            )
        elif row["Report_Name"] not in ["nan", ""]:
            df.at[index, "Report_Name"] = (
                f"{row['Report_Name']} ({row['Report_Platform']})"
            )

    # If this node isn't an input table, try to resolve it using the report or pipeline link.
    if node not in df["Input_Layer"].unique().tolist():
        if link in df["Report_Link"].unique().tolist():
            temp_df = df[df["Report_Link"] == link].reset_index(drop=True)
            node = temp_df.at[0, "Report_Name"]
        elif link in df["Pipeline_Layer_Link"].unique().tolist():
            temp_df = df[df["Pipeline_Layer_Link"] == link].reset_index(drop=True)
            node = temp_df.at[0, "Pipeline_Layer"]

    # If we can't find this node anywhere in the lineage, just bail out early.
    if (
        node not in df["Input_Layer"].unique().tolist()
        and node not in df["Pipeline_Layer"].unique().tolist()
        and node not in df["Report_Name"].unique().tolist()
        and node not in df["Output_Layer"].unique().tolist()
    ):
        return "failure"

    # Build the graph and grab everything upstream and downstream of the selected node.
    G = create_knowledge_graph(df)
    sources_in, sources_out = find_data_sources(G, node.strip())

    # Treat the center node plus all its ancestors/descendants as the interesting slice of the graph.
    relevant_nodes = [node.strip()] + sources_in + sources_out

    # Normalize everything to strings and drop any placeholder "nan" values.
    relevant_nodes = [str(item) for item in relevant_nodes if str(item) != "nan"]

    # Track which PyVis nodes and edges we've already created so we don't duplicate them.
    unique_nodes = {}
    unique_pairs = []

    counter = 1
    for _, row in df.iterrows():
        input_layer = row["Input_Layer"]
        pipeline_layer = row["Pipeline_Layer"]
        output_layer = row["Output_Layer"]
        report_name = row["Report_Name"]

        # CASE 1: Input table wired directly to a report.
        if input_layer in relevant_nodes and report_name in relevant_nodes:
            if input_layer not in unique_nodes:
                counter += 1
                if input_layer == node:
                    kg.add_node(
                        counter,
                        title=f"{input_layer} ({row['Input_Layer_Platform']})",
                        uo_name=input_layer,
                        borderWidth=10,
                        size=40,
                    )
                else:
                    kg.add_node(
                        counter,
                        title=f"{input_layer} ({row['Input_Layer_Platform']})",
                        uo_name=input_layer,
                        borderWidth=0,
                        size=40,
                    )
                unique_nodes[input_layer] = counter

            if report_name not in unique_nodes:
                counter += 1
                if report_name == node:
                    kg.add_node(
                        counter,
                        title=f"{report_name}",
                        shape="square",
                        size=80,
                        color="#FFA500",
                        uo_name=report_name,
                        borderWidth=10,
                    )
                else:
                    kg.add_node(
                        counter,
                        title=f"{report_name}",
                        shape="square",
                        size=80,
                        color="#FFA500",
                        uo_name=report_name,
                        borderWidth=0,
                    )
                unique_nodes[report_name] = counter

            pair = [unique_nodes[input_layer], unique_nodes[report_name]]
            if pair not in unique_pairs:
                kg.add_edge(
                    unique_nodes[input_layer],
                    unique_nodes[report_name],
                    arrows="To",
                    strikeThrough=False,
                    width=20,
                )
                unique_pairs.append(pair)

        # CASE 2: Full chain: Input -> Pipeline -> Output.
        elif (
            input_layer in relevant_nodes
            and pipeline_layer in relevant_nodes
            and output_layer in relevant_nodes
        ):
            if input_layer not in unique_nodes:
                counter += 1
                if input_layer == node:
                    kg.add_node(
                        counter,
                        title=f"{input_layer} ({row['Input_Layer_Platform']})",
                        uo_name=input_layer,
                        borderWidth=10,
                        size=40,
                    )
                else:
                    kg.add_node(
                        counter,
                        title=f"{input_layer} ({row['Input_Layer_Platform']})",
                        uo_name=input_layer,
                        borderWidth=0,
                        size=40,
                    )
                unique_nodes[input_layer] = counter

            if pipeline_layer not in unique_nodes:
                counter += 1
                if pipeline_layer == node:
                    kg.add_node(
                        counter,
                        title=f"{pipeline_layer}",
                        shape="triangle",
                        size=70,
                        color="#FFFF00",
                        uo_name=pipeline_layer,
                        borderWidth=10,
                    )
                else:
                    kg.add_node(
                        counter,
                        title=f"{pipeline_layer}",
                        shape="triangle",
                        size=70,
                        color="#FFFF00",
                        uo_name=pipeline_layer,
                        borderWidth=0,
                    )
                unique_nodes[pipeline_layer] = counter

            if output_layer not in unique_nodes:
                counter += 1
                if output_layer == node:
                    kg.add_node(
                        counter,
                        title=f"{output_layer}",
                        uo_name=output_layer,
                        borderWidth=10,
                        size=40,
                    )
                else:
                    kg.add_node(
                        counter,
                        title=f"{output_layer}",
                        uo_name=output_layer,
                        borderWidth=0,
                        size=40,
                    )
                unique_nodes[output_layer] = counter

            pair = [unique_nodes[input_layer], unique_nodes[pipeline_layer]]
            if pair not in unique_pairs:
                kg.add_edge(
                    unique_nodes[input_layer],
                    unique_nodes[pipeline_layer],
                    arrows="To",
                    strikeThrough=False,
                    width=20,
                )
                unique_pairs.append(pair)

            pair = [unique_nodes[pipeline_layer], unique_nodes[output_layer]]
            if pair not in unique_pairs:
                kg.add_edge(
                    unique_nodes[pipeline_layer],
                    unique_nodes[output_layer],
                    arrows="To",
                    strikeThrough=False,
                    width=20,
                )
                unique_pairs.append(pair)

    # Save the selected node as the current title so the rest of the app can reuse it.
    temp_df = pd.read_csv("files/title.csv")
    temp_df.at[0, "title"] = node
    temp_df.to_csv("files/title.csv", index=False)

    # Reset the Streamlit session buckets the UI uses to show upstream/downstream lists.
    st.session_state["reports_sourced"] = []
    st.session_state["pipelines_used"] = []
    st.session_state["pipelines_sourced"] = []
    st.session_state["tables_used"] = []
    st.session_state["tables_sourced"] = []

    # Strip out any "nan" noise from the upstream/downstream lists.
    sources_in = [str(item) for item in sources_in if str(item) != "nan"]
    sources_out = [str(item) for item in sources_out if str(item) != "nan"]

    # Figure out which lineage rows directly involve the selected node.
    if node in df["Input_Layer"].unique().tolist():
        temp_df = df[df["Input_Layer"] == node]
    elif node in df["Pipeline_Layer"].unique().tolist():
        temp_df = df[df["Pipeline_Layer"] == node]
    elif node in df["Report_Name"].unique().tolist():
        temp_df = df[df["Report_Name"] == node]
    else:
        temp_df = df.copy()

    # Fill in the "used" side (everything this node depends on).
    for item in sources_in:
        # If it shows up in this filtered view, we treat it as a direct dependency.
        if temp_df.isin([item]).any().any():
            signal = "direct"
        else:
            signal = "indirect"

        if item in df["Input_Layer"].unique().tolist() or item in df["Output_Layer"].unique().tolist():
            st.session_state["tables_used"].append({"name": item, "direct": signal})
        if item in df["Pipeline_Layer"].unique().tolist():
            st.session_state["pipelines_used"].append({"name": item, "direct": signal})

    # Fill in the "sourced" side (who's downstream of this node).
    for item in sources_out:
        if temp_df.isin([item]).any().any():
            signal = "direct"
        else:
            signal = "indirect"

        if item in df["Input_Layer"].unique().tolist() or item in df["Output_Layer"].unique().tolist():
            st.session_state["tables_sourced"].append({"name": item, "direct": signal})
        if item in df["Pipeline_Layer"].unique().tolist():
            st.session_state["pipelines_sourced"].append({"name": item, "direct": signal})
        if item in df["Report_Name"].unique().tolist():
            st.session_state["reports_sourced"].append({"name": item, "direct": signal})

    # Tune the physics/layout and write the knowledge graph out as HTML.
    kg.barnes_hut(
        gravity=-50000,
        central_gravity=0.1,
        spring_length=250,
        spring_strength=0.001,
        damping=0.09,
        overlap=0,
    )
    kg.show("kg2.html", notebook=False)

    return "success"