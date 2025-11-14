"""Utilities for classifying driver survey comments into high-level categories.

This module loads survey comments from BigQuery, sends them to an LLM for
categorization, and persists progress incrementally so long-running jobs can
be resumed safely.
"""

import ast
import os
import time
from typing import Iterable, List, Tuple

import pandas as pd

from mlutils import dataset  # Walmart Package

# NOTE: Real implementation stripped out here on purpose to keep company details private.
def chat_gpt(chat_input):
    placeholder = 0
    # Basic LLM request with auth headers, etc.

# Master list of the high-level categories we want to map comments into.
allowed_categories = {
    "Cleanliness",
    "Yard",
    "Wait Time",
    "Operations",
    "Gate",
    "Other",
}

def chunked_iterable(iterable: Iterable, size: int) -> Iterable[List]:
    """Yield lists of at most `size` items from `iterable`.

    This is used to break large datasets into smaller batches for LLM calls.
    """
    items = list(iterable)
    for i in range(0, len(items), size):
        # Hand back a chunk of up to `size` items at a time.
        yield items[i : i + size]

def safe_parse_category_list(raw: str, expected_len: int) -> List[str]:
    """Safely parse an LLM response into a list of category strings.

    The LLM is expected to return something like:
        ['Cleanliness', 'Gate', 'Other']

    This helper:
    - Uses ast.literal_eval for safety.
    - Normalizes and validates each item against allowed_categories.
    - Falls back to 'Other' when parsing fails or lengths mismatch.
    """
    try:
        parsed = ast.literal_eval(raw)
        if not isinstance(parsed, list):
            return ["Other"] * expected_len
    except Exception:
        # If parsing blows up, just default the whole batch to "Other".
        return ["Other"] * expected_len

    normalized: List[str] = []
    for item in parsed:
        # Work with a clean, trimmed string version of whatever the model gave us.
        try:
            s = str(item).strip()
        except Exception:
            s = "Other"

        matched = None
        # First pass: try an exact (case-insensitive) match to one of our categories.
        for cat in allowed_categories:
            if s.lower() == cat.lower():
                matched = cat
                break

        # If that fails, fall back to some light keyword heuristics.
        if matched is None:
            s_norm = s.lower()
            if "clean" in s_norm:
                matched = "Cleanliness"
            elif "yard" in s_norm:
                matched = "Yard"
            elif "wait" in s_norm or "waiting" in s_norm:
                matched = "Wait Time"
            elif "gate" in s_norm:
                matched = "Gate"
            elif "oper" in s_norm:
                matched = "Operations"
            else:
                matched = "Other"

        normalized.append(matched)

    # Make sure the list length lines up with how many rows we expect.
    if len(normalized) < expected_len:
        normalized.extend(["Other"] * (expected_len - len(normalized)))
    elif len(normalized) > expected_len:
        normalized = normalized[:expected_len]

    return normalized

def classify_descriptions_df(
    df: pd.DataFrame,
    desc_col: str = "description",
    id_col: str = "event_id",
    batch_size: int = 100,
    save_path: str = "nlp_classifications_progress.csv",
) -> pd.DataFrame:
    """Classify survey descriptions into categories using an LLM.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe with at least `id_col` and `desc_col`.
    desc_col : str
        Column name containing the free-text survey comments.
    id_col : str
        Column name containing a unique identifier for each comment.
    batch_size : int
        Number of comments to send to the LLM in a single request.
    save_path : str
        CSV path where incremental and final progress is written.

    Returns
    -------
    pd.DataFrame
        A dataframe with columns [event_id, nlp_category].
    """

    # Pull out (event_id, description) pairs so we can batch them up.
    rows: List[Tuple] = list(df[[id_col, desc_col]].itertuples(index=False, name=None))
    batches = list(chunked_iterable(rows, batch_size))
    total_batches = len(batches)

    print(
        f"Starting Classification: {len(df)} rows, "
        f"{total_batches} batches of up to {batch_size} items each"
    )

    results: List[Tuple[str, str]] = []

    for batch_idx, batch in enumerate(batches, start=1):
        start_time = time.time()

        event_ids = [row[0] for row in batch]
        descriptions = [row[1] for row in batch]

        print(f"\nProcessing batch {batch_idx}/{total_batches} ({len(batch)} comments)")

        # Build a numbered list of comments so the prompt/order stays obvious.
        numbered_comments = "\n".join(
            f"{i + 1}. {text}" for i, text in enumerate(descriptions)
        )

        chat_prompt = f"""
Given the following survey comments from drivers as they stop at stores:

{numbered_comments}

For each comment, assign it to one of the following categories:
Cleanliness, Yard, Wait Time, Operations, Gate, or Other.
If there is nothing in the comment of substance, the default should be Other.

Please respond with a Python list of the categories, in the same order.
Example: ['Cleanliness', 'Gate', 'Other']

Include nothing else besides this list.
"""

        try:
            raw_response = chat_gpt(chat_prompt)
            print(
                f"Batch {batch_idx}: LLM response received "
                f"({len(raw_response)}) chars"
            )
        except Exception as e:
            # If the LLM call fails, treat this whole batch as "Other".
            print(f"LLM call failed for batch {batch_idx}: {e}")
            categories = ["Other"] * len(event_ids)
            results.extend(zip(event_ids, categories))
            continue

        # Parse whatever the model returned and normalize it into our category set.
        categories = safe_parse_category_list(
            raw_response, expected_len=len(event_ids)
        )
        if len(categories) != len(event_ids):
            print(
                f"Length mismatch in batch {batch_idx}: "
                f"got {len(categories)}, expected {len(event_ids)}"
            )

        # Append new results and write out progress so we can resume if needed.
        results.extend(zip(event_ids, categories))

        output_df = pd.DataFrame(results, columns=["event_id", "nlp_category"])
        output_df.to_csv(save_path, index=False)
        print(
            f"Progress saved to {save_path}. "
            f"{len(output_df)} rows completed so far"
        )

        elapsed = time.time() - start_time
        print(f"Completed batch {batch_idx}/{total_batches} in {elapsed:.2f}s")

    print("All batches complete; building final dataframe")

    # Reindex back to the original df order and plug gaps with "Other".
    output_df = pd.DataFrame(results, columns=["event_id", "nlp_category"])
    output_df = output_df.set_index("event_id").reindex(df[id_col]).reset_index()
    output_df["nlp_category"] = output_df["nlp_category"].fillna("Other")

    output_df.to_csv(save_path, index=False)
    print(f"Final DF built and saved to {save_path}")

    return output_df[["event_id", "nlp_category"]]

if __name__ == "__main__":
    # Grab the source survey data from BigQuery via the internal connector.
    df = dataset.load(
        name="{BQ_Connector_Name}",
        query='''

select distinct event_id, description 
from {survey_table_name}
where event_id not in
(
    select distinct event_id from {updated_nlp_table_name}                                  
)
and description is not null
and trim(upper(description)) <> 'NO_COMMENTS_PROVIDED'
and answers.label is null

''',
    )

    # Run the classifier and hang on to the resulting dataframe.
    out = classify_descriptions_df(
        df,
        desc_col="description",
        id_col="event_id",
        batch_size=100,
    )