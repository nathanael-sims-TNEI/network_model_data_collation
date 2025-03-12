"""
Processes demand data by sorting the FES workbook export file based on criteria in config file.
Associates ETYS node name to each row.
"""


import os
import pandas as pd
from src import config
import logging
from network_data import get_network_data
from typing import Optional

# Configure logging per best practice.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def lookup_etys_node(gsp: Optional[str], nodes_df: pd.DataFrame) -> Optional[str]:
    """
    Look up the ETYS node based on the given GSP value.

    Returns a matching node if found (first an exact match, then based on the first 5 characters,
    and finally the first 4 characters) or None otherwise.

    :param gsp: The GSP value to look up.
    :param nodes_df: DataFrame containing network node data with a 'Node' column.
    :return: A matching node string or None.
    """
    if not gsp:
        return None

    # Exact match.
    exact_matches = nodes_df[nodes_df["Node"] == gsp]
    if not exact_matches.empty:
        return exact_matches.iloc[0]["Node"]

    # First 5 characters match.
    gsp_str = str(gsp)
    prefix5 = gsp_str[:5]
    partial_matches5 = nodes_df[nodes_df["Node"].str[:5] == prefix5]
    if not partial_matches5.empty:
        return partial_matches5.iloc[0]["Node"]

    # First 4 characters match.
    prefix4 = gsp_str[:4]
    partial_matches4 = nodes_df[nodes_df["Node"].str[:4] == prefix4]
    if not partial_matches4.empty:
        return partial_matches4.iloc[0]["Node"]

    return None


def add_etys_node_to_demand(df: pd.DataFrame, nodes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column 'ETYS_Node' to the demand DataFrame based on the 'GSP' column.

    :param df: Demand DataFrame with a 'GSP' column.
    :param nodes_df: DataFrame containing network node data with a 'Node' column.
    :return: Updated DataFrame with the 'ETYS_Node' column.
    """
    df["ETYS_Node"] = df["GSP"].apply(lambda x: lookup_etys_node(x, nodes_df))
    return df


def load_demand_data() -> pd.DataFrame:
    """
    Loads and processes the FES active power demand data.
    This includes:
      - Reading the CSV file.
      - Removing underscores from the 'GSP' column.
      - Converting the 'year' column to numeric.
      - Filtering the data based on year, scenario, and demand types.
      - Adding the ETYS_Node column using network node data.

    :return: The filtered and updated pandas DataFrame.
    """
    logger.info("Starting to load demand data.")

    # Load the CSV file.
    try:
        df = pd.read_csv(config.DEMAND_FILE_PATH)
        logger.info(f"Loaded {len(df)} rows from {config.DEMAND_FILE_PATH}.")
    except Exception as e:
        logger.exception(f"Failed to load demand data from {config.DEMAND_FILE_PATH}: {e}")
        raise FileNotFoundError(f"Error reading file at {config.DEMAND_FILE_PATH}: {e}")

    # Remove underscores from the "GSP" column if it exists.
    if "GSP" in df.columns:
        df["GSP"] = df["GSP"].str.replace("_", "", regex=False)
        logger.info("Removed underscores from the 'GSP' column.")
    else:
        logger.warning("Column 'GSP' not found in demand data.")

    # Convert YEAR_OF_ANALYSIS to two-digit representation.
    year_two_digits = int(str(config.YEAR_OF_ANALYSIS)[-2:])
    logger.info(f"YEAR_OF_ANALYSIS {config.YEAR_OF_ANALYSIS} converted to two digits: {year_two_digits}.")

    # Ensure 'year' column exists and is numeric.
    if "year" not in df.columns:
        logger.error("Column 'year' not found in demand data.")
        raise KeyError("Column 'year' not found in demand data.")
    df["year"] = pd.to_numeric(df["year"], errors='coerce')

    # Apply filters based on year, scenario, and demand types.
    filtered_df = df[
        (df["year"] == year_two_digits) &
        (df["scenario"] == config.FES_SCENARIO) &
        (df["type"].isin(config.CONSIDER_DEMAND_TYPES))
        ].copy()
    logger.info(f"After filtering, {len(filtered_df)} rows remain.")

    # Retrieve network node data.
    nodes_df = get_network_data().get("all_nodes_df", pd.DataFrame())
    if nodes_df.empty:
        logger.warning("No network node data available; skipping ETYS_Node population.")
    else:
        filtered_df = add_etys_node_to_demand(filtered_df, nodes_df)
        logger.info("ETYS_Node column added to demand data.")

    return filtered_df


def export_demand_data(df: pd.DataFrame, output_path: str) -> None:
    """
    Exports the processed demand data to an Excel file.

    :param df: DataFrame to export.
    :param output_path: Path to the output Excel file.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Demand Data", index=False)
        logger.info(f"Demand data exported successfully to {output_path}.")
    except Exception as e:
        logger.exception(f"Failed to export demand data to {output_path}: {e}")
        raise


def main() -> None:
    """
    Main function to process and export FES active power demand data.
    """
    logger.info("Beginning demand data processing.")
    try:
        data = load_demand_data()
        export_demand_data(data, config.DEMAND_OUTPUT_FILE_PATH)
        logger.info("Demand data processing complete.")
    except Exception as e:
        logger.exception("An error occurred during demand data processing.")


if __name__ == "__main__":
    main()
