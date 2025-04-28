"""
Processes plant data by merging TEC and IC registers with their respective mapping files,
and cleans/sorts the data by computing new capacity columns based on specific rules.

For TEC register:
- Adds a new column "MW_Capacity" based on:
    • If Project Status is "Built":
          - If year(MW Effective From) > YEAR_OF_ANALYSIS then MW_Capacity = MW Connected.
          - Else, MW_Capacity = Cumulative Total Capacity (MW).
    • If Project Status is not "Built":
          - If year(MW Effective From) > YEAR_OF_ANALYSIS then MW_Capacity = 0.
          - Else:
              ○ If Stage is blank, MW_Capacity = Cumulative Total Capacity (MW).
              ○ Else, MW_Capacity = MW Increase / Decrease.

For IC register:
- Adds new columns "MW_Import_Capacity" and "MW_Export_Capacity" based on:
    • If year(MW Effective From) > YEAR_OF_ANALYSIS then both capacities = 0.
    • Else (year ≤ YEAR_OF_ANALYSIS):
          ○ If "Stage" is blank or equals 1 then:
                MW_Import_Capacity = MW Import - Total
                MW_Export_Capacity = MW Export - Total
          ○ Else (i.e. Stage is not blank and not equal to 1) then:
                MW_Import_Capacity = MW Import - Increase / Decrease
                MW_Export_Capacity = MW Export - Increase / Decrease

Additional functionality:
- Imports network node data from network_data.py (from the all_nodes_df DataFrame).
- Adds a new column "ETYS_Node" to both TEC and IC registers.
- Populates "ETYS_Node" as follows:
    1) If "Node_Name" is not blank and an exact match exists in the network node column "Node",
       then "ETYS_Node" is set to that matching value.
    2) If no exact match is found, but the first 5 characters of "Node_Name" match the first 5 characters
       of a "Node", then "ETYS_Node" is set to that full node name (using only the first match).

Optionally, each DataFrame is sorted by "Asset Type" if that column exists.
"""

import pandas as pd
import logging
import sys
from typing import Dict
from src.config import (
    TEC_REGISTER_FILE_PATH,
    TEC_REGISTER_MAPPING_FILE_PATH,
    IC_REGISTER_FILE_PATH,
    IC_REGISTER_MAPPING_FILE_PATH,
    PLANT_OUTPUT_FILE_PATH,
    YEAR_OF_ANALYSIS,
    SELECTED_TAGS,
    GEN_CAPACITY_FOR_TRANSMISSION
)

# Import the network data function to retrieve node information.
from src.data_processing.network_data import get_network_data

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load a CSV file into a DataFrame.
    :param file_path: Path to the CSV file.
    :return: DataFrame containing the file data.
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {file_path} with {len(df)} rows.")
        return df
    except Exception as e:
        logger.exception(f"Error loading CSV file: {file_path}")
        return pd.DataFrame()


def merge_mapping_with_register(register_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge a register DataFrame with its corresponding mapping DataFrame on "Project Number",
    ensuring only the "Node_Name" column from the mapping file is added.
    :param register_df: The main register DataFrame.
    :param mapping_df: The mapping DataFrame.
    :return: Merged DataFrame with "Node_Name" column added.
    """
    logger.info(f"Register Columns: {register_df.columns.tolist()}")
    logger.info(f"Mapping Columns: {mapping_df.columns.tolist()}")

    if "Project Number" not in register_df.columns or "Project Number" not in mapping_df.columns:
        logger.warning("❌ 'Project Number' column missing in one of the datasets. Exiting.")
        sys.exit()

    if "Node_Name" not in mapping_df.columns:
        logger.warning("❌ 'Node_Name' column missing in mapping dataset. Exiting.")
        sys.exit()

    merged_df = register_df.merge(
        mapping_df[["Project Number", "Node_Name"]],
        on="Project Number",
        how="left"
    )

    logger.info(f"✅ Merged register with mapping file. Final row count: {len(merged_df)}")
    return merged_df

def filter_by_selected_regions(df: pd.DataFrame, df_name: str = "DataFrame") -> pd.DataFrame:
    """
    Filters the provided DataFrame to include rows where 'HOST TO' is in the SELECTED_TAGS list,
    always including 'OFTO' entries by default.

    :param df: The DataFrame to filter.
    :return: The filtered DataFrame.
    """
    if "HOST TO" not in df.columns:
        logger.warning("'HOST TO' column not found in {df_name}. No filtering applied.")
        return df

    tags_to_include = SELECTED_TAGS.union({"OFTO"})

    filtered_df = df[df["HOST TO"].isin(tags_to_include)].copy()
    logger.info(f"Filtering {df_name}. 'HOST TO' options include: {sorted(df['HOST TO'].unique())}. Dataframe of {len(df)} rows to {len(filtered_df)} rows based on SELECTED_TAGS: {SELECTED_TAGS} + 'OFTO' (by default)")
    return filtered_df

def clean_register_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and sorts the TEC register DataFrame by adding the MW_Capacity column
    based on specific rules.

    Rules for TEC register:
    - If Project Status is "Built":
         • If year(MW Effective From) > YEAR_OF_ANALYSIS, then MW_Capacity = MW Connected.
         • Else, MW_Capacity = Cumulative Total Capacity (MW).
    - If Project Status is not "Built":
         • If year(MW Effective From) > YEAR_OF_ANALYSIS, then MW_Capacity = 0.
         • Else:
              ○ If Stage is blank, MW_Capacity = Cumulative Total Capacity (MW).
              ○ Else, MW_Capacity = MW Increase / Decrease.
    - If "Asset Type" exists, the DataFrame is sorted by this column.
    :param df: DataFrame to be cleaned.
    :return: Cleaned and sorted DataFrame.
    """
    if "MW Effective From" in df.columns:
        df["MW Effective From"] = pd.to_datetime(df["MW Effective From"], errors="coerce")
    else:
        logger.warning("'MW Effective From' column not found in DataFrame. Exiting.")
        sys.exit()

    def compute_mw_capacity(row):
        effective_year = row["MW Effective From"].year if pd.notnull(row.get("MW Effective From")) else None
        status = row.get("Project Status", "")
        stage = row.get("Stage", "")

        if status == "Built":
            if effective_year is not None and effective_year > YEAR_OF_ANALYSIS:
                return row.get("MW Connected", None)
            else:
                return row.get("Cumulative Total Capacity (MW)", None)
        else:  # For projects not Built
            if effective_year is not None and effective_year > YEAR_OF_ANALYSIS:
                return 0
            else:
                if pd.isna(stage) or stage == "":
                    return row.get("Cumulative Total Capacity (MW)", None)
                else:
                    return row.get("MW Increase / Decrease", None)

    df["MW_Capacity"] = df.apply(compute_mw_capacity, axis=1)

    # Optionally sort by "Project Name"
    if "Project Name" in df.columns:
        df.sort_values(by=["Project Name"], inplace=True)

    return df


def clean_ic_register_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and sorts the IC register DataFrame by adding new columns for MW Import and Export capacities.

    Rules for IC register:
    - Adds columns "MW_Import_Capacity" and "MW_Export_Capacity".
    - If year(MW Effective From) > YEAR_OF_ANALYSIS then both capacities = 0.
    - Else (year ≤ YEAR_OF_ANALYSIS):
         • If "Stage" is blank or equals 1 then:
               MW_Import_Capacity = MW Import - Total
               MW_Export_Capacity = MW Export - Total
         • Else (i.e. Stage is not blank and not equal to 1) then:
               MW_Import_Capacity = MW Import - Increase / Decrease
               MW_Export_Capacity = MW Export - Increase / Decrease.
    - If "Asset Type" exists, the DataFrame is sorted by this column.
    :param df: DataFrame to be cleaned.
    :return: Cleaned and sorted DataFrame.
    """
    if "MW Effective From" in df.columns:
        df["MW Effective From"] = pd.to_datetime(df["MW Effective From"], errors="coerce")
    else:
        logger.warning("'MW Effective From' column not found in DataFrame.")

    def compute_ic_capacities(row):
        effective_year = row["MW Effective From"].year if pd.notnull(row.get("MW Effective From")) else None
        stage = row.get("Stage", "")
        # If effective year is available and is greater than YEAR_OF_ANALYSIS, return zeros.
        if effective_year is not None and effective_year > YEAR_OF_ANALYSIS:
            return 0, 0
        else:
            # When effective year is within the analysis or not provided, use Stage to decide:
            # If Stage is blank or equals 1, use the total capacities;
            # otherwise, use the increase/decrease values.
            if pd.isna(stage) or stage == "" or stage == 1 or stage == "1":
                return row.get("MW Import - Total", None), row.get("MW Export - Total", None)
            else:
                return row.get("MW Import - Increase / Decrease", None), row.get("MW Export - Increase / Decrease", None)

    capacities = df.apply(
        lambda row: pd.Series(compute_ic_capacities(row), index=["MW_Import_Capacity", "MW_Export_Capacity"]),
        axis=1
    )
    df = pd.concat([df, capacities], axis=1)

    # Optionally sort by "Asset Type" if the column exists
    if "Asset Type" in df.columns:
        df.sort_values(by=["Asset Type"], inplace=True)

    return df


def add_etys_node(df: pd.DataFrame, nodes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a new column 'ETYS_Node' to the provided DataFrame based on the 'Node_Name' column.

    For each row:
    1) If 'Node_Name' is not blank and an exact match exists in nodes_df['Node'], assign that value.
    2) Otherwise, if the first 5 characters of 'Node_Name' match the first 5 characters
       of any node in nodes_df, assign that full node name (first match only).
    3) If still no match is found, check if the first 4 characters match and assign the corresponding node.

    :param df: The register DataFrame (TEC or IC) with a 'Node_Name' column.
    :param nodes_df: DataFrame containing network node data with a 'Node' column.
    :return: The updated DataFrame with an 'ETYS_Node' column.
    """

    def lookup_etys_node(row):
        node_name = row.get("Node_Name")
        if pd.isna(node_name) or node_name == "":
            return None

        # First, try exact match
        exact_matches = nodes_df[nodes_df["Node"] == node_name]
        if not exact_matches.empty:
            return exact_matches.iloc[0]["Node"]

        # If no exact match, check first 5 characters
        node_name_prefix5 = node_name[:5]
        partial_matches5 = nodes_df[nodes_df["Node"].str[:5] == node_name_prefix5]
        if not partial_matches5.empty:
            return partial_matches5.iloc[0]["Node"]

        # If still no match, check first 4 characters
        node_name_prefix4 = node_name[:4]
        partial_matches4 = nodes_df[nodes_df["Node"].str[:4] == node_name_prefix4]
        if not partial_matches4.empty:
            # Apply advanced selection based on 5th character and capacity
            capacity = max(
                row.get("MW_Capacity", 0) or 0,
                row.get("MW_Import_Capacity", 0) or 0,
                row.get("MW_Export_Capacity", 0) or 0
            )

            # Add a column extracting the 5th character as an integer
            partial_matches4 = partial_matches4.copy()
            partial_matches4["5th_digit"] = partial_matches4["Node"].str[4].astype(str)

            if capacity > GEN_CAPACITY_FOR_TRANSMISSION:
                # Prefer 5th digit = 2 or 4
                first_node = partial_matches4.iloc[0]["Node"]
                first_node_5th_digit = first_node[4]

                if first_node_5th_digit in ["2", "4"]:
                    return first_node
                else:
                    preferred = partial_matches4[partial_matches4["5th_digit"].isin(["2", "4"])]
                    if not preferred.empty:
                        return preferred.iloc[0]["Node"]
                    else:
                        return first_node
            else:
                # Prefer NOT 2 or 4 if capacity <= GEN_CAPACITY_FOR_TRANSMISSION
                first_node = partial_matches4.iloc[0]["Node"]
                first_node_5th_digit = first_node[4]

                if first_node_5th_digit not in ["2", "4"]:
                    return first_node
                else:
                    non_preferred = partial_matches4[~partial_matches4["5th_digit"].isin(["2", "4"])]
                    if not non_preferred.empty:
                        return non_preferred.iloc[0]["Node"]
                    else:
                        return first_node

        # No matches at all
        logger.warning(
            f"⚠️ No ETYS Node match found for Project '{row.get('Project Name', 'Unknown')}', "
            f"Node_Name '{node_name}'"
        )
        return None

    df["ETYS_Node"] = df.apply(lookup_etys_node, axis=1)

    # Check if the 5th digit is problematic for high capacity (>GEN_CAPACITY_FOR_TRANSMISSION)
    for idx, row in df.iterrows():
        etys_node = row.get("ETYS_Node")
        if pd.isna(etys_node) or len(str(etys_node)) < 5:
            continue  # skip if ETYS_Node is missing or less than 5 digits

        fifth_digit = str(etys_node)[4]
        capacity = max(
            row.get("MW_Capacity", 0) or 0,
            row.get("MW_Import_Capacity", 0) or 0,
            row.get("MW_Export_Capacity", 0) or 0
        )

        if capacity > GEN_CAPACITY_FOR_TRANSMISSION and fifth_digit not in ["2", "4"]:
            logger.warning(
                f"⚠️ High-capacity project (>{GEN_CAPACITY_FOR_TRANSMISSION}MW) '{row.get('Project Name', 'Unknown')}' "
                f"assigned to node '{etys_node}' with max capacity={capacity} with 5th digit '{fifth_digit}' not 2 or 4 i.e. not 275 or 400kV."
            )

    return df
    # add code to:
    # 2) sort the partial matches such that the highest voltage is picked based on the criteria unless explicitly defined


def process_plant_data() -> Dict[str, pd.DataFrame]:
    """
    Process plant data by merging TEC and IC registers with their respective mapping files,
    cleaning the data (computing capacity columns), adding the ETYS_Node column, and filtering by selected tags.

    :return: Dictionary containing the processed TEC and IC register DataFrames.
    """
    logger.info("Processing plant data...")

    # Load TEC and IC registers and their mappings.
    tec_register_df = load_csv(TEC_REGISTER_FILE_PATH)
    tec_mapping_df = load_csv(TEC_REGISTER_MAPPING_FILE_PATH)
    ic_register_df = load_csv(IC_REGISTER_FILE_PATH)
    ic_mapping_df = load_csv(IC_REGISTER_MAPPING_FILE_PATH)

    # Merge Node_Name into registers.
    tec_merged = merge_mapping_with_register(tec_register_df, tec_mapping_df)
    ic_merged = merge_mapping_with_register(ic_register_df, ic_mapping_df)

    # Filter by SELECTED_TAGS
    tec_merged = filter_by_selected_regions(tec_merged, df_name="TEC Register")
    ic_merged = filter_by_selected_regions(ic_merged, df_name="IC Register")

    # Clean registers (compute MW capacity columns).
    tec_merged = clean_register_data(tec_merged)
    ic_merged = clean_ic_register_data(ic_merged)

    # Retrieve network node data from network_data.py.
    nodes_df = get_network_data().get("all_nodes_df", pd.DataFrame())

    if nodes_df.empty:
        logger.warning("Network node data is empty. 'ETYS_Node' column will not be populated.")
    else:
        # Add the ETYS_Node column to both TEC and IC registers.
        tec_merged = add_etys_node(tec_merged, nodes_df)
        ic_merged = add_etys_node(ic_merged, nodes_df)

    return {"tec_register": tec_merged, "ic_register": ic_merged}



def main() -> None:
    """
    Main function to process plant data and save the output.
    """
    try:
        data = process_plant_data()

        # Save output to an Excel file with separate sheets for TEC and IC registers.
        with pd.ExcelWriter(PLANT_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
            data["tec_register"].to_excel(writer, sheet_name="TEC Register", index=False)
            data["ic_register"].to_excel(writer, sheet_name="IC Register", index=False)

        logger.info(f"Plant data processing complete. Output saved to {PLANT_OUTPUT_FILE_PATH}")

    except Exception as e:
        logger.exception("An error occurred during plant data processing.")


if __name__ == "__main__":
    main()
