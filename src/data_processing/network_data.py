"""
Pulls network data from ETYS sheets (regions defined in config.py sheet.
Sorts and compiles network data into dataframes corresponding to asset type.
Dataframes form part of a dictionary which is exportable into single xlsx file (default)
"""

import warnings
warnings.filterwarnings("ignore", message="Cannot parse header or footer so it will be ignored")

import pandas as pd
import logging
from typing import Dict, List, Set, Any, Tuple
from src.config import (
    ETYSB_FILE_PATH,
    COORDINATES_FILE_PATH,
    SHEET_ASSOCIATIONS,
    SELECTED_TAGS,
    YEAR_OF_ANALYSIS,
    NETWORK_OUTPUT_FILE_PATH
)

# Configure logging to include timestamps, log level and message.
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================================
# Constants for sheet names and column mapping
# ============================================================================

INDEX_SHEETS: List[str] = ["B-1-1a", "B-1-1b", "B-1-1c", "B-1-1d"]

CIRCUIT_SHEETS: List[str] = [
    "B-2-1a", "B-2-1b", "B-2-1c", "B-2-1d",
    "B-2-2a", "B-2-2b", "B-2-2c", "B-2-2d",
]

TRANSFORMER_SHEETS: List[str] = [
    "B-3-1a", "B-3-1b", "B-3-1c", "B-3-1d",
    "B-3-2a", "B-3-2b", "B-3-2c", "B-3-2d",
]

REACTIVE_SHEETS: List[str] = [
    "B-4-1a", "B-4-1b", "B-4-1c", "B-4-1d",
    "B-4-2a", "B-4-2b", "B-4-2c", "B-4-2d",
]

# Combined list of all network data sheets.
NETWORK_DATA_SHEETS: List[str] = CIRCUIT_SHEETS + TRANSFORMER_SHEETS + REACTIVE_SHEETS

# Mapping to standardise column names from the Excel file.
COLUMN_RENAME_MAP: Dict[str, str] = {
    "Node1": "Node 1",
    "Node2": "Node 2",
    "OHL Length(km)": "OHL Length (km)",
    "Cable Length(km)": "Cable Length (km)",
    "Rating (MVA)": "Winter Rating (MVA)",
    "R (% on 100 MVA)": "R (% on 100MVA)",
    "X (% on 100 MVA)": "X (% on 100MVA)",
    "B (% on 100 MVA)": "B (% on 100MVA)",
    "Mvar Generation": "MVAr Generation",
    "Mvar Absorption": "MVAr Absorption",
    "MVar Generation": "MVAr Generation",
    "MVar Absorption": "MVAr Absorption",
}

# Mapping for deriving voltage based on a digit in the node name.
VOLTAGE_MAPPING: Dict[str, str] = {
    "1": "132",
    "2": "275",
    "3": "33",
    "4": "400",
    "5": "11",
    "6": "66",
    "7": "25",
    "8": "22",
}


# ============================================================================
# Data Parsing and Processing Functions
# ============================================================================

def parse_all_sheets(file_path: str, rename_map: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Load and parse all sheets from an Excel file.

    Each sheet is read with header=1, extra spaces are stripped from column names,
    and columns are renamed using the provided map.

    :param file_path: Path to the Excel file.
    :param rename_map: Dictionary mapping original column names to standardised names.
    :return: A dictionary mapping sheet names to their corresponding DataFrames.
    """
    logger.info("Loading and parsing Excel file...")
    try:
        xls = pd.ExcelFile(file_path)
        sheets_dict: Dict[str, pd.DataFrame] = {}
        for sheet_name in xls.sheet_names:
            logger.info(f"Parsing sheet: {sheet_name}")
            df = xls.parse(sheet_name, header=1)
            df.columns = df.columns.astype(str).str.strip() # Strip to ensure column names are clean strings.
            logger.info(f"Columns in '{sheet_name}': {df.columns.tolist()}")
            df.rename(columns=rename_map, inplace=True)
            sheets_dict[sheet_name] = df
        return sheets_dict
    except Exception as e:
        logger.exception(f"Error parsing sheets from {file_path}")
        return {}


def filter_relevant_sheets_data(all_data: Dict[str, pd.DataFrame],
                                associations: Dict[str, str],
                                tags: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Filter sheets based on the last character of the sheet name.

    Only sheets whose last character maps (via the given associations) to a tag in the provided list are retained.

    :param all_data: Dictionary of all sheets.
    :param associations: Mapping of sheet name suffixes to tag values.
    :param tags: List of selected tags.
    :return: Filtered dictionary of sheets.
    """
    return {
        sheet_name: data
        for sheet_name, data in all_data.items()
        if sheet_name[-1] in associations and associations[sheet_name[-1]] in tags
    }


def concatenate_sheets(sheet_list: List[str],
                       sheets_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenate sheets from a given list that exist in sheets_data.

    A 'Sheet_Name' column is added to each DataFrame to record its source.

    :param sheet_list: List of sheet names to concatenate.
    :param sheets_data: Dictionary mapping sheet names to DataFrames.
    :return: A single concatenated DataFrame.
    """
    dfs = [
        sheets_data[sheet].assign(Sheet_Name=sheet)
        for sheet in sheet_list if sheet in sheets_data
    ]
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def concatenate_and_process_sheets(sheets_data: Dict[str, pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Concatenate the circuit, transformer and reactive compensation data sheets separately.

    :param sheets_data: Dictionary of relevant sheets.
    :return: Tuple of DataFrames (circuit, transformer, reactive).
    """
    logger.info("Concatenating and processing sheets.")
    try:
        circuit_df = concatenate_sheets(CIRCUIT_SHEETS, sheets_data)
        logger.info("Circuit sheets concatenated.")
        transformer_df = concatenate_sheets(TRANSFORMER_SHEETS, sheets_data)
        transformer_df['Transformer Type'] = 'Transformer'
        logger.info("Transformer sheets concatenated.")
        reactive_df = concatenate_sheets(REACTIVE_SHEETS, sheets_data)
        logger.info("Reactive sheets concatenated.")
        return circuit_df, transformer_df, reactive_df
    except Exception as e:
        logger.exception("Error during sheet concatenation")
        raise


def filter_data_based_on_status_and_year(df: pd.DataFrame, year: int, is_reactive: bool = False) -> pd.DataFrame:
    """
    Filter rows based on 'Status' and 'Year' columns.

    - Rows with missing Status or Year are included.
    - Rows with a Year greater than the target are excluded.
    - For rows with Status "Addition", the row is kept.
    - For "Removed" rows, any matching rows already in the filtered set are removed.
    - For "Change" rows, any matching rows are removed and the new row is added.

    :param df: The DataFrame to filter.
    :param year: The target year for analysis.
    :param is_reactive: Whether the DataFrame is reactive data (affects column selection).
    :return: Filtered DataFrame.
    """
    logger.info("Filtering data based on status and year.")
    filtered_rows = []
    for _, row in df.iterrows():
        status = row.get("Status")
        row_year = row.get("Year")
        # Include row if Status or Year is missing.
        if pd.isna(status) or pd.isna(row_year):
            filtered_rows.append(row)
            continue
        # Exclude rows beyond the target year.
        if row_year > year:
            continue
        if status == "Addition":
            filtered_rows.append(row)
        elif status == "Removed":
            if is_reactive:
                filtered_rows = [r for r in filtered_rows if r.get("Node") != row.get("Node")]
            else:
                filtered_rows = [
                    r for r in filtered_rows
                    if (r.get("Node 1"), r.get("Node 2")) != (row.get("Node 1"), row.get("Node 2"))
                ]
        elif status == "Change":
            if is_reactive:
                filtered_rows = [r for r in filtered_rows if r.get("Node") != row.get("Node")]
            else:
                filtered_rows = [
                    r for r in filtered_rows
                    if (r.get("Node 1"), r.get("Node 2")) != (row.get("Node 1"), row.get("Node 2"))
                ]
            filtered_rows.append(row)
    result_df = pd.DataFrame(filtered_rows, columns=df.columns)
    logger.info("Data filtering completed.")
    return result_df


def split_data_by_type(df: pd.DataFrame, column: str) -> Dict[Any, pd.DataFrame]:
    """
    Split a DataFrame into sub-DataFrames based on the unique values in a specified column.

    :param df: The DataFrame to split.
    :param column: The column name to split by.
    :return: Dictionary mapping unique column values to DataFrames.
    """
    if column in df.columns:
        unique_values = df[column].dropna().unique()
        return {val: df[df[column] == val] for val in unique_values}
    return {}


def derive_voltage(node_name: str) -> str:
    """
    Derive a voltage (in kV) value from the node name based on the digit in the 5th character (note this may not work for OFTO sheets due to naming inconsistencies!).

    :param node_name: The node name string.
    :return: The derived voltage or 'Unknown' if not applicable.
    """
    if len(node_name) >= 5 and node_name[4].isdigit():
        return VOLTAGE_MAPPING.get(node_name[4], "Unknown")
    return "Unknown"


def compile_node_info(*dfs: pd.DataFrame) -> pd.DataFrame:
    """
    Compile a unique, sorted list of nodes from the provided DataFrames.

    For each node, also compile:
      - The derived voltage.
      - A comma-separated list of the sheet names where the node appears.
      - A comma-separated list of the "Relevant TO" values derived from the sheet names.

    :param dfs: DataFrames to compile node information from.
    :return: A DataFrame containing node info.
    """
    node_info: Dict[str, Set[str]] = {}  # Map node -> set of sheet names
    for df in dfs:
        if "Sheet_Name" not in df.columns:
            continue
        for col in ["Node 1", "Node 2", "Node"]:
            if col in df.columns:
                for _, row in df.iterrows():
                    node_val = row.get(col)
                    if pd.isna(node_val):
                        continue
                    node_val = str(node_val).strip()
                    sheet_name = row.get("Sheet_Name")
                    if not sheet_name:
                        continue
                    node_info.setdefault(node_val, set()).add(sheet_name)
    # Build output list with additional derived details.
    data = []
    for node, sheets in node_info.items():
        sheet_list = sorted(sheets)
        relevant_to_set = {SHEET_ASSOCIATIONS.get(s[-1], "Unknown") for s in sheet_list if s}
        data.append({
            "Node": node,
            "Voltage (Derived)": derive_voltage(node),
            "Sheet Names": ", ".join(sheet_list),
            "Relevant TO": ", ".join(sorted(relevant_to_set))
        })
    # Include nodes that might not have any associated sheet info.
    nodes_already = set(node_info.keys())
    for df in dfs:
        for col in ["Node 1", "Node 2", "Node"]:
            if col in df.columns:
                for node_val in df[col].dropna().unique():
                    node_val = str(node_val).strip()
                    if node_val not in nodes_already:
                        data.append({
                            "Node": node_val,
                            "Voltage (Derived)": derive_voltage(node_val),
                            "Sheet Names": "",
                            "Relevant TO": ""
                        })
                        nodes_already.add(node_val)
    nodes_df = pd.DataFrame(data)
    nodes_df = nodes_df.sort_values("Node").reset_index(drop=True)
    return nodes_df


def compile_site_name_mapping(all_sheets_data: Dict[str, pd.DataFrame],
                              index_sheets: List[str]) -> Dict[str, str]:
    """
    Compile a mapping from Site Code to Site Name using the index sheets.

    Assumes each index sheet contains "Site Code" and "Site Name" columns.

    :param all_sheets_data: Dictionary of all sheets.
    :param index_sheets: List of sheet names to use for the mapping.
    :return: Dictionary mapping site codes to site names.
    """
    mapping: Dict[str, str] = {}
    for sheet in index_sheets:
        if sheet in all_sheets_data:
            df = all_sheets_data[sheet]
            if "Site Code" in df.columns and "Site Name" in df.columns:
                for _, row in df.iterrows():
                    site_code = str(row["Site Code"]).strip()
                    site_name = row["Site Name"]
                    if site_code and site_name:
                        mapping[site_code] = site_name
    return mapping


def add_coordinates_and_site_name_to_nodes(nodes_df: pd.DataFrame,
                                           coordinates_file: str,
                                           site_name_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Merge node information with coordinates and site names.

    Extracts the first 4 characters of the node (assumed Site Code) and merges with a coordinates CSV.
    Then uses the provided site_name_mapping to add a "Site Name" column.

    :param nodes_df: DataFrame with node information.
    :param coordinates_file: Path to the CSV file containing coordinates.
    :param site_name_mapping: Mapping of site codes to site names.
    :return: Enhanced nodes DataFrame with latitude, longitude, and site names.
    """
    try:
        coords_df = pd.read_csv(coordinates_file)
        nodes_df["Site_Code"] = nodes_df["Node"].astype(str).str[:4]
        merged_df = pd.merge(
            nodes_df,
            coords_df[["Site Code", "latitude", "longitude"]],
            left_on="Site_Code",
            right_on="Site Code",
            how="left"
        )
        merged_df["Site Name"] = merged_df["Site_Code"].map(site_name_mapping)
        merged_df.drop(columns=["Site_Code", "Site Code"], inplace=True)
        return merged_df
    except Exception as e:
        logger.exception("Error merging coordinates and site names with node data")
        return nodes_df


def get_network_data() -> Dict[str, Any]:
    """
    Process the input Excel file and compile network data.

    The function returns a dictionary containing:
      - 'circuit_data_filtered'
      - 'transformer_data_filtered'
      - 'reactive_data_filtered'
      - 'filtered_dataframes': A dict of DataFrames split by type (if applicable)
      - 'all_nodes_df': A compiled DataFrame with node details (voltage, coordinates, site name, etc.)

    :return: Dictionary with the processed network data.
    """
    # Parse all sheets from the Excel file.
    all_sheets_data = parse_all_sheets(ETYSB_FILE_PATH, COLUMN_RENAME_MAP)
    # Build site name mapping using index sheets.
    site_name_mapping = compile_site_name_mapping(all_sheets_data, INDEX_SHEETS)
    # Filter sheets based on associations and selected tags.
    relevant_sheets_data = filter_relevant_sheets_data(all_sheets_data, SHEET_ASSOCIATIONS, SELECTED_TAGS)
    if not relevant_sheets_data:
        raise ValueError("No relevant sheets found.")

    # Concatenate and filter data.
    circuit_data, transformer_data, reactive_data = concatenate_and_process_sheets(relevant_sheets_data)
    circuit_data_filtered = filter_data_based_on_status_and_year(circuit_data, YEAR_OF_ANALYSIS)
    transformer_data_filtered = filter_data_based_on_status_and_year(transformer_data, YEAR_OF_ANALYSIS)
    reactive_data_filtered = filter_data_based_on_status_and_year(reactive_data, YEAR_OF_ANALYSIS, is_reactive=True)

    # Optionally, split filtered data by type for output.
    filtered_dataframes: Dict[Any, pd.DataFrame] = {}
    filtered_dataframes.update(split_data_by_type(circuit_data_filtered, "Circuit Type"))
    filtered_dataframes.update(split_data_by_type(transformer_data_filtered, "Transformer Type"))
    filtered_dataframes.update(split_data_by_type(reactive_data_filtered, "Compensation Type"))

    # Compile node information and merge with coordinates and site names.
    all_nodes_df = compile_node_info(circuit_data_filtered, transformer_data_filtered, reactive_data_filtered)
    all_nodes_df = add_coordinates_and_site_name_to_nodes(all_nodes_df, COORDINATES_FILE_PATH, site_name_mapping)

    return {
        'circuit_data_filtered': circuit_data_filtered,
        'transformer_data_filtered': transformer_data_filtered,
        'reactive_data_filtered': reactive_data_filtered,
        'filtered_dataframes': filtered_dataframes,
        'all_nodes_df': all_nodes_df
    }


def main() -> None:
    """
    Main function to process network data and write the results to an Excel file.

    The output file includes a 'Nodes' sheet and additional sheets from split filtered data.
    """
    logger.info("Starting sheet processing.")
    try:
        data = get_network_data()
        # Write output to an Excel file using xlsxwriter.
        with pd.ExcelWriter(NETWORK_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
            # Write the Nodes sheet.
            nodes_sheet_name = "Nodes"
            data['all_nodes_df'].to_excel(writer, sheet_name=nodes_sheet_name, index=False)
            logger.info(f"Saved sheet: {nodes_sheet_name}")
            # Write other filtered DataFrames to separate sheets.
            for sheet_name, df in data['filtered_dataframes'].items():
                # Ensure the sheet name is safe for Excel (max 31 characters, no invalid characters).
                safe_sheet_name = sheet_name[:31].replace("/", "_").replace("\\", "_")
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                logger.info(f"Saved sheet: {safe_sheet_name}")
        logger.info(f"Processing complete. Data saved to {NETWORK_OUTPUT_FILE_PATH}")
    except Exception as e:
        logger.exception("An error occurred during processing.")


if __name__ == '__main__':
    main()
