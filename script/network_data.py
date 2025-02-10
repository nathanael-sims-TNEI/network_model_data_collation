import os
import pandas as pd
import logging
from config import (
    ETYSB_FILE_PATH,
    COORDINATES_FILE_PATH,
    SHEET_ASSOCIATIONS,
    SELECTED_TAGS,
    YEAR_OF_ANALYSIS,
    NETWORK_OUTPUT_FILE_PATH
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants for sheet names
INDEX_SHEETS = ["B-1-1a", "B-1-1b", "B-1-1c", "B-1-1d"]

CIRCUIT_SHEETS = [
    "B-2-1a", "B-2-1b", "B-2-1c", "B-2-1d",
    "B-2-2a", "B-2-2b", "B-2-2c", "B-2-2d",
]
TRANSFORMER_SHEETS = [
    "B-3-1a", "B-3-1b", "B-3-1c", "B-3-1d",
    "B-3-2a", "B-3-2b", "B-3-2c", "B-3-2d",
]
REACTIVE_SHEETS = [
    "B-4-1a", "B-4-1b", "B-4-1c", "B-4-1d",
    "B-4-2a", "B-4-2b", "B-4-2c", "B-4-2d",
]

NETWORK_DATA_SHEETS = CIRCUIT_SHEETS + TRANSFORMER_SHEETS + REACTIVE_SHEETS

COLUMN_RENAME_MAP = {
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

VOLTAGE_MAPPING = {
    "1": "132",
    "2": "275",
    "3": "33",
    "4": "400",
    "5": "11",
    "6": "66",
    "7": "25",
    "8": "22",
}


def parse_all_sheets(file_path: str, rename_map: dict) -> dict:
    """
    Load and parse all sheets from an Excel file.
    Each sheet is read with header=1, columns are stripped of extra spaces,
    and renamed using the provided map.
    """
    logger.info("Loading and parsing Excel file...")
    try:
        xls = pd.ExcelFile(file_path)
        sheets_dict = {}
        for sheet_name in xls.sheet_names:
            logger.info(f"Parsing sheet: {sheet_name}")
            df = xls.parse(sheet_name, header=1)
            df.columns = df.columns.astype(str).str.strip()
            logger.info(f"Columns in '{sheet_name}': {df.columns.tolist()}")
            df.rename(columns=rename_map, inplace=True)
            sheets_dict[sheet_name] = df
        return sheets_dict
    except Exception as e:
        logger.exception(f"Error parsing sheets from {file_path}")
        return {}


def filter_relevant_sheets_data(all_data: dict, associations: dict, tags: list) -> dict:
    """
    Filter sheets based on whether the last character of the sheet name is in associations
    and if the corresponding value in associations is in the provided tags.
    """
    return {
        sheet_name: data
        for sheet_name, data in all_data.items()
        if sheet_name[-1] in associations and associations[sheet_name[-1]] in tags
    }


def concatenate_sheets(sheet_list: list, sheets_data: dict) -> pd.DataFrame:
    """
    Concatenate sheets from a given list that exist in sheets_data.
    Adds a 'Sheet_Name' column to indicate the origin of each row.
    """
    dfs = [
        sheets_data[sheet].assign(Sheet_Name=sheet)
        for sheet in sheet_list if sheet in sheets_data
    ]
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def concatenate_and_process_sheets(sheets_data: dict):
    """
    Concatenate circuit, transformer, and reactive sheets.
    """
    logger.info("Concatenating and processing sheets.")
    try:
        circuit_df = concatenate_sheets(CIRCUIT_SHEETS, sheets_data)
        logger.info("Circuit sheets concatenated.")
        transformer_df = concatenate_sheets(TRANSFORMER_SHEETS, sheets_data)
        logger.info("Transformer sheets concatenated.")
        reactive_df = concatenate_sheets(REACTIVE_SHEETS, sheets_data)
        logger.info("Reactive sheets concatenated.")
        return circuit_df, transformer_df, reactive_df
    except Exception as e:
        logger.exception("Error during sheet concatenation")
        raise


def filter_data_based_on_status_and_year(df: pd.DataFrame, year: int, is_reactive: bool = False) -> pd.DataFrame:
    """
    Filter data rows based on the 'Status' and 'Year' columns.
    - For missing Status or Year, the row is included by default.
    - 'Addition' rows are added if their Year is less than or equal to the target year.
    - 'Remove' rows cause existing matching rows to be removed.
    - 'Change' rows remove matching rows and then add the new row.
    """
    logger.info("Filtering data based on status and year.")
    filtered_rows = []
    for _, row in df.iterrows():
        status = row.get("Status")
        row_year = row.get("Year")
        if pd.isna(status) or pd.isna(row_year):
            filtered_rows.append(row)
            continue
        if row_year > year:
            continue
        if status == "Addition":
            filtered_rows.append(row)
        elif status == "Remove":
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


def split_data_by_type(df: pd.DataFrame, column: str) -> dict:
    """
    Split a DataFrame into a dictionary of DataFrames based on the unique values in a given column.
    """
    if column in df.columns:
        unique_values = df[column].dropna().unique()
        return {val: df[df[column] == val] for val in unique_values}
    return {}


def derive_voltage(node_name: str) -> str:
    """
    Derive a voltage from the node name.
    If the node name is at least 5 characters long and the 5th character is a digit,
    return the voltage from VOLTAGE_MAPPING. Otherwise, return 'Unknown'.
    """
    if len(node_name) >= 5 and node_name[4].isdigit():
        return VOLTAGE_MAPPING.get(node_name[4], "Unknown")
    return "Unknown"


def compile_node_info(*dfs: pd.DataFrame) -> pd.DataFrame:
    """
    Compile a unique sorted list of nodes from the provided dataframes,
    along with associated sheet names and corresponding Relevant TO names.
    Searches for node columns: "Node 1", "Node 2", and "Node" in each dataframe (which must also include "Sheet_Name").
    Returns a DataFrame with the following columns:
      - Node
      - Voltage (Derived)
      - Sheet Names: Comma-separated list of sheets where the node appears.
      - Relevant TO: Comma-separated list derived from the last character of each sheet name (using SHEET_ASSOCIATIONS).
    """
    node_info = {}  # Mapping: node -> set of sheet names
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
    # Build the output list
    data = []
    for node, sheets in node_info.items():
        sheet_list = sorted(sheets)
        # Derive the Relevant TO names based on the last character of each sheet name
        relevant_to_set = {SHEET_ASSOCIATIONS.get(s[-1], "Unknown") for s in sheet_list if s}
        data.append({
            "Node": node,
            "Voltage (Derived)": derive_voltage(node),
            "Sheet Names": ", ".join(sheet_list),
            "Relevant TO": ", ".join(sorted(relevant_to_set))
        })
    # Also include nodes that might not have associated sheet info
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


def compile_site_name_mapping(all_sheets_data: dict, index_sheets: list) -> dict:
    """
    Compile a mapping of Site Code to Site Name from the index sheets.
    Assumes that each index sheet contains columns "Site Code" and "Site Name".
    """
    mapping = {}
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


def add_coordinates_and_site_name_to_nodes(nodes_df: pd.DataFrame, coordinates_file: str,
                                           site_name_mapping: dict) -> pd.DataFrame:
    """
    Reads the coordinates CSV file and merges it with the nodes DataFrame.
    For each node, the first 4 characters (assumed to be the Site Code) are extracted and matched
    with the "Site Code" column in the coordinates file to add latitude and longitude.
    Additionally, using the provided site_name_mapping, a "Site Name" column is added.
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


# ========================
# NEW FUNCTION: get_network_data()
# ========================
def get_network_data():
    """
    Process the input Excel file and return the filtered and compiled network data as a dictionary.
    Returns a dictionary containing:
        - 'circuit_data_filtered'
        - 'transformer_data_filtered'
        - 'reactive_data_filtered'
        - 'filtered_dataframes': a dict of split dataframes for output
        - 'all_nodes_df': the compiled nodes DataFrame (with voltage, coordinates, site name, etc.)
    """
    # Parse all sheets from the Excel file
    all_sheets_data = parse_all_sheets(ETYSB_FILE_PATH, COLUMN_RENAME_MAP)
    # Build a mapping from Site Code to Site Name using the index sheets
    site_name_mapping = compile_site_name_mapping(all_sheets_data, INDEX_SHEETS)
    # Filter sheets based on associations and tags
    relevant_sheets_data = filter_relevant_sheets_data(all_sheets_data, SHEET_ASSOCIATIONS, SELECTED_TAGS)
    if not relevant_sheets_data:
        raise ValueError("No relevant sheets found.")

    # Concatenate and filter the data
    circuit_data, transformer_data, reactive_data = concatenate_and_process_sheets(relevant_sheets_data)
    circuit_data_filtered = filter_data_based_on_status_and_year(circuit_data, YEAR_OF_ANALYSIS)
    transformer_data_filtered = filter_data_based_on_status_and_year(transformer_data, YEAR_OF_ANALYSIS)
    reactive_data_filtered = filter_data_based_on_status_and_year(reactive_data, YEAR_OF_ANALYSIS, is_reactive=True)

    # Split filtered data by type for output (if needed)
    filtered_dataframes = {}
    filtered_dataframes.update(split_data_by_type(circuit_data_filtered, "Circuit Type"))
    filtered_dataframes.update(split_data_by_type(transformer_data_filtered, "Transformer Type"))
    filtered_dataframes.update(split_data_by_type(reactive_data_filtered, "Compensation Type"))

    # Compile node info and merge with coordinates and site names
    all_nodes_df = compile_node_info(circuit_data_filtered, transformer_data_filtered, reactive_data_filtered)
    all_nodes_df = add_coordinates_and_site_name_to_nodes(all_nodes_df, COORDINATES_FILE_PATH, site_name_mapping)

    return {
        'circuit_data_filtered': circuit_data_filtered,
        'transformer_data_filtered': transformer_data_filtered,
        'reactive_data_filtered': reactive_data_filtered,
        'filtered_dataframes': filtered_dataframes,
        'all_nodes_df': all_nodes_df
    }


def main():
    logger.info("Starting sheet processing.")
    try:
        data = get_network_data()
        # Write output to an Excel file
        with pd.ExcelWriter(NETWORK_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
            # Write the Nodes sheet first
            nodes_sheet_name = "Nodes"
            data['all_nodes_df'].to_excel(writer, sheet_name=nodes_sheet_name, index=False)
            logger.info(f"Saved sheet: {nodes_sheet_name}")
            # Write the other filtered dataframes
            for sheet_name, df in data['filtered_dataframes'].items():
                safe_sheet_name = sheet_name[:31].replace("/", "_").replace("\\", "_")
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                logger.info(f"Saved sheet: {safe_sheet_name}")
        logger.info(f"Processing complete. Data saved to {NETWORK_OUTPUT_FILE_PATH}")
    except Exception as e:
        logger.exception("An error occurred during processing.")


if __name__ == '__main__':
    main()
