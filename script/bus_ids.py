import pandas as pd
import logging
from config import ETYSB_FILE_PATH, COORDINATES_FILE_PATH, NODE_OUTPUT_FILE_PATH, SHEET_ASSOCIATIONS, SELECTED_TAGS

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

INDEX_SHEETS = ['B-1-1a', 'B-1-1b', 'B-1-1c', 'B-1-1d']

CIRCUIT_SHEETS = [
    'B-2-1a', 'B-2-1b', 'B-2-1c', 'B-2-1d',
    'B-2-2a', 'B-2-2b', 'B-2-2c', 'B-2-2d'
]

TRANSFORMER_SHEETS = [
    'B-3-1a', 'B-3-1b', 'B-3-1c', 'B-3-1d',
    'B-3-2a', 'B-3-2b', 'B-3-2c', 'B-3-2d'
]

REACTIVE_SHEETS = [
    'B-4-1a', 'B-4-1b', 'B-4-1c', 'B-4-1d',
    'B-4-2a', 'B-4-2b', 'B-4-2c', 'B-4-2d'
]

NETWORK_DATA_SHEETS = CIRCUIT_SHEETS + TRANSFORMER_SHEETS + REACTIVE_SHEETS

COLUMN_RENAME_MAP = {
    'Node1': 'Node 1',
    'Node2': 'Node 2',
    'OHL Length(km)': 'OHL Length (km)',
    'Cable Length(km)': 'Cable Length (km)',
    'Rating (MVA)': 'Winter Rating (MVA)',
    'R (% on 100 MVA)': 'R (% on 100MVA)',
    'X (% on 100 MVA)': 'X (% on 100MVA)',
    'B (% on 100 MVA)': 'B (% on 100MVA)',
    'Mvar Generation': 'MVAr Generation',
    'Mvar Absorption': 'MVAr Absorption',
    'MVar Generation': 'MVAr Generation',
    'MVar Absorption': 'MVAr Absorption'
}

VOLTAGE_MAPPING = {
    '1': '132', '2': '275', '3': '33', '4': '400',
    '5': '11', '6': '66', '7': '25', '8': '22'
}

#-----------------------------------------------------------

def parse_all_sheets(file_path, rename_map):
    logger.info("Loading and parsing Excel file...")
    try:
        xls = pd.ExcelFile(file_path)
        sheets_dict = {}
        for sheet_name in xls.sheet_names:
            logger.info(f"Parsing sheet: {sheet_name}")
            df = xls.parse(sheet_name, header=1)
            df.columns = df.columns.astype(str).str.strip()
            logger.info(f"Columns found in sheet '{sheet_name}': {df.columns.tolist()}")
            df.rename(columns=rename_map, inplace=True)
            sheets_dict[sheet_name] = df
        return sheets_dict
    except Exception as e:
        logger.error(f"Error parsing sheets from {file_path}: {e}")
        return {}

def filter_relevant_sheets_data(all_data, associations, tags):
    return {
        sheet_name: data
        for sheet_name, data in all_data.items()
        if sheet_name[-1] in associations and associations[sheet_name[-1]] in tags
    }

def get_sheet_data(sheet_name):
    df = all_sheets_data.get(sheet_name)
    if df is None or df.empty:
        logger.warning(f"Sheet '{sheet_name}' is missing or empty.")
        return pd.DataFrame()
    return df

def filter_relevant_sheets(sheets, associations, tags):
    return [
        sheet for sheet in sheets
        if sheet[-1] in associations and associations[sheet[-1]] in tags
    ]

def extract_node_data(df, sheet_association, sheet_name):
    node_columns = ['Node 1', 'Node 2']
    node_data = {}
    for col in node_columns:
        if col in df.columns:
            for node_name in df[col].dropna().astype(str).unique():
                node_name = node_name.strip()
                if node_name not in node_data:
                    node_data[node_name] = {'associations': set(), 'sheets': set()}
                node_data[node_name]['associations'].add(sheet_association)
                node_data[node_name]['sheets'].add(sheet_name)
    return node_data

def derive_voltage(node_name):
    if len(node_name) >= 5 and node_name[4].isdigit():
        return VOLTAGE_MAPPING.get(node_name[4], 'Unknown')
    return 'Unknown'

try:
    logger.info("Processing node input_data from relevant sheets...")

    all_sheets_data = parse_all_sheets(ETYSB_FILE_PATH, COLUMN_RENAME_MAP)
    relevant_sheets_data = filter_relevant_sheets_data(all_sheets_data, SHEET_ASSOCIATIONS, SELECTED_TAGS)

    if not relevant_sheets_data:
        logger.error("No relevant sheets were found.")
    else:
        logger.info("Successfully filtered relevant sheets.")

    filtered_sheets = filter_relevant_sheets(NETWORK_DATA_SHEETS, SHEET_ASSOCIATIONS, SELECTED_TAGS)
    node_data = {}

    for sheet in filtered_sheets:
        df = get_sheet_data(sheet)
        if not df.empty:
            sheet_association = SHEET_ASSOCIATIONS.get(sheet[-1], '')
            sheet_node_data = extract_node_data(df, sheet_association, sheet)
            for node, data in sheet_node_data.items():
                if node not in node_data:
                    node_data[node] = {'associations': set(), 'sheets': set()}
                node_data[node]['associations'].update(data['associations'])
                node_data[node]['sheets'].update(data['sheets'])

    node_df = pd.DataFrame([{
        'Node Name': node,
        'Relevant TO': ', '.join(sorted(node_data[node]['associations'])),
        'Sheets': ', '.join(sorted(node_data[node]['sheets']))
    } for node in node_data])
    node_df.sort_values(by='Node Name', inplace=True)

    node_df['Voltage (Derived)'] = node_df['Node Name'].apply(derive_voltage)
    node_df['Merge Key'] = node_df['Node Name'].str[:4] + '-' + node_df['Voltage (Derived)']

    merge_data = pd.DataFrame()
    for sheet in INDEX_SHEETS:
        df = get_sheet_data(sheet)
        if {'Site Code', 'Voltage (kV)', 'Site Name'}.issubset(df.columns):
            merge_data = pd.concat([merge_data, df[['Site Code', 'Voltage (kV)', 'Site Name']].dropna()], ignore_index=True)

    merge_data['Site Code'] = merge_data['Site Code'].astype(str)
    merge_data['Merge Key'] = merge_data['Site Code'].str[:4] + '-' + merge_data['Voltage (kV)'].astype(int).astype(str)

    merged_df = node_df.merge(merge_data, on='Merge Key', how='left')

    if merged_df['Site Name'].isna().any():
        no_match_df = merged_df[merged_df['Site Name'].isna()].copy()
        no_match_df['Node Prefix'] = no_match_df['Node Name'].str[:4]
        merge_data['Site Prefix'] = merge_data['Site Code'].str[:4]
        secondary_merge = no_match_df.merge(
            merge_data[['Site Prefix', 'Site Name']],
            left_on='Node Prefix',
            right_on='Site Prefix',
            how='left'
        )
        merged_df.loc[merged_df['Site Name'].isna(), 'Site Name'] = secondary_merge['Site Name_y']

    merged_df.loc[merged_df['Site Name'].isna(), 'Site Name'] = merged_df['Node Name'].str[:4] + " (Node Name used)"

    coordinates_data = pd.read_csv(COORDINATES_FILE_PATH)
    coordinates_data = coordinates_data[['Site Code', 'latitude', 'longitude']]
    coordinates_data['Site Code'] = coordinates_data['Site Code'].astype(str).str.strip()
    coordinates_data['Site Prefix'] = coordinates_data['Site Code'].str[:4]
    coordinates_data = coordinates_data.drop_duplicates(subset='Site Code', keep='first')

    merged_df['Node Prefix'] = merged_df['Node Name'].str[:4]
    merged_with_coordinates_df = merged_df.merge(
        coordinates_data[['Site Prefix', 'latitude', 'longitude']],
        left_on='Node Prefix',
        right_on='Site Prefix',
        how='left'
    )

    merged_with_coordinates_df['Full Name'] = merged_with_coordinates_df.apply(
        lambda row: f"{row['Site Name']} {row['Voltage (Derived)']}kV" if row['Site Name'] else 'Unknown', axis=1
    )

    bus_ids_df = merged_with_coordinates_df[
        ['Node Name', 'Relevant TO', 'Sheets', 'Voltage (Derived)', 'Site Name', 'latitude', 'longitude', 'Full Name']
    ]

    bus_ids_df.to_csv(NODE_OUTPUT_FILE_PATH, index=False)
    logger.info(f"Processing complete. Data saved to {NODE_OUTPUT_FILE_PATH}")

except Exception as e:
    logger.error(f"An error occurred: {e}")
