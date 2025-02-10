import os

PROJECT_DIR = os.path.dirname(__file__)

ETYSB_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/etys_appendix_b_2024.xlsx")
COORDINATES_FILE_PATH = os.path.join(
    PROJECT_DIR, "input_data/substation_coordinates.csv"
)

NETWORK_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Node_and_Network_Data.xlsx")

SHEET_ASSOCIATIONS = {"a": "SHET", "b": "SPT", "c": "NGET", "d": "OFTO"}

# ---------------------------
# Network Model Data Collation Configuration
# ---------------------------

YEAR_OF_ANALYSIS = 2028
SELECTED_TAGS = {"SHET", "SPT", "NGET"}
# 'SHET', 'SPT', 'NGET', 'OFTO'
