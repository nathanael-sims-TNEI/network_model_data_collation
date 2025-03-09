import os

PROJECT_DIR = os.path.dirname(__file__)

ETYSB_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/etys_appendix_b_2024.xlsx")
COORDINATES_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/substation_coordinates.csv")
TEC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_04feb2025.csv")
IC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/interconnector_register_04feb2025.csv")
TEC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_mapping.csv")
IC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/ic_register_mapping.csv")

PLANT_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Plant_Data.xlsx")
NETWORK_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Node_and_Network_Data.xlsx")

SHEET_ASSOCIATIONS = {"a": "SHET", "b": "SPT", "c": "NGET", "d": "OFTO"}

# ---------------------------
# Network Model Data Collation Configuration
# ---------------------------

YEAR_OF_ANALYSIS = 2030
SELECTED_TAGS = {"SHET", "SPT", "NGET"}
# 'SHET', 'SPT', 'NGET', 'OFTO'
