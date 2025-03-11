import os

PROJECT_DIR = os.path.dirname(__file__)

ETYSB_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/etys_appendix_b_2024.xlsx")
COORDINATES_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/substation_coordinates.csv")
TEC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_04feb2025.csv")
IC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/interconnector_register_04feb2025.csv")
TEC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_mapping.csv")
IC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/ic_register_mapping.csv")
DEMAND_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/fes_2024_active_power_demand_data.csv")

NETWORK_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Node_and_Network_Data.xlsx")
PLANT_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Plant_Data.xlsx")
DEMAND_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/Demand_Data.xlsx")
FULL_GRID_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, "output_data/FULL_GRID.xlsx")

SHEET_ASSOCIATIONS = {"a": "SHET", "b": "SPT", "c": "NGET", "d": "OFTO"}

# ---------------------------
# Network Model Data Collation Configuration
# ---------------------------

YEAR_OF_ANALYSIS = 2030
FES_SCENARIO = "HT"
# Applies to demand only
# "HT" = Holistic Transition, "HE" = Hydrogen Evolution, "EE" = Electric Engagement
CONSIDER_DEMAND_TYPES = ["R", "E", "C", "I", "H", "D", "T", "Z"]
# "R" = Residential, "E" = Electric Vehicles, "C" = Commercial, "I" = Industrial, "H" = Heat pumps, "D" = District heat, "T" = Transmission direct connects, "Z" = Electrolysers
SELECTED_TAGS = {"SHET", "SPT", "NGET"}
# 'SHET', 'SPT', 'NGET', 'OFTO'
IGNORE_DER = 1
# 1 = YES, 0 = NO
