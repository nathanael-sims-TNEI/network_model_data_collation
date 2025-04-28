import os
from datetime import datetime

# ---------------------------
# Network Model Data Collation Configuration
# ---------------------------

YEAR_OF_ANALYSIS = 2050
FES_SCENARIO = "HT"
# Applies to demand only
# "HT" = Holistic Transition, "HE" = Hydrogen Evolution, "EE" = Electric Engagement
CONSIDER_DEMAND_TYPES = ["R", "E", "C", "I", "H", "D", "T", "Z"]
# "R" = Residential, "E" = Electric Vehicles, "C" = Commercial, "I" = Industrial, "H" = Heat pumps, "D" = District heat, "T" = Transmission direct connects, "Z" = Electrolysers
SELECTED_TAGS = {'NGET'}
# 'SHET', 'SPT', 'NGET', 'OFTO'
# Note: 'OFTO' should be selected ONLY if 'SHET', 'SPT' and 'NGET' are also selected, to avoid isolated OFTO nodes.
IGNORE_DER = 1 # YET TO CONFIGURE?
# 1 = YES, 0 = NO


# ---------------------------
# Other Settings
# ---------------------------

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))
date_str = datetime.now().strftime("%d-%m-%Y")

ETYSB_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/etys_appendix_b_2024.xlsx")
COORDINATES_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/substation_coordinates.csv")
TEC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_04feb2025.csv")
IC_REGISTER_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/interconnector_register_04feb2025.csv")
TEC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/tec_register_mapping.csv")
IC_REGISTER_MAPPING_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/ic_register_mapping.csv")
DEMAND_FILE_PATH = os.path.join(PROJECT_DIR, "input_data/fes_2024_active_power_demand_data.csv")

NETWORK_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, f"output_data/NODE_NETWORK_DATA_{date_str}.xlsx")
PLANT_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, f"output_data/PLANT_DATA_{date_str}.xlsx")
DEMAND_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, f"output_data/DEMAND_DATA_{date_str}.xlsx")
HVDC_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, f"output_data/INTRA_HVDC_{date_str}.xlsx")
FULL_GRID_OUTPUT_FILE_PATH = os.path.join(PROJECT_DIR, f"output_data/FULL_GRID_{date_str}.xlsx")

SHEET_ASSOCIATIONS = {"a": "SHET", "b": "SPT", "c": "NGET", "d": "OFTO", "1": "All"}

