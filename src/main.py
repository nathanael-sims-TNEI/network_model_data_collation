"""
This script combines the data outputs from:
 - network_data
 - plant_data
 - load_data
 - intra_hvdc_data
into a single output, ready for feeding into a power system model
"""

import os
import pandas as pd
from src import config

from src.data_processing.load_data import load_demand_data
from src.data_processing.network_data import get_network_data
from src.data_processing.plant_data import process_plant_data
from src.data_processing.intra_hvdc import process_intra_hvdc_data  # New import

def combine_outputs():
    demand_df = load_demand_data()

    network_data_dict = get_network_data()
    network_nodes_df = network_data_dict.get('all_nodes_df', pd.DataFrame())
    network_filtered = network_data_dict.get('filtered_dataframes', {})

    plant_data_dict = process_plant_data()
    tec_register_df = plant_data_dict.get('tec_register', pd.DataFrame())
    ic_register_df = plant_data_dict.get('ic_register', pd.DataFrame())

    intra_hvdc_df = process_intra_hvdc_data()

    # Create directory if it does not exist.
    os.makedirs(os.path.dirname(config.FULL_GRID_OUTPUT_FILE_PATH), exist_ok=True)

    # Write all outputs to a single Excel file with multiple sheets.
    with pd.ExcelWriter(config.FULL_GRID_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
        # Write network data: nodes sheet.
        if not network_nodes_df.empty:
            network_nodes_df.to_excel(writer, sheet_name="Nodes", index=False)

        # Write additional network filtered data sheets.
        for sheet_name, df in network_filtered.items():
            safe_sheet_name = sheet_name[:31].replace("/", "_").replace("\\", "_")
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

        # Write plant data sheets: TEC Register and IC Register.
        tec_register_df.to_excel(writer, sheet_name="TEC Register", index=False)
        ic_register_df.to_excel(writer, sheet_name="IC Register", index=False)

        # Write demand data.
        demand_df.to_excel(writer, sheet_name="Demand Data", index=False)

        # Write intra HVDC data.
        if not intra_hvdc_df.empty:
            intra_hvdc_df.to_excel(writer, sheet_name="Intra_HVDC", index=False)

    print(f"Combined output successfully saved to {config.FULL_GRID_OUTPUT_FILE_PATH}")

if __name__ == "__main__":
    combine_outputs()
