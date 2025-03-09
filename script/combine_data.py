import os
import pandas as pd
import config

# Import the processing functions from each script.
from load_data import load_demand_data
from network_data import get_network_data
from plant_data import process_plant_data


def combine_outputs():
    # Load demand data (from load_data.py)
    demand_df = load_demand_data()

    # Get network data (from network_data.py)
    network_data_dict = get_network_data()
    network_nodes_df = network_data_dict.get('all_nodes_df', pd.DataFrame())
    network_filtered = network_data_dict.get('filtered_dataframes', {})

    # Process plant data (from plant_data.py)
    plant_data_dict = process_plant_data()
    tec_register_df = plant_data_dict.get('tec_register', pd.DataFrame())
    ic_register_df = plant_data_dict.get('ic_register', pd.DataFrame())

    # Ensure the directory for the output file exists.
    os.makedirs(os.path.dirname(config.FULL_GRID_OUTPUT_FILE_PATH), exist_ok=True)

    # Write all outputs to a single Excel file with multiple sheets.
    with pd.ExcelWriter(config.FULL_GRID_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
        # Write network data: nodes sheet
        if not network_nodes_df.empty:
            network_nodes_df.to_excel(writer, sheet_name="Network Nodes", index=False)

        # Write additional network filtered data sheets
        for sheet_name, df in network_filtered.items():
            # Ensure sheet name is valid for Excel (max 31 characters and no special characters)
            safe_sheet_name = sheet_name[:31].replace("/", "_").replace("\\", "_")
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)

        # Write plant data sheets: TEC Register and IC Register
        tec_register_df.to_excel(writer, sheet_name="TEC Register", index=False)
        ic_register_df.to_excel(writer, sheet_name="IC Register", index=False)

        # Write demand data
        demand_df.to_excel(writer, sheet_name="Demand Data", index=False)

    print(f"Combined output successfully saved to {config.FULL_GRID_OUTPUT_FILE_PATH}")


if __name__ == "__main__":
    combine_outputs()
