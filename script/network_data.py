import pandas as pd
import logging
from bus_ids import relevant_sheets_data, CIRCUIT_SHEETS, TRANSFORMER_SHEETS, REACTIVE_SHEETS, bus_ids_df
from config import NETWORK_OUTPUT_FILE_PATH, YEAR_OF_ANALYSIS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Modified function to handle both adding the sheet name and processing circuit input_data conditions
def concatenate_and_process_sheets(relevant_sheets_data):
    logger.info("Starting concatenation and processing of sheets.")
    try:
        # Concatenate circuit sheets and add the sheet name column
        circuit_df = pd.concat(
            [relevant_sheets_data[sheet].assign(Sheet_Name=sheet) for sheet in CIRCUIT_SHEETS if
             sheet in relevant_sheets_data],
            ignore_index=True
        )
        logger.info("Circuit sheets concatenated successfully.")

        # Concatenate transformer sheets and add the sheet name column
        transformer_df = pd.concat(
            [relevant_sheets_data[sheet].assign(Sheet_Name=sheet) for sheet in TRANSFORMER_SHEETS if
             sheet in relevant_sheets_data],
            ignore_index=True
        )
        logger.info("Transformer sheets concatenated successfully.")

        # Concatenate reactive sheets and add the sheet name column
        reactive_df = pd.concat(
            [relevant_sheets_data[sheet].assign(Sheet_Name=sheet) for sheet in REACTIVE_SHEETS if
             sheet in relevant_sheets_data],
            ignore_index=True
        )
        logger.info("Reactive sheets concatenated successfully.")

        return circuit_df, transformer_df, reactive_df
    except Exception as e:
        logger.error(f"Error during sheet concatenation: {e}")
        raise


# Function to filter input_data based on status and year criteria
def filter_data_based_on_status_and_year(df, year, is_reactive=False):
    logger.info("Starting input_data filtering based on status and year.")
    filtered_df = pd.DataFrame(columns=df.columns)

    try:
        # Iterate through each row to handle conditions for Additions, Removals, and Changes
        for index, row in df.iterrows():
            status = row.get('Status')
            row_year = row.get('Year')

            # Determine node columns based on input_data type
            if is_reactive:
                node = row['Node']
            else:
                node_1, node_2 = row['Node 1'], row['Node 2']

            # Include rows with missing Year or Status by default unless removal conditions apply
            if pd.isna(status) or pd.isna(row_year):
                filtered_df = pd.concat([filtered_df, pd.DataFrame([row])], ignore_index=True)
                continue

            if status == 'Addition' and row_year <= year:
                # Include the row if it's a new addition before or during the given year
                filtered_df = pd.concat([filtered_df, pd.DataFrame([row])], ignore_index=True)

            elif status == 'Remove' and row_year <= year:
                # For removal, search and remove any existing matching row
                if is_reactive:
                    filtered_df = filtered_df[filtered_df['Node'] != node]
                else:
                    filtered_df = filtered_df[~((filtered_df['Node 1'] == node_1) & (filtered_df['Node 2'] == node_2))]

            elif status == 'Change' and row_year <= year:
                # For changes, remove existing matching row and add the change row
                if is_reactive:
                    filtered_df = filtered_df[filtered_df['Node'] != node]
                else:
                    filtered_df = filtered_df[~((filtered_df['Node 1'] == node_1) & (filtered_df['Node 2'] == node_2))]
                filtered_df = pd.concat([filtered_df, pd.DataFrame([row])], ignore_index=True)

        logger.info("Data filtering completed successfully.")
    except Exception as e:
        logger.error(f"Error during input_data filtering: {e}")
        raise

    return filtered_df


logger.info("Starting sheet processing.")

try:
    circuit_data, transformer_data, reactive_data = concatenate_and_process_sheets(relevant_sheets_data)

    # Apply filtering based on the year for circuit, transformer, and reactive input_data
    circuit_data_filtered = filter_data_based_on_status_and_year(circuit_data, YEAR_OF_ANALYSIS)
    transformer_data_filtered = filter_data_based_on_status_and_year(transformer_data, YEAR_OF_ANALYSIS)
    reactive_data_filtered = filter_data_based_on_status_and_year(reactive_data, YEAR_OF_ANALYSIS, is_reactive=True)

    # Split filtered input_data by type for output
    filtered_dataframes = {}

    if 'Circuit Type' in circuit_data_filtered.columns:
        filtered_dataframes.update({
            circuit_type: circuit_data_filtered[circuit_data_filtered['Circuit Type'] == circuit_type]
            for circuit_type in circuit_data_filtered['Circuit Type'].unique()
        })

    if 'Transformer Type' in transformer_data_filtered.columns:
        filtered_dataframes.update({
            transformer_type: transformer_data_filtered[
                transformer_data_filtered['Transformer Type'] == transformer_type]
            for transformer_type in transformer_data_filtered['Transformer Type'].unique()
        })

    if 'Compensation Type' in reactive_data_filtered.columns:
        filtered_dataframes.update({
            reactive_type: reactive_data_filtered[reactive_data_filtered['Compensation Type'] == reactive_type]
            for reactive_type in reactive_data_filtered['Compensation Type'].unique()
        })

    with pd.ExcelWriter(NETWORK_OUTPUT_FILE_PATH, engine='xlsxwriter') as writer:
        bus_ids_df.to_excel(writer, sheet_name='Nodes', index=False)
        logger.info("Saved Nodes sheet.")

        for sheet_name, df in filtered_dataframes.items():
            safe_sheet_name = sheet_name[:31].replace('/', '_').replace('\\', '_')
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            logger.info(f"Saved sheet: {safe_sheet_name}")

    logger.info(f"Processing complete. Data saved to {NETWORK_OUTPUT_FILE_PATH}")


except Exception as e:
    logger.error(f"An error occurred during processing: {e}")
