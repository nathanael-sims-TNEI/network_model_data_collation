import pandas as pd
import logging
from config import YEAR_OF_ANALYSIS, HVDC_OUTPUT_FILE_PATH, ETYSB_FILE_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def filter_by_planned_year(df: pd.DataFrame, target_year: int) -> pd.DataFrame:
    """
    1) Creates a 'Year' column:
       - If 'Planned from year' can be converted to a number, that number is stored in 'Year'.
       - If 'Planned from year' equals "Existing" (case-insensitive), 'Year' becomes NaN.
    2) Filters rows, keeping those where:
       - 'Planned from year' equals "Existing", OR
       - 'Year' is numeric and <= target_year.
    3) Adds a 'Status' column which is set to "Addition" if the 'Year' column contains a number,
       otherwise it is set to "Existing".
    4) Logs how many rows were filtered out.
    """
    if "Planned from year" not in df.columns:
        logger.warning("'Planned from year' column not found. Skipping year-based filtering.")
        return df

    # Convert 'Planned from year' to string to ensure consistent checking.
    df["Planned from year"] = df["Planned from year"].astype(str)

    # Create a new "Year" column: numeric value where possible; non-numeric becomes NaN.
    df["Year"] = pd.to_numeric(df["Planned from year"], errors="coerce")

    # Identify rows where 'Planned from year' is "Existing"
    condition_existing = df["Planned from year"].str.lower() == "existing"

    # For numeric rows, keep only if the year is less than or equal to the target year.
    condition_numeric = df["Year"].notna() & (df["Year"] <= target_year)

    before_filter = len(df)
    df = df[condition_existing | condition_numeric].copy()  # Create a copy to avoid SettingWithCopyWarning.
    after_filter = len(df)
    logger.info(f"Filtered out {before_filter - after_filter} rows based on 'Planned from year' > {target_year} or not 'Existing'.")

    # Add the 'Status' column: "Addition" if a numeric year exists, otherwise "Existing".
    df["Status"] = df["Year"].apply(lambda x: "Addition" if pd.notnull(x) else "Existing")

    return df

def main() -> None:
    """
    Main processing function for the INTRA HVDC sheet.

    Reads sheet 'B-5-1' from the Excel file defined in ETYSB_FILE_PATH,
    processes the data by filtering rows based on the 'Planned from year' column,
    adds the 'Year' and 'Status' columns, and writes the output to HVDC_OUTPUT_FILE_PATH.
    """
    try:
        logger.info(f"Reading sheet 'B-5-1' from {ETYSB_FILE_PATH}...")
        df = pd.read_excel(ETYSB_FILE_PATH, sheet_name="B-5-1", header=1)
        df.columns = df.columns.astype(str).str.strip()  # Clean column names

        # Filter rows and add 'Year' and 'Status' columns.
        df = filter_by_planned_year(df, YEAR_OF_ANALYSIS)

        # Save the processed output into an Excel file.
        logger.info(f"Saving processed data to {HVDC_OUTPUT_FILE_PATH}...")
        with pd.ExcelWriter(HVDC_OUTPUT_FILE_PATH, engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name="Intra_HVDC", index=False)

        logger.info("Processing complete.")

    except Exception as e:
        logger.exception("An error occurred during the INTRA HVDC processing.")

if __name__ == "__main__":
    main()
