import pandas as pd
import logging
from src.config import YEAR_OF_ANALYSIS, ETYSB_FILE_PATH

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def filter_by_planned_year(df: pd.DataFrame, target_year: int) -> pd.DataFrame:
    if "Planned from year" not in df.columns:
        logger.warning("'Planned from year' column not found. Skipping year-based filtering.")
        return df

    df["Planned from year"] = df["Planned from year"].astype(str)
    df["Year"] = pd.to_numeric(df["Planned from year"], errors="coerce")

    condition_existing = df["Planned from year"].str.lower() == "existing"
    condition_numeric = df["Year"].notna() & (df["Year"] <= target_year)

    before_filter = len(df)
    df = df[condition_existing | condition_numeric].copy()
    after_filter = len(df)
    logger.info(
        f"Filtered out {before_filter - after_filter} rows based on 'Planned from year' > {target_year} or not 'Existing'.")

    df["Status"] = df["Year"].apply(lambda x: "Addition" if pd.notnull(x) else "Existing")
    return df


def process_intra_hvdc_data() -> pd.DataFrame:
    """
    Processes the Intra HVDC data by reading the specified sheet,
    filtering rows, and adding 'Year' and 'Status' columns.
    Returns the processed DataFrame.
    """
    try:
        logger.info(f"Reading sheet 'B-5-1' from {ETYSB_FILE_PATH}...")
        df = pd.read_excel(ETYSB_FILE_PATH, sheet_name="B-5-1", header=1)
        df.columns = df.columns.astype(str).str.strip()
        df = filter_by_planned_year(df, YEAR_OF_ANALYSIS)
        logger.info("Successfully processed Intra HVDC data.")
        return df
    except Exception as e:
        logger.exception("An error occurred during the INTRA HVDC processing.")
        return pd.DataFrame()


# Retain the existing main if you still want to run this module standalone.
if __name__ == "__main__":
    processed_df = process_intra_hvdc_data()
    # If desired, you can save the output here.
