"""Data preparation script for the Key Lime Pie BI project.

Loads raw CSVs, applies cleaning via DataScrubber, and saves processed datasets
for downstream analytics. Handles file paths, logging, and reproducibility.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics_project.data_scrubber import DataScrubber
from src.analytics_project.utils_logger import logger

# Resolve project root safely
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    PROJECT_ROOT = Path.cwd()

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def prep_dataset(filename: str, required_columns: list[str] | None = None) -> None:
    """Prepare and clean a dataset.

    Loads a raw CSV, applies DataScrubber cleaning steps, enforces required
    columns, and saves the cleaned dataset to the processed directory.

    Args:
        filename (str): Base name of the dataset (without extension).
        required_columns (list[str] | None): Columns that must not be empty.
    """
    raw_path = RAW_DIR / f"{filename}.csv"
    output_path = PROCESSED_DIR / f"{filename}_clean.csv"

    logger.info(f"Processing {raw_path.name}...")

    if not raw_path.exists():
        logger.error(f"Raw file not found: {raw_path}")
        return

    try:
        scrubber = (
            DataScrubber.from_csv(raw_path)
            .standardize_column_names()
            .strip_whitespace()
            .drop_empty_rows()
            .drop_duplicates()
        )

        df = scrubber.df

        if required_columns:
            initial_count = len(df)
            for col in required_columns:
                df[col] = df[col].replace("", pd.NA)
                df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)
                df = df[df[col].notna()]
            removed = initial_count - len(df)
            logger.info(f"Removed {removed} rows with empty required columns: {required_columns}")

        df = df.dropna(how="all").drop_duplicates()
        df.to_csv(output_path, index=False)
        logger.info(f"Cleaned data saved to {output_path.name}")

    except Exception as e:
        logger.error(f"Failed to process {filename}: {e}", exc_info=True)


def main() -> None:
    """Prepare all datasets for the Key Lime Pie BI project.

    Ensures the processed directory exists, then cleans customers, products,
    and sales datasets.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    datasets = [
        ("customers", ["customersegmentid"]),
        ("key_lime_products", ["productid"]),
        ("sales_fact", ["transactionid"]),
    ]

    for dataset, required_cols in datasets:
        prep_dataset(dataset, required_cols)

    logger.info(f"Data prep complete. Clean files written to: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
