"""ETL pipeline: normalize CSVs and load into a SQLite data warehouse.

- Consistent column naming across normalization, schema, and inserts
- Basic validation for foreign keys
- Logging for clarity and reproducibility
"""

from __future__ import annotations

from datetime import datetime
import logging
import pathlib
import sqlite3
import sys
from typing import List  # noqa: UP035

import pandas as pd

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

# -----------------------------------------------------------------------------
# Project paths
# -----------------------------------------------------------------------------
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
logging.info(f"Project root {PROJECT_ROOT}")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

DW_DIR = PROJECT_ROOT / "data" / "dw"
DB_PATH = DW_DIR / "keylime_sales.db"
PREPARED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def drop_dupes(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Drop duplicate rows from a DataFrame based on a key column.

    Args:
        df: Input DataFrame.
        key: Column name to check for duplicates.

    Returns:
        DataFrame with duplicates removed.
    """
    return df.drop_duplicates(subset=[key], keep="first").copy()


# -----------------------------------------------------------------------------
# Normalization functions
# -----------------------------------------------------------------------------
def norm_customers(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize headers
    df.columns = df.columns.str.strip().str.lower()

    # Rename to match schema
    df = df.rename(
        columns={"customersegmentid": "customer_segmentid", "customersegment": "customer_segment"}
    )

    df = df[["customer_segmentid", "customer_segment"]].copy()

    # Convert "C1" → 1, "C2" → 2, etc.
    df["customer_segmentid"] = (
        df["customer_segmentid"]
        .astype(str)  # ensure string type
        .str.strip()  # remove leading trainling spaces
        .str.extract(r"[Cc](\d+)", expand=False)  # captures digits after "C" or "c"
        .astype("Int64")
    )

    df = df.dropna(subset=["customer_segmentid"])
    return drop_dupes(df, "customer_segmentid")


def norm_products(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize products to:
      - product_id: str
    - product_variant: str.
    """  # noqa: D205
    df = df.rename(
        columns={
            "productid": "product_id",
            "productvariant": "product_variant",
        }
    )
    df = df[["product_id", "product_variant"]].copy()
    df["product_id"] = (
        df["product_id"]
        .astype(str)
        .str.strip()
        .str.extract(r"[Pp](\d+)", expand=False)  # captures digits after "P" or "p"
        .astype("Int64")
    )  # Drop missing IDs and dedupe
    df = df.dropna(subset=["product_id"])
    return drop_dupes(df, "product_id")


def norm_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Docstring for norm_sales."""
    df.columns = df.columns.str.strip().str.lower()

    df = df.rename(
        columns={
            "transactionid": "sales_id",
            "customersegmentid": "customer_segmentid",
            "productid": "product_id",
            "date": "sale_date",
            "unitssold": "units_sold",
            "revenue": "sale_amount",
            "profitmargin": "profit_margin",
        }
    )

    required = [
        "sales_id",
        "customer_segmentid",
        "product_id",
        "units_sold",
        "sale_date",
        "sale_amount",
        "profit_margin",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required sales columns after rename: {missing}")

    df = df[required].copy()

    # Convert IDs to integers by stripping prefixes
    df["customer_segmentid"] = (
        df["customer_segmentid"]
        .astype(str)
        .str.strip()
        .str.extract(r"[Cc](\d+)", expand=False)
        .astype("Int64")
    )

    df["product_id"] = (
        df["product_id"]
        .astype(str)
        .str.strip()
        .str.extract(r"[Pp](\d+)", expand=False)
        .astype("Int64")
    )

    # Convert sales_id to integer
    df["sales_id"] = pd.to_numeric(df["sales_id"], errors="coerce").astype("Int64")

    # Convert measures to numeric
    df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce")
    df["sale_amount"] = pd.to_numeric(df["sale_amount"], errors="coerce")
    df["profit_margin"] = pd.to_numeric(df["profit_margin"], errors="coerce")

    # Drop rows missing critical values
    df = df.dropna(subset=["customer_segmentid", "product_id", "sale_amount"])

    # Reassign sales_id if duplicates or nulls
    if df["sales_id"].isna().any() or df["sales_id"].duplicated().any():
        df = df.reset_index(drop=True)
    df["sales_id"] = (df.index + 1).astype("Int64")
    return df


# -----------------------------------------------------------------------------
# Date dimension
# -----------------------------------------------------------------------------
def generate_date_dimension(start_date: str, end_date: str) -> pd.DataFrame:
    """Generate date dimension table with various date attributes.

    Args:
        start_date: 'YYYY-MM-DD'
        end_date: 'YYYY-MM-DD'

    Returns:
        Date dimension DataFrame.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_range = pd.date_range(start=start, end=end, freq="D")

    return pd.DataFrame(
        {
            "date_id": date_range.strftime("%Y%m%d").astype(int),
            "full_date": date_range.strftime("%m/%d/%Y"),
            "year": date_range.year,
            "month": date_range.month,
            "month_name": date_range.strftime("%B"),
            "day": date_range.day,
            "week": date_range.isocalendar().week,
        }
    )


# -----------------------------------------------------------------------------
# Schema
# -----------------------------------------------------------------------------
def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create the database schema for the data warehouse."""
    # Drop in FK-safe order
    cursor.execute("DROP TABLE IF EXISTS sales")
    cursor.execute("DROP TABLE IF EXISTS product")
    cursor.execute("DROP TABLE IF EXISTS customer")
    cursor.execute("DROP TABLE IF EXISTS dim_date")

    # dim_date
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            day INTEGER NOT NULL,
            week INTEGER NOT NULL
        )
        """
    )

    # customer (aligned to normalized output)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer (
            customer_segmentid INTEGER PRIMARY KEY,
            customer_segment TEXT
        )
        """
    )

    # product (aligned to normalized output)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS product (
            product_id INTEGER PRIMARY KEY,
            product_variant TEXT
        )
        """
    )

    # sales (FKs aligned to customer + product)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            sales_id INTEGER PRIMARY KEY,
            customer_segmentid INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            units_sold REAL,
            sale_amount REAL NOT NULL,
            sale_date TEXT,
            profit_margin REAL,
            FOREIGN KEY (customer_segmentid) REFERENCES customer (customer_segmentid),
            FOREIGN KEY (product_id) REFERENCES product (product_id)
        )
        """
    )


# -----------------------------------------------------------------------------
# Loaders
# -----------------------------------------------------------------------------
def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all records in tables, FK-safe order (sales first)."""
    for table in ["sales", "product", "customer", "dim_date"]:
        logging.info(f"Deleting all rows from {table}")
        cursor.execute(f"DELETE FROM {table}")


def insert_dim_date(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert date dimension data into dim_date table."""
    df.to_sql("dim_date", cursor.connection, if_exists="append", index=False)
    logging.info(f"Inserted {len(df)} records into dim_date")


def insert_customers(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customers data into customer table."""
    df.to_sql("customer", cursor.connection, if_exists="append", index=False)
    logging.info(f"Inserted {len(df)} customers")


def insert_products(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert products data into product table."""
    df.to_sql("product", cursor.connection, if_exists="append", index=False)
    logging.info(f"Inserted {len(df)} products")


def insert_sales(df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data and validate foreign keys exist in dimension tables."""
    initial_count = len(df)
    logging.info(f"Processing {initial_count} sales records")

    # Fetch valid FK sets
    cursor.execute("SELECT customer_segmentid FROM customer")
    valid_customer_segmentids = {row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT product_id FROM product")
    valid_product_ids = {row[0] for row in cursor.fetchall()}

    # Filter by valid FKs
    before = len(df)
    df = df[df["customer_segmentid"].isin(valid_customer_segmentids)].copy()
    df = df[df["product_id"].isin(valid_product_ids)].copy()
    after = len(df)

    logging.info(f"Sales filtered by FK: {before} -> {after}")

    if after > 0:
        df.to_sql("sales", cursor.connection, if_exists="append", index=False)
        logging.info(f"Inserted {len(df)} sales")
    else:
        logging.warning("No valid sales records to insert")


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------
def print_table_row_counts(cursor: sqlite3.Cursor, tables: List[str]) -> None:
    """Print row counts for given tables."""
    logging.info("Table row counts:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        logging.info(f"{table}: {count}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def load_data_to_db(truncate: bool = False) -> None:
    """Load normalized CSVs into SQLite DW.

    Args:
        truncate: If True, delete existing rows before insert.
    """
    DW_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()

    create_schema(cursor)

    if truncate:
        delete_existing_records(cursor)

    # Date dimension (e.g., 2022 through 2026)
    logging.info("Generating date dimension ...")
    dim_date_df = generate_date_dimension("2022-01-01", "2026-01-01")
    insert_dim_date(dim_date_df, cursor)

    # Customers
    customer_file_path = PREPARED_DATA_DIR / "customers_clean.csv"
    if not customer_file_path.exists():
        raise FileNotFoundError(f"Missing file: {customer_file_path}")
    logging.info(f"Loading file: {customer_file_path.name}")
    customers_df = pd.read_csv(customer_file_path)
    customers_df = norm_customers(customers_df)
    insert_customers(customers_df, cursor)

    # Products
    product_file_path = PREPARED_DATA_DIR / "key_lime_products_clean.csv"
    if not product_file_path.exists():
        raise FileNotFoundError(f"Missing file: {product_file_path}")
    logging.info(f"Loading file: {product_file_path.name}")
    products_df = pd.read_csv(product_file_path)
    products_df = norm_products(products_df)
    insert_products(products_df, cursor)

    # Sales
    sales_file_path = PREPARED_DATA_DIR / "sales_fact_clean.csv"
    if not sales_file_path.exists():
        raise FileNotFoundError(f"Missing file: {sales_file_path}")
    logging.info(f"Loading file: {sales_file_path.name}")
    sales_df = pd.read_csv(sales_file_path)
    sales_df = norm_sales(sales_df)
    insert_sales(sales_df, cursor)

    conn.commit()
    print_table_row_counts(cursor, ["dim_date", "customer", "product", "sales"])

    conn.close()
    logging.info("ETL process completed successfully!")


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    load_data_to_db(truncate=True)
