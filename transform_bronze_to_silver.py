import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import io
import logging
from datetime import datetime

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
BRONZE_CONTAINER        = os.getenv("BRONZE_CONTAINER", "bronze")
SILVER_CONTAINER        = os.getenv("SILVER_CONTAINER", "silver")
BRONZE_FILE_NAME        = os.getenv("BRONZE_FILE_NAME", "online_retail_II.parquet")
SILVER_FILE_NAME        = f"silver_retail_{datetime.today().strftime('%Y%m%d')}.parquet"

KEY_COLUMNS = ["Customer_ID", "InvoiceDate", "Quantity"]


# ── Step 1: Read Parquet from Bronze container ─────────────────────────────────
def read_bronze(connection_string: str, container: str, filename: str) -> pd.DataFrame:
    log.info(f"Reading '{filename}' from bronze container...")

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client  = blob_service.get_blob_client(container=container, blob=filename)
    stream       = blob_client.download_blob().readall()
    df           = pd.read_parquet(io.BytesIO(stream))
    df.columns = [col.replace(" ", "") for col in df.columns]

    # Fix column names — removes spaces so "Customer ID" becomes "CustomerID"
    df.columns = [col.replace(" ", "") for col in df.columns]

    log.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns from bronze.")
    log.info(f"Columns detected: {df.columns.tolist()}")
    return df


# ── Step 2: Drop nulls in key columns ─────────────────────────────────────────
def drop_nulls(df: pd.DataFrame, key_cols: list) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=key_cols)
    dropped = before - len(df)
    log.info(f"Dropped {dropped:,} rows with nulls in {key_cols}. Remaining: {len(df):,}")
    return df


# ── Step 3: Remove duplicate rows ─────────────────────────────────────────────
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    log.info(f"Removed {dropped:,} duplicate rows. Remaining: {len(df):,}")
    return df


# ── Step 4: Filter out cancelled orders (InvoiceNo starts with 'C') ───────────
def filter_cancellations(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df["Invoice"] = df["Invoice"].astype(str)
    df = df[~df["Invoice"].str.startswith("C")]
    dropped = before - len(df)
    log.info(f"Filtered {dropped:,} cancelled orders. Remaining: {len(df):,}")
    return df


# ── Step 5: Data quality checks ────────────────────────────────────────────────
def run_quality_checks(df: pd.DataFrame) -> None:
    log.info("Running data quality checks...")

    negative_qty   = (df["Quantity"] <= 0).sum()
    negative_price = (df["Price"] <= 0).sum()

    if negative_qty > 0:
        log.warning(f"{negative_qty:,} rows have Quantity <= 0")
    if negative_price > 0:
        log.warning(f"{negative_price:,} rows have Price <= 0")

    if negative_qty == 0 and negative_price == 0:
        log.info("All quality checks passed.")

    print("\n" + "="*55)
    print("  DATA QUALITY REPORT")
    print("="*55)
    print(f"  Total clean rows       : {len(df):,}")
    print(f"  Unique customers       : {df['Customer_ID'].nunique():,}")
    print(f"  Unique invoices        : {df['Invoice'].nunique():,}")
    print(f"  Unique products        : {df['StockCode'].nunique():,}")
    print(f"  Date range             : {df['InvoiceDate'].min()} → {df['InvoiceDate'].max()}")
    print(f"  Rows with Qty <= 0     : {negative_qty:,}")
    print(f"  Rows with Price <= 0   : {negative_price:,}")
    print("="*55 + "\n")


# ── Step 6: Add derived columns ────────────────────────────────────────────────
def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["Quantity"]     = pd.to_numeric(df["Quantity"], errors="coerce")
    df["Price"]        = pd.to_numeric(df["Price"], errors="coerce")
    df["TotalRevenue"] = (df["Quantity"] * df["Price"]).round(2)
    df["InvoiceDate"]  = pd.to_datetime(df["InvoiceDate"]).dt.strftime("%Y-%m-%d")
    df["InvoiceYear"]  = pd.to_datetime(df["InvoiceDate"]).dt.year
    df["InvoiceMonth"] = pd.to_datetime(df["InvoiceDate"]).dt.month
    df["Customer_ID"] = df["Customer_ID"].astype(float).astype(int).astype(str)

    log.info("Added derived columns: TotalRevenue, InvoiceYear, InvoiceMonth.")
    return df


# ── Step 7: Write clean Parquet to Silver container ───────────────────────────
def write_silver(
    df: pd.DataFrame,
    connection_string: str,
    container: str,
    filename: str
) -> None:
    log.info(f"Writing {len(df):,} rows to silver container as '{filename}'...")

    buffer = io.BytesIO()
    table  = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)
    buffer.seek(0)

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client  = blob_service.get_blob_client(container=container, blob=filename)
    blob_client.upload_blob(buffer, overwrite=True)

    log.info(f"Silver Parquet written to '{container}/{filename}' successfully.")


# ── Main pipeline ──────────────────────────────────────────────────────────────
def main():
    log.info("Starting Bronze → Silver transformation...")

    df = read_bronze(AZURE_CONNECTION_STRING, BRONZE_CONTAINER, BRONZE_FILE_NAME)
    df = drop_nulls(df, KEY_COLUMNS)
    df = remove_duplicates(df)
    df = filter_cancellations(df)
    df = add_derived_columns(df)
    run_quality_checks(df)
    write_silver(df, AZURE_CONNECTION_STRING, SILVER_CONTAINER, SILVER_FILE_NAME)

    log.info("Bronze → Silver transformation complete.")


if __name__ == "__main__":
    main()