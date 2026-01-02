"""
Bronze Layer: Raw Data Ingestion
Ingests raw CSV data and saves as Parquet with metadata.
No transformations - just landing the raw data.
"""

import pandas as pd
from datetime import datetime
import os


def ingest_raw_data(source_path: str, output_path: str, source_system: str = "credit_card_system") -> dict:
    """
    Ingest raw CSV data and save as Parquet with metadata.

    """
    print(f"[BRONZE] Starting ingestion from {source_path}")
    
    # Read raw CSV
    df = pd.read_csv(source_path)
    
    # Add metadata columns
    df['_ingestion_timestamp'] = datetime.now()
    df['_source_file'] = os.path.basename(source_path)
    df['_source_system'] = source_system
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save as Parquet
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    metadata = {
        'row_count': len(df),
        'column_count': len(df.columns),
        'ingestion_timestamp': datetime.now().isoformat(),
        'source_file': source_path,
        'output_file': output_path
    }
    
    print(f"[BRONZE] Ingestion complete: {metadata['row_count']:,} rows")
    
    return metadata


if __name__ == "__main__":
    # Local testing
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    source = os.path.join(base_dir, "data", "raw", "credit_card_transactions.csv")
    output = os.path.join(base_dir, "data", "bronze", "transactions.parquet")
    
    metadata = ingest_raw_data(source, output)
    print(f"Metadata: {metadata}")
