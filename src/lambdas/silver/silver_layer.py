"""
Silver Layer: Data Cleaning and Validation
Transforms bronze data into clean, validated, analytics-ready data.
"""

import pandas as pd
from datetime import datetime
import os


def validate_transaction(row: pd.Series) -> tuple:
    """
    Validate a single transaction record.
    
    """
    issues = []
    
    # Check for negative amounts
    if pd.notna(row['amt']) and row['amt'] < 0:
        issues.append('negative_amount')
    
    # Check for missing critical fields
    if pd.isna(row['amt']):
        issues.append('missing_amount')
    if pd.isna(row['merchant']):
        issues.append('missing_merchant')
    if pd.isna(row['category']):
        issues.append('missing_category')
    
    # Check for invalid dates (future dates)
    try:
        trans_date = pd.to_datetime(row['trans_date_trans_time'])
        if trans_date > datetime.now():
            issues.append('future_date')
    except:
        issues.append('invalid_date')
    
    is_valid = len(issues) == 0 # True or False
    
    return is_valid, issues


def clean_and_validate(bronze_path: str, output_path: str) -> dict:
    """
    Clean and validate bronze layer data.
        
    """
    print(f"[SILVER] Starting transformation from {bronze_path}")
    
    # Read bronze data
    df = pd.read_parquet(bronze_path)
    
    # Data type conversions
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['dob'] = pd.to_datetime(df['dob'])
    df['amt'] = pd.to_numeric(df['amt'], errors='coerce') # coerce errors to NaN
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['long'] = pd.to_numeric(df['long'], errors='coerce')
    df['merch_lat'] = pd.to_numeric(df['merch_lat'], errors='coerce')
    df['merch_long'] = pd.to_numeric(df['merch_long'], errors='coerce')
    
    # Add validation columns
    validation_results = df.apply(validate_transaction, axis=1) # row-wise
    df['is_valid'] = validation_results.apply(lambda x: x[0])
    df['quality_issues'] = validation_results.apply(lambda x: ','.join(x[1]) if x[1] else None)
    
    # Add processing metadata
    df['_silver_processed_at'] = datetime.now()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save silver data
    df.to_parquet(output_path, index=False, engine='pyarrow')
    
    # Calculate metadata
    valid_count = df['is_valid'].sum()
    total_count = len(df)
    metadata = {
        'total_rows': total_count,
        'valid_rows': valid_count,
        'invalid_rows': (~df['is_valid']).sum(),
        'processed_at': datetime.now().isoformat(),
        'output_file': output_path
    }
    
    return metadata


# Local testing
if __name__ == "__main__":
    
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    bronze = os.path.join(base_dir, "data", "bronze", "transactions.parquet")
    output = os.path.join(base_dir, "data", "silver", "transactions_clean.parquet")
    
    metadata = clean_and_validate(bronze, output)
    print(f"Metadata: {metadata}")
