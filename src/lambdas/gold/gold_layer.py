"""
Gold Layer: Analytical Aggregations
Creates business-ready analytical tables for insights and reporting.
"""

import pandas as pd
from datetime import datetime
import os


def create_customer_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create customer spending summary table.

    """
    print("Creating customer summary table")
    
    # Create customer identifier
    df['customer_id'] = df['cc_num']
    df['full_name'] = df['first'] + ' ' + df['last']
    
    # Aggregate by customer
    customer_summary = df.groupby(['customer_id', 'full_name']).agg({
        'trans_num': 'count',
        'amt': ['sum', 'mean', 'median'],
        'is_fraud': 'sum',
        'trans_date_trans_time': ['min', 'max'],
        'merchant': 'nunique',
        'city': 'first',
        'state': 'first',
        'job': 'first'
    }).reset_index()
    
    # Flatten column names
    customer_summary.columns = [
        'customer_id', 'full_name', 'total_transactions', 'total_spend',
        'avg_transaction', 'median_transaction', 'fraud_count',
        'first_transaction_date', 'last_transaction_date', 'unique_merchants',
        'city', 'state', 'job'
    ]
    
    # Calculate fraud rate
    customer_summary['fraud_rate'] = (
        customer_summary['fraud_count'] / customer_summary['total_transactions'] * 100
        ).round(2)
    
    # Calculate customer lifetime in days
    customer_summary['customer_lifetime_days'] = (
        customer_summary['last_transaction_date'] - customer_summary['first_transaction_date']
        ).dt.days
    
    # Round monetary values
    customer_summary['total_spend'] = customer_summary['total_spend'].round(2)
    customer_summary['avg_transaction'] = customer_summary['avg_transaction'].round(2)
    customer_summary['median_transaction'] = customer_summary['median_transaction'].round(2)
    
    print(f"[GOLD] Customer summary: {len(customer_summary):,} customers")
    
    return customer_summary


def create_merchant_category_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create merchant category analysis table.

    """
    print("Creating merchant category analysis table")
    
    # Aggregate by category
    category_analysis = df.groupby('category').agg({
        'trans_num': 'count',
        'amt': ['sum', 'mean', 'median'],
        'is_fraud': 'sum',
        'cc_num': 'nunique',
        'merchant': 'nunique'
    }).reset_index()
    
    # Flatten column names
    category_analysis.columns = [
        'category', 'total_transactions', 'total_amount', 'avg_amount',
        'median_amount', 'fraud_count', 'unique_customers', 'unique_merchants'
    ]
    
    # Calculate fraud rate
    category_analysis['fraud_rate'] = (
        category_analysis['fraud_count'] / category_analysis['total_transactions'] * 100
    ).round(2)
    
    # Round monetary values
    category_analysis['total_amount'] = category_analysis['total_amount'].round(2)
    category_analysis['avg_amount'] = category_analysis['avg_amount'].round(2)
    category_analysis['median_amount'] = category_analysis['median_amount'].round(2)
    
    # Sort by transaction volume
    category_analysis = category_analysis.sort_values('total_transactions', ascending=False)
    
    print(f"[GOLD] Category analysis: {len(category_analysis)} categories")
    
    return category_analysis


def create_analytical_tables(silver_path: str, output_dir: str) -> dict:
    """
    Create all gold layer analytical tables.
    
    """
    print(f"[GOLD] Starting analytical table creation from {silver_path}")
    
    # Read silver data
    df = pd.read_parquet(silver_path)
    
    # Only use valid records for analytics
    df_valid = df[df['is_valid'] == True].copy()
    
    print(f"[GOLD] Using {len(df_valid):,} valid records out of {len(df):,} total")
    
    # Create analytical tables
    customer_summary = create_customer_summary(df_valid)
    category_analysis = create_merchant_category_analysis(df_valid)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Save tables
    customer_path = os.path.join(output_dir, "customer_summary.parquet")
    category_path = os.path.join(output_dir, "merchant_category_analysis.parquet")
    
    customer_summary.to_parquet(customer_path, index=False, engine='pyarrow')
    category_analysis.to_parquet(category_path, index=False, engine='pyarrow')
    
    metadata = {
        'source_rows': len(df),
        'valid_rows_used': len(df_valid),
        'customers': len(customer_summary),
        'categories': len(category_analysis),
        'created_at': datetime.now().isoformat(),
        'tables': {
            'customer_summary': customer_path,
            'merchant_category_analysis': category_path
        }
    }
    
    print(f"[GOLD] Analytical tables created successfully")
    print(f"  - Customer summary: {metadata['customers']:,} records")
    print(f"  - Category analysis: {metadata['categories']} records")
    
    return metadata


if __name__ == "__main__":
    # Local testing
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    silver = os.path.join(base_dir, "data", "silver", "transactions_clean.parquet")
    output_dir = os.path.join(base_dir, "data", "gold")
    
    metadata = create_analytical_tables(silver, output_dir)
    print(f"Metadata: {metadata}")
