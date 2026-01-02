"""
Credit Card Transaction Pipeline - Main Orchestrator
Runs the complete medallion architecture pipeline: Bronze -> Silver -> Gold
"""

import os
import sys
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from lambdas.bronze.bronze_layer import ingest_raw_data
from lambdas.silver.silver_layer import clean_and_validate
from lambdas.gold.gold_layer import create_analytical_tables
from insights import generate_insights


def run_pipeline():
    """
    Execute the complete data pipeline.
    """
    print("=" * 80)
    print("CREDIT CARD TRANSACTION PIPELINE - MEDALLION ARCHITECTURE")
    print("=" * 80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Define paths
    base_dir = os.path.dirname(__file__)
    raw_csv = os.path.join(base_dir, "data", "raw", "credit_card_transactions.csv")
    bronze_output = os.path.join(base_dir, "data", "bronze", "transactions.parquet")
    silver_output = os.path.join(base_dir, "data", "silver", "transactions_clean.parquet")
    gold_dir = os.path.join(base_dir, "data", "gold")
    
    try:
        # BRONZE LAYER - Raw Ingestion
        print("\n" + "=" * 80)
        print("BRONZE LAYER - RAW DATA INGESTION")
        print("=" * 80)
        bronze_metadata = ingest_raw_data(raw_csv, bronze_output)
        
        # SILVER LAYER - Cleaning & Validation
        print("\n" + "=" * 80)
        print("SILVER LAYER - DATA CLEANING & VALIDATION")
        print("=" * 80)
        silver_metadata = clean_and_validate(bronze_output, silver_output)
        
        # GOLD LAYER - Analytical Tables
        print("\n" + "=" * 80)
        print("GOLD LAYER - ANALYTICAL AGGREGATIONS")
        print("=" * 80)
        gold_metadata = create_analytical_tables(silver_output, gold_dir)
        
        # INSIGHTS - Generate Business Insights
        print("\n" + "=" * 80)
        print("INSIGHTS - BUSINESS INTELLIGENCE")
        print("=" * 80)
        insights = generate_insights(gold_dir)
        
        # Pipeline Summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"✓ Bronze Layer: {bronze_metadata['row_count']:,} rows ingested")
        print(f"✓ Silver Layer: {silver_metadata['valid_rows']:,} valid rows out of {silver_metadata['total_rows']:,}")
        print(f"✓ Gold Layer: {gold_metadata['customers']:,} customers, {gold_metadata['categories']} categories")
        print(f"✓ Insights: Generated successfully")
        print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
