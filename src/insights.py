"""
Insights Generator
Analyzes gold layer data and produces business insights.
"""

import pandas as pd
import os
from datetime import datetime


def generate_insights(gold_dir: str) -> dict:
    """
    Generate business insights from gold layer tables.

    """
    # Read gold tables
    customer_summary = pd.read_parquet(os.path.join(gold_dir, "customer_summary.parquet"))
    category_analysis = pd.read_parquet(os.path.join(gold_dir, "merchant_category_analysis.parquet"))
    
    print("\n📊 INSIGHT 1: HIGH-RISK MERCHANT CATEGORIES (Fraud Analysis)")
    print("-" * 80)
    
    # Top fraud categories
    high_fraud = category_analysis.nlargest(5, 'fraud_rate')[
        ['category', 'fraud_rate', 'fraud_count', 'total_transactions']
    ]
    
    for idx, row in high_fraud.iterrows():
        print(f"{row['category']:20s} - {row['fraud_rate']:5.2f}% fraud rate "
              f"({row['fraud_count']:,} fraudulent / {row['total_transactions']:,} total)")
    
    print("\n💡 Recommendation: Implement enhanced verification for these high-risk categories.")
    
    print("\n" + "=" * 80)
    print("📊 KEY METRICS SUMMARY")
    print("-" * 80)
    
    total_customers = len(customer_summary)
    total_categories = len(category_analysis)
    total_transactions = category_analysis['total_transactions'].sum()
    total_fraud = category_analysis['fraud_count'].sum()
    overall_fraud_rate = (total_fraud / total_transactions * 100)
    
    print(f"Total Customers: {total_customers:,}")
    print(f"Total Merchant Categories: {total_categories}")
    print(f"Total Transactions: {total_transactions:,}")
    print(f"Total Fraudulent Transactions: {total_fraud:,}")
    print(f"Overall Fraud Rate: {overall_fraud_rate:.2f}%")
    
    # Save insights report
    report_path = os.path.join(gold_dir, "insights_report.txt")
    with open(report_path, 'w') as f:
        f.write("CREDIT CARD TRANSACTION INSIGHTS REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("INSIGHT 1: HIGH-RISK MERCHANT CATEGORIES\n")
        f.write("-" * 80 + "\n")
        f.write(high_fraud.to_string(index=False))
        f.write("\n\nRecommendation: Implement enhanced verification for these high-risk categories.\n\n")
        
        f.write("KEY METRICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Customers: {total_customers:,}\n")
        f.write(f"Total Transactions: {total_transactions:,}\n")
        f.write(f"Overall Fraud Rate: {overall_fraud_rate:.2f}%\n")
    
    print(f"\n📄 Insights report saved to: {report_path}")
    
    return {
        'total_customers': total_customers,
        'total_transactions': total_transactions,
        'fraud_rate': overall_fraud_rate,
        'report_path': report_path
    }


if __name__ == "__main__":
    # Local testing
    base_dir = os.path.dirname(os.path.dirname(__file__))
    gold_dir = os.path.join(base_dir, "data", "gold")
    
    insights = generate_insights(gold_dir)
