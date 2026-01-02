"""
Utility Functions
Shared helper functions used across pipeline layers.
"""

import pandas as pd
from typing import List, Optional


def get_column_info(df: pd.DataFrame) -> dict:
    """
    Get comprehensive information about DataFrame columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        dict: Column information including types, nulls, etc.
    """
    info = {
        'total_columns': len(df.columns),
        'total_rows': len(df),
        'columns': []
    }
    
    for col in df.columns:
        col_info = {
            'name': col,
            'dtype': str(df[col].dtype),
            'null_count': int(df[col].isna().sum()),
            'null_percentage': round(df[col].isna().sum() / len(df) * 100, 2),
            'unique_values': int(df[col].nunique())
        }
        info['columns'].append(col_info)
    
    return info


def format_currency(amount: float) -> str:
    """Format amount as currency string."""
    return f"${amount:,.2f}"


def format_percentage(value: float, decimals: int = 2) -> str:
    """Format value as percentage string."""
    return f"{value:.{decimals}f}%"


def deduplicate_dataframe(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = 'first'
) -> pd.DataFrame:
    """
    Remove duplicate rows from DataFrame.
    
    Args:
        df: Input DataFrame
        subset: Column names to consider for duplicates
        keep: Which duplicates to keep ('first', 'last', False)
        
    Returns:
        DataFrame with duplicates removed
    """
    before_count = len(df)
    df_deduped = df.drop_duplicates(subset=subset, keep=keep)
    after_count = len(df_deduped)
    
    if before_count > after_count:
        print(f"Removed {before_count - after_count:,} duplicate rows")
    
    return df_deduped
