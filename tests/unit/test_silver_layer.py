"""Unit tests for silver layer transformations"""

import pytest
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from lambdas.silver.silver_layer import validate_transaction, clean_and_validate


class TestValidateTransaction:
    """Test the validate_transaction function"""
    
    def test_valid_transaction(self):
        """
        Test that a valid transaction passes validation
        
        """
        row = pd.Series({
            'amt': 50.00,
            'merchant': 'Test Store',
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now() - timedelta(days=1)
        })

        is_valid, issues = validate_transaction(row)
        assert is_valid == True
        assert len(issues) == 0


    def test_negative_amount(self):
        """
        Test that negative amounts are flagged

        """
        row = pd.Series({
            'amt': -50.00,
            'merchant': 'Test Store',
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now()
        })

        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'negative_amount' in issues


    def test_missing_amount(self):
        """
        Test that missing amounts are flagged

        """
        row = pd.Series({
            'amt': None,
            'merchant': 'Test Store',
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now()
        })

        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'missing_amount' in issues


    def test_missing_merchant(self):
        """Test that missing merchant is flagged"""
        row = pd.Series({
            'amt': 50.00,
            'merchant': None,
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now()
        })
        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'missing_merchant' in issues


    def test_missing_category(self):
        """Test that missing category is flagged"""
        row = pd.Series({
            'amt': 50.00,
            'merchant': 'Test Store',
            'category': None,
            'trans_date_trans_time': datetime.now()
        })
        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'missing_category' in issues


    def test_future_date(self):
        """
        Test that future dates are flagged

        """
        row = pd.Series({
            'amt': 50.00,
            'merchant': 'Test Store',
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now() + timedelta(days=1)
        })
        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'future_date' in issues


    def test_multiple_issues(self):
        """
        Test that multiple issues are captured

        """
        row = pd.Series({
            'amt': -50.00,
            'merchant': None,
            'category': 'shopping_pos',
            'trans_date_trans_time': datetime.now()
        })

        is_valid, issues = validate_transaction(row)
        assert is_valid == False
        assert 'negative_amount' in issues
        assert 'missing_merchant' in issues
        assert len(issues) == 2


class TestCleanAndValidate:
    """Test the clean_and_validate function with sample data"""
    
    @pytest.fixture
    def sample_data(self, tmp_path):
        """
        Create sample bronze data for testing

        """
        df = pd.DataFrame({
            'trans_num': ['trans1', 'trans2', 'trans3', 'trans4'],
            'amt': [100.50, -50.00, 200.00, None],
            'merchant': ['Store A', 'Store B', None, 'Store D'],
            'category': ['shopping_pos', 'grocery_pos', 'gas_transport', 'misc_net'],
            'trans_date_trans_time': [
                '2024-01-01 10:00:00',
                '2024-01-02 11:00:00',
                '2024-01-03 12:00:00',
                '2024-01-04 13:00:00'
            ],
            'dob': ['1990-01-01', '1985-05-15', '1992-08-20', '1988-03-10'],
            'lat': [40.7128, 34.0522, 41.8781, 29.7604],
            'long': [-74.0060, -118.2437, -87.6298, -95.3698],
            'merch_lat': [40.7130, 34.0525, 41.8785, 29.7610],
            'merch_long': [-74.0065, -118.2440, -87.6300, -95.3700]
        })
        
        bronze_path = tmp_path / "bronze.parquet"
        df.to_parquet(bronze_path, index=False)
        return bronze_path, df


    def test_validation_columns_added(self, sample_data, tmp_path):
        """
        Test that validation columns are added

        """
        bronze_path, _ = sample_data
        silver_path = tmp_path / "silver.parquet"
        
        metadata = clean_and_validate(str(bronze_path), str(silver_path))
        
        df = pd.read_parquet(silver_path)
        assert 'is_valid' in df.columns
        assert 'quality_issues' in df.columns
        assert '_silver_processed_at' in df.columns


    def test_validation_counts(self, sample_data, tmp_path):
        """
        Test that validation counts are correct

        """
        bronze_path, _ = sample_data
        silver_path = tmp_path / "silver.parquet"
        
        metadata = clean_and_validate(str(bronze_path), str(silver_path))
        
        assert metadata['total_rows'] == 4
        assert metadata['valid_rows'] == 1  # Only first row is valid
        assert metadata['invalid_rows'] == 3


    def test_data_type_conversions(self, sample_data, tmp_path):
        """
        Test that data types are converted correctly

        """
        bronze_path, _ = sample_data
        silver_path = tmp_path / "silver.parquet"
        
        clean_and_validate(str(bronze_path), str(silver_path))
        
        df = pd.read_parquet(silver_path)
        assert pd.api.types.is_datetime64_any_dtype(df['trans_date_trans_time'])
        assert pd.api.types.is_datetime64_any_dtype(df['dob'])
        assert pd.api.types.is_numeric_dtype(df['amt'])
