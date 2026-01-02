"""Unit tests for gold layer aggregations"""

import pytest
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from lambdas.gold.gold_layer import create_customer_summary, create_merchant_category_analysis


class TestCreateCustomerSummary:
    """
    Test the create_customer_summary function

    """
    
    @pytest.fixture
    def sample_data(self):
        """Create sample silver data for testing"""
        return pd.DataFrame({
            'cc_num': [1111, 1111, 2222, 2222, 3333],
            'first': ['John', 'John', 'Jane', 'Jane', 'Bob'],
            'last': ['Doe', 'Doe', 'Smith', 'Smith', 'Johnson'],
            'trans_num': ['t1', 't2', 't3', 't4', 't5'],
            'amt': [100.00, 50.00, 200.00, 150.00, 75.00],
            'is_fraud': [0, 1, 0, 0, 1],
            'trans_date_trans_time': [
                datetime(2024, 1, 1),
                datetime(2024, 1, 10),
                datetime(2024, 1, 5),
                datetime(2024, 1, 15),
                datetime(2024, 1, 20)
            ],
            'merchant': ['Store A', 'Store B', 'Store C', 'Store A', 'Store D'],
            'city': ['NYC', 'NYC', 'LA', 'LA', 'Chicago'],
            'state': ['NY', 'NY', 'CA', 'CA', 'IL'],
            'job': ['Engineer', 'Engineer', 'Doctor', 'Doctor', 'Teacher'],
            'category': ['shopping_pos', 'grocery_pos', 'gas_transport', 'shopping_pos', 'misc_net']
        })


    def test_customer_count(self, sample_data):
        """
        Test that correct number of customers are identified

        """
        result = create_customer_summary(sample_data)
        assert len(result) == 3  # 3 unique customers
    

    def test_transaction_counts(self, sample_data):
        """
        Test that transaction counts are correct

        """
        result = create_customer_summary(sample_data)
        john = result[result['full_name'] == 'John Doe'].iloc[0]
        jane = result[result['full_name'] == 'Jane Smith'].iloc[0]
        
        assert john['total_transactions'] == 2
        assert jane['total_transactions'] == 2


    def test_spending_aggregations(self, sample_data):
        """
        Test that spending aggregations are correct

        """
        result = create_customer_summary(sample_data)
        john = result[result['full_name'] == 'John Doe'].iloc[0]
        
        assert john['total_spend'] == 150.00
        assert john['avg_transaction'] == 75.00
        assert john['median_transaction'] == 75.00
    

    def test_fraud_calculations(self, sample_data):
        """
        Test that fraud metrics are calculated correctly

        """
        result = create_customer_summary(sample_data)
        john = result[result['full_name'] == 'John Doe'].iloc[0]
        bob = result[result['full_name'] == 'Bob Johnson'].iloc[0]
        
        assert john['fraud_count'] == 1
        assert john['fraud_rate'] == 50.00  # 1 out of 2 transactions
        assert bob['fraud_count'] == 1
        assert bob['fraud_rate'] == 100.00  # 1 out of 1 transaction


    def test_customer_lifetime(self, sample_data):
        """
        Test that customer lifetime is calculated correctly

        """
        result = create_customer_summary(sample_data)
        john = result[result['full_name'] == 'John Doe'].iloc[0]
        
        assert john['customer_lifetime_days'] == 9  # Jan 1 to Jan 10


    def test_unique_merchants(self, sample_data):
        """
        Test that unique merchant count is correct

        """
        result = create_customer_summary(sample_data)
        john = result[result['full_name'] == 'John Doe'].iloc[0]
        jane = result[result['full_name'] == 'Jane Smith'].iloc[0]
        
        assert john['unique_merchants'] == 2  # Store A and Store B
        assert jane['unique_merchants'] == 2  # Store C and Store A


    def test_required_columns(self, sample_data):
        """
        Test that all expected columns are present

        """
        result = create_customer_summary(sample_data)
        expected_columns = [
            'customer_id', 'full_name', 'total_transactions', 'total_spend',
            'avg_transaction', 'median_transaction', 'fraud_count',
            'first_transaction_date', 'last_transaction_date', 'unique_merchants',
            'city', 'state', 'job', 'fraud_rate', 'customer_lifetime_days'
        ]
        for col in expected_columns:
            assert col in result.columns


class TestCreateMerchantCategoryAnalysis:
    """Test the create_merchant_category_analysis function"""
    
    @pytest.fixture
    def sample_data(self):
        """
        Create sample silver data for testing

        """
        return pd.DataFrame({
            'category': ['shopping_pos', 'shopping_pos', 'grocery_pos', 'grocery_pos', 'gas_transport'],
            'trans_num': ['t1', 't2', 't3', 't4', 't5'],
            'amt': [100.00, 50.00, 200.00, 150.00, 75.00],
            'is_fraud': [0, 1, 0, 0, 1],
            'cc_num': [1111, 2222, 3333, 1111, 2222],
            'merchant': ['Store A', 'Store B', 'Store C', 'Store D', 'Store E']
        })


    def test_category_count(self, sample_data):
        """
        Test that correct number of categories are identified

        """
        result = create_merchant_category_analysis(sample_data)
        assert len(result) == 3  # 3 unique categories


    def test_transaction_counts_by_category(self, sample_data):
        """
        Test that transaction counts per category are correct

        """
        result = create_merchant_category_analysis(sample_data)
        shopping = result[result['category'] == 'shopping_pos'].iloc[0]
        grocery = result[result['category'] == 'grocery_pos'].iloc[0]
        
        assert shopping['total_transactions'] == 2
        assert grocery['total_transactions'] == 2


    def test_amount_aggregations(self, sample_data):
        """
        Test that amount aggregations are correct

        """
        result = create_merchant_category_analysis(sample_data)
        shopping = result[result['category'] == 'shopping_pos'].iloc[0]
        
        assert shopping['total_amount'] == 150.00
        assert shopping['avg_amount'] == 75.00
        assert shopping['median_amount'] == 75.00


    def test_fraud_rate_by_category(self, sample_data):
        """
        Test that fraud rates are calculated correctly per category

        """
        result = create_merchant_category_analysis(sample_data)
        shopping = result[result['category'] == 'shopping_pos'].iloc[0]
        gas = result[result['category'] == 'gas_transport'].iloc[0]
        
        assert shopping['fraud_rate'] == 50.00  # 1 fraud out of 2
        assert gas['fraud_rate'] == 100.00  # 1 fraud out of 1


    def test_unique_counts(self, sample_data):
        """
        Test that unique customer and merchant counts are correct

        """
        result = create_merchant_category_analysis(sample_data)
        shopping = result[result['category'] == 'shopping_pos'].iloc[0]
        
        assert shopping['unique_customers'] == 2  # customers 1111 and 2222
        assert shopping['unique_merchants'] == 2  # Store A and Store B


    def test_sorted_by_volume(self, sample_data):
        """
        Test that results are sorted by transaction volume

        """
        result = create_merchant_category_analysis(sample_data)
        # Should be sorted descending by total_transactions
        assert result.iloc[0]['total_transactions'] >= result.iloc[1]['total_transactions']
        assert result.iloc[1]['total_transactions'] >= result.iloc[2]['total_transactions']


    def test_required_columns(self, sample_data):
        """
        Test that all expected columns are present

        """
        result = create_merchant_category_analysis(sample_data)
        expected_columns = [
            'category', 'total_transactions', 'total_amount', 'avg_amount',
            'median_amount', 'fraud_count', 'unique_customers', 'unique_merchants',
            'fraud_rate'
        ]
        for col in expected_columns:
            assert col in result.columns
