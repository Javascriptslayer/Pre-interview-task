import pytest
import pandas as pd
import requests
import sqlite3
import sys
import os
from unittest.mock import patch, Mock

# Ensure the 'weather' module is in the path
sys.path.insert(0, os.path.abspath(os.getcwd()))

from sales_analysis import (
    load_and_merge_data,
    perform_aggregations_and_store,
    fetch_data_from_database
)

def print_test_result(test_name, success):
    """Helper function to print formatted test results"""
    status = "✅ Passed" if success else "❌ Failed"
    print(f"\n{status} {test_name}")

@pytest.fixture
def mock_user_data():
    return [
        {
            'id': 1,
            'name': 'John Doe',
            'username': 'johndoe',
            'email': 'john@example.com',
            'address': {
                'geo': {'lat': '40.7128', 'lng': '-74.0060'}
            }
        },
        {
            'id': 2,
            'name': 'Jane Smith',
            'username': 'janesmith',
            'email': 'jane@example.com',
            'address': {
                'geo': {'lat': '34.0522', 'lng': '-118.2437'}
            }
        },
        {
            'id': 3,
            'name': 'Alice Johnson',
            'username': 'alicej',
            'email': 'alice@example.com',
            'address': {
                'geo': {'lat': '51.5074', 'lng': '-0.1278'}
            }
        }
    ]

@patch('sales_analysis.pd.read_csv')  # Mock CSV loading
@patch('sales_analysis.requests.get')  # Mock API call
def test_load_and_merge_data(mock_get, mock_read_csv, mock_user_data):
    """Test data loading and merging functionality"""
    test_name = "Data Loading and Merging"
    try:
        # Mock Sales Data
        mock_read_csv.return_value = pd.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "product_id": [101, 102, 103, 104],
            "quantity": [5, 3, 7, 2],
            "price": [20.0, 15.0, 10.0, 30.0]
        })

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = mock_user_data
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Execute test
        merged_df = load_and_merge_data()

        # Assertions
        assert not merged_df.empty, "Empty dataframe returned"
        assert 'name' in merged_df.columns, "Missing name column"
        assert 'lat' in merged_df.columns, "Missing latitude column"
        assert merged_df.shape[0] == 4, f"Incorrect row count: {merged_df.shape[0]} (Expected: 4)"

        print_test_result(test_name, True)
    except Exception as e:
        print_test_result(test_name, False)
        raise e

def test_perform_aggregations_and_store():
    """Test sales data aggregations and storage"""
    test_name = "Data Aggregations and Storage"
    try:
        # Test data setup
        test_df = pd.DataFrame({
            'order_id': [1, 2],
            'customer_id': [1, 2],
            'product_id': [101, 102],
            'quantity': [2, 3],
            'price': [10.0, 15.0],
            'order_date': ['2023-01-01', '2023-02-01']
        })

        # Execute test
        result = perform_aggregations_and_store(test_df)

        # Assertions
        assert 'total_sales' in test_df.columns, "Missing total_sales column"
        assert test_df['total_sales'].tolist() == [20.0, 45.0], "Incorrect sales calculations"
        assert os.path.exists('sales_data.db'), "Database file not created"

        print_test_result(test_name, True)
    except Exception as e:
        print_test_result(test_name, False)
        raise e

def test_fetch_data_from_database():
    """Test database fetching functionality"""
    test_name = "Database Fetch Operations"
    try:
        # Setup test database
        conn = sqlite3.connect('sales_data.db')
        test_df = pd.DataFrame({
            'order_id': [1, 2],
            'customer_id': [1, 2],
            'product_id': [101, 102],
            'quantity': [2, 3],
            'price': [10.0, 15.0],
            'order_date': ['2023-01-01', '2023-02-01'],
            'total_sales': [20.0, 45.0]
        })
        test_df.to_sql('sales', conn, if_exists='replace', index=False)
        conn.close()

        # Execute test
        df = fetch_data_from_database()

        # Assertions
        assert df.shape == (2, 7), f"Incorrect dataframe shape from DB: {df.shape}"
        assert 'total_sales' in df.columns, "Missing total_sales column in fetched data"

        print_test_result(test_name, True)
    except Exception as e:
        print_test_result(test_name, False)
        raise e

if __name__ == "__main__":
    pytest.main()
