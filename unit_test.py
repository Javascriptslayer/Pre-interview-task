import pytest
import pandas as pd
import requests
import sqlite3
import sys
import os
from unittest.mock import patch, MagicMock

# Ensure the 'weather' module is in the path
sys.path.insert(0, os.path.abspath(os.getcwd()))

from weather import (
    is_valid_coordinate,
    load_and_merge_data,
    fetch_and_add_weather_data,
    perform_aggregations_and_store,
)

# Test is_valid_coordinate
def test_is_valid_coordinate():
    assert is_valid_coordinate(45.0) is True
    assert is_valid_coordinate(-179.99) is True
    assert is_valid_coordinate(180.1) is False
    assert is_valid_coordinate(-181) is False
    assert is_valid_coordinate(None) is False  # Ensure None input is handled
    assert is_valid_coordinate("invalid") is False  # Ensure string input is handled
    assert is_valid_coordinate([]) is False  # Ensure list input is handled
    assert is_valid_coordinate({}) is False  # Ensure dict input is handled
    assert is_valid_coordinate(0) is True  # Edge case: 0 should be valid
    assert is_valid_coordinate(179.99) is True  # Edge case: Valid max coordinate
    assert is_valid_coordinate(-179.99) is True  # Edge case: Valid min coordinate

# Test load_and_merge_data with mocked file and API responses
@patch("pandas.read_csv")
@patch("requests.get")
def test_load_and_merge_data(mock_get, mock_read_csv):
    mock_read_csv.return_value = pd.DataFrame({
        "customer_id": [1, 2],
        "product_id": [101, 102],
        "quantity": [2, 3],
        "price": [20.0, 15.0]
    })
    
    mock_get.return_value.json.return_value = [
        {"id": 1, "name": "Alice", "username": "alice123", "email": "alice@test.com", "address": {"geo": {"lat": "34.05", "lng": "-118.25"}}},
        {"id": 2, "name": "Bob", "username": "bob456", "email": "bob@test.com", "address": {"geo": {"lat": "40.71", "lng": "-74.01"}}}
    ]
    
    merged_data = load_and_merge_data()
    assert merged_data is not None
    assert "lat" in merged_data.columns
    assert "lng" in merged_data.columns
    assert len(merged_data) == 2

# Test fetch_and_add_weather_data with mocked API response
@patch("requests.get")
def test_fetch_and_add_weather_data(mock_get):
    mock_get.return_value.json.return_value = {"main": {"temp": 25.0}, "weather": [{"description": "clear sky"}]}
    
    test_df = pd.DataFrame({
        "customer_id": [1],
        "lat": [34.05],
        "lng": [-118.25],
        "quantity": [2],
        "price": [20.0]
    })
    
    result_df = fetch_and_add_weather_data(test_df)
    assert result_df is not None
    assert "temp" in result_df.columns
    assert "conditions" in result_df.columns
    assert result_df.iloc[0]["temp"] == 25.0
    assert result_df.iloc[0]["conditions"] == "clear sky"

# Test perform_aggregations_and_store with a sample dataframe
def test_perform_aggregations_and_store():
    test_df = pd.DataFrame({
        "customer_id": [1, 2],
        "quantity": [2, 3],
        "price": [20.0, 15.0],
        "order_date": ["2023-01-15", "2023-02-20"],
        "conditions": ["clear sky", "rain"]
    })
    test_df["order_date"] = pd.to_datetime(test_df["order_date"])
    
    result_df = perform_aggregations_and_store(test_df)
    assert result_df is not None, "Function returned None, check for errors in processing."
    assert "total_sales" in result_df.columns, "Column 'total_sales' missing in output DataFrame."
    assert result_df["total_sales"].sum() == (2 * 20.0 + 3 * 15.0)
    
    # Ensure function handles missing columns gracefully
    incomplete_df = pd.DataFrame({"customer_id": [1, 2]})
    assert perform_aggregations_and_store(incomplete_df) is None, "Function should return None when required columns are missing."
    
    # Ensure function handles incorrect data types gracefully
    incorrect_df = pd.DataFrame({
        "customer_id": [1, 2],
        "quantity": ["two", 3],
        "price": [20.0, "fifteen"],
        "order_date": ["2023-01-15", "2023-02-20"]
    })
    assert perform_aggregations_and_store(incorrect_df) is None, "Function should return None when data types are incorrect."

if __name__ == "__main__":
    pytest.main()
