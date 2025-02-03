import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os

def load_and_merge_data():
    """Loads sales data, fetches user data, and merges them."""
    try:
        # Load Sales Data
        sales_df = pd.read_csv('sales_data.csv')
        print("Sales Data Loaded:\n", sales_df.head())

        # Fetch User Data
        user_url = "https://jsonplaceholder.typicode.com/users"
        user_response = requests.get(user_url)
        user_response.raise_for_status()
        user_data = user_response.json()
        user_df = pd.DataFrame(user_data)
        user_df = user_df[['id', 'name', 'username', 'email', 'address']]
        user_df['lat'] = user_df['address'].apply(lambda x: x['geo']['lat'])
        user_df['lng'] = user_df['address'].apply(lambda x: x['geo']['lng'])
        user_df = user_df[['id', 'name', 'username', 'email', 'lat', 'lng']]
        print("\nUser Data Loaded:\n", user_df.head())

        # Merge Sales and User Data
        merged_df = pd.merge(sales_df, user_df, left_on='customer_id', right_on='id', how='left')
        merged_df = merged_df.drop(columns=['id'])
        print("\nMerged Sales and User Data:\n", merged_df.head())
        return merged_df
    except Exception as e:
        print(f"Error in data loading/merging: {e}")
        return None

def perform_aggregations_and_store(final_df):
    """Performs data aggregations and stores the final data in database."""
    if final_df is None:
        return False
    try:
        # Data Aggregations
        final_df['total_sales'] = final_df['quantity'] * final_df['price']
        customer_sales = final_df.groupby('customer_id')['total_sales'].sum().reset_index()
        print("\nTotal Sales Per Customer:\n", customer_sales)

        avg_order_quantity = final_df.groupby('product_id')['quantity'].mean().reset_index()
        print("\nAverage Order Quantity Per Product:\n", avg_order_quantity)

        top_selling_products = final_df.groupby('product_id')['quantity'].sum().nlargest(5).reset_index()
        print("\nTop Selling Products:\n", top_selling_products)

        top_selling_customers = final_df.groupby('customer_id')['total_sales'].sum().nlargest(5).reset_index()
        print("\nTop Selling Customers:\n", top_selling_customers)

        final_df['order_date'] = pd.to_datetime(final_df['order_date'])
        final_df['month'] = final_df['order_date'].dt.month
        monthly_sales = final_df.groupby('month')['total_sales'].sum().reset_index()
        print("\nMonthly Sales Trends:\n", monthly_sales)

        final_df['quarter'] = final_df['order_date'].dt.quarter
        quarterly_sales = final_df.groupby('quarter')['total_sales'].sum().reset_index()
        print("\nQuarterly Sales Trends:\n", quarterly_sales)

        # Store Data in SQLite Database
        conn = sqlite3.connect('sales_data.db')
        final_df.to_sql('sales', conn, if_exists='replace', index=False)
        conn.close()
        print("\nData Successfully Saved to 'sales_data.db' in table 'sales'")
        return final_df
    except Exception as e:
        print(f"Error in data aggregations and store: {e}")
        return None

def fetch_data_from_database():
    """Fetch and print the final data from the SQLite database."""
    try:
        conn = sqlite3.connect('sales_data.db')
        query = "SELECT * FROM sales"
        df = pd.read_sql_query(query, conn)
        conn.close()
        print("\nFinal Merged Data from Database:\n", df.head())
        return df
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return None

if __name__ == "__main__":
    merged_data = load_and_merge_data()
    if merged_data is not None:
        final_processed_data = perform_aggregations_and_store(merged_data)
        if final_processed_data is not None:
            fetch_data_from_database()