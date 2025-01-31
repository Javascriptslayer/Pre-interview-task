import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os
import sys

# Replace with your actual API key
OPENWEATHER_API_KEY = "e43f3e27126063f9ebfb5c6741771886"

def is_valid_coordinate(coord):
    """Checks if a coordinate is a number and within a reasonable range."""
    if coord is None or not isinstance(coord, (int, float)):
        return False
    return -180 <= coord <= 180

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
        user_df['lat'] = user_df['address'].apply(lambda x: float(x['geo']['lat']) if isinstance(x, dict) else None)
        user_df['lng'] = user_df['address'].apply(lambda x: float(x['geo']['lng']) if isinstance(x, dict) else None)
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

def fetch_and_add_weather_data(merged_df):
    """Fetches weather data for each sale location and adds to the merged dataframe."""
    if merged_df is None:
        return None
    weather_data_list = []
    if not OPENWEATHER_API_KEY or OPENWEATHER_API_KEY == "your_api_key":
        print("Error: OPENWEATHER_API_KEY is not set or set to default. Please set a valid API key.")
        return None
    
    for _, row in merged_df.iterrows():
        lat = row['lat']
        lng = row['lng']
        if pd.notna(lat) and pd.notna(lng):
            try:
                lat = float(lat)
                lng = float(lng)
                if is_valid_coordinate(lat) and is_valid_coordinate(lng):
                    weather_url = "https://api.openweathermap.org/data/2.5/weather"
                    weather_params = {
                        'lat': lat,
                        'lon': lng,
                        'appid': OPENWEATHER_API_KEY,
                        'units': 'metric'
                    }
                    weather_response = requests.get(weather_url, params=weather_params)
                    weather_response.raise_for_status()
                    weather_data = weather_response.json()
                    temp = weather_data.get('main', {}).get('temp')
                    conditions = weather_data.get('weather', [{}])[0].get('description')
                    weather_data_list.append({'temp': temp, 'conditions': conditions})
                else:
                    weather_data_list.append({'temp': None, 'conditions': None})
            except Exception:
                weather_data_list.append({'temp': None, 'conditions': None})
        else:
            weather_data_list.append({'temp': None, 'conditions': None})

    weather_df = pd.DataFrame(weather_data_list)
    final_df = pd.concat([merged_df.reset_index(drop=True), weather_df], axis=1)
    print("\nMerged Data with Weather:\n", final_df.head())
    return final_df

def perform_aggregations_and_store(final_df):
    """Performs data aggregations and stores the final data in the database."""
    required_columns = {"quantity", "price", "order_date"}
    if final_df is None or not required_columns.issubset(final_df.columns):
        return None

    try:
        final_df["total_sales"] = final_df["quantity"] * final_df["price"]
        final_df["order_date"] = pd.to_datetime(final_df["order_date"])
        final_df["month"] = final_df["order_date"].dt.month
        final_df["quarter"] = final_df["order_date"].dt.quarter
         # Calculate total sales amount per customer
        customer_sales = final_df.groupby('customer_id')['total_sales'].sum().reset_index()
        print("\nTotal Sales Amount per Customer:")
        print(customer_sales)
        
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
        final_data = fetch_and_add_weather_data(merged_data)
        if final_data is not None:
            final_processed_data = perform_aggregations_and_store(final_data)
            if final_processed_data is not None:
                fetch_data_from_database()
    sys.exit()