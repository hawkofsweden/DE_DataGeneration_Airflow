import pytest
import pandas as pd
from customer_data import generate_customer_data
from store_data import generate_store_data, european_locations
from menu_data import menu_data, coffee_products

def test_customer_data_generation():
    num_rows = 50
    df = generate_customer_data(num_rows=num_rows)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == num_rows
    assert 'customer_id' in df.columns
    assert 'customer_email' in df.columns

def test_store_data_generation():
    num_stores = 10
    df = generate_store_data(num_stores=num_stores)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == num_stores
    
    # Check if city belongs to the country's list in european_locations
    for _, row in df.iterrows():
        country = row['store_country']
        city = row['store_city']
        assert city in european_locations[country]

def test_menu_data_generation():
    df = menu_data(coffee_products)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert 'product_id' in df.columns
    assert 'unit_price' in df.columns
    assert df['unit_price'].dtype == 'float64'
