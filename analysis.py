import sqlite3
import pandas as pd

conn = sqlite3.connect('pokemon_market.db')

# 1. Load the "Long" data from the database
df_long = pd.read_sql("SELECT id, name, date_fetched, market_price_normal FROM prices", conn)

# 2. Pivot the table: Dates become columns, Card IDs stay as rows
df_pivot = df_long.pivot(index=['id', 'name'], columns='date_fetched', values='market_price_normal')

breakpoint()

print(df_pivot.head())