import sqlite3
import pandas as pd

def fetch_data(conn):
    """Fetches the raw 'long' data from the database."""
    query = "SELECT id, name, number, set_id, set_name, rarity, price_type, date_fetched, market_price FROM prices"
    df_long = pd.read_sql(query, conn)
    return df_long

def pivot_prices(df_long):
    """Pivots the long data into a wide format (dates as columns)."""
    df_pivot = df_long.pivot(
        index=['id', 'name', 'number', 'set_id', 'set_name', 'rarity', 'price_type'], 
        columns='date_fetched', 
        values='market_price'
    )
    # Reset the index so id, name, etc., become normal columns again
    df_pivot.reset_index(inplace=True)
    return df_pivot

def avg_pokemon_price(df_long, pokemon_name='Mew'):
    """Calculates the historical average price from the unpivoted long data."""
    # We use df_long here because the 'market_price' column still exists
    poke_df = df_long[df_long['name'].str.contains(pokemon_name)]
    
    if poke_df.empty:
        return None
        
    return poke_df['market_price'].mean()

def main():
    # Opening the connection using a context manager ensures it closes automatically
    with sqlite3.connect('pokemon_market.db') as conn:
        try:
            # 1. Get the raw data
            df_long = fetch_data(conn)
            
            # 2. Create the pivoted version for your other reporting needs
            df_pivot = pivot_prices(df_long)
            
            # 3. Calculate the average price using the raw long data
            mew_avg = avg_pokemon_price(df_long, 'Mew')
            
            if mew_avg is not None:
                print(f"Historical Average Price for Mew: ${mew_avg:.2f}")
            else:
                print("No data found for 'Mew' in the database.")
                
            # Optional: print(df_pivot.head()) to see your wide format
        except Exception as e:
            print(f"An error occurred: {e}")
    breakpoint()
    
if __name__ == '__main__':
    main()