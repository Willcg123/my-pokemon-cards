from pokemontcgsdk import Card, Set, RestClient
from pokemontcgsdk.tcgplayer import TCGPlayer
from typing import Optional
import pandas as pd
from multiprocessing import Pool, cpu_count
import time
import sqlite3
from datetime import datetime

# --- SDK WORKAROUND ---
# Fix the SDK crash where 'updatedAt' is missing from some cards
api_key = input("Enter your Pokémon TCG API key: ").strip()

RestClient.configure(api_key)
TCGPlayer.__dataclass_fields__['updatedAt'].type = Optional[str]
TCGPlayer.__dataclass_fields__['updatedAt'].default = None

def process_single_card(card):
    """Worker function to create a separate row for each price variant."""
    try:
        rows = []
        
        # Common data for all rows of this card
        base_info = {
            "id": card.id,
            "name": card.name,
            "number": card.number,
            "rarity": getattr(card, 'rarity', 'Unknown'),
            "hp": getattr(card, 'hp', None),
            "types": ", ".join(card.types) if hasattr(card, 'types') and card.types else None,
            "set_id": card.set.id if hasattr(card, 'set') else None,
            "set_name": card.set.name if hasattr(card, 'set') else None  # <-- Added set_name here
        }

        tcg = getattr(card, 'tcgplayer', None)
        prices = getattr(tcg, 'prices', None) if tcg else None

        if prices:
            # Define the price types we want to check
            price_types = [
                ('normal', 'Normal'),
                ('holofoil', 'Holofoil'),
                ('reverseHolofoil', 'Reverse Holo'),
                ('firstEditionHolofoil', '1st Edition Holo'),
                ('firstEditionNormal', '1st Edition Normal')
            ]

            for attr, label in price_types:
                price_obj = getattr(prices, attr, None)
                if price_obj and hasattr(price_obj, 'market') and price_obj.market is not None:
                    # Create a specific row for this price type
                    row = base_info.copy()
                    row["price_type"] = label
                    row["market_price"] = price_obj.market
                    rows.append(row)
        
        # If no prices were found, ensure the card is still recorded
        if not rows:
            row = base_info.copy()
            row["price_type"] = "Unknown"
            row["market_price"] = None
            rows.append(row)

        return rows
    except Exception:
        return None

def main():
    print("Fetching all set IDs...")
    try:
        all_sets = Set.all()
        set_ids = [s.id for s in all_sets]
        print(f"Found {len(set_ids)} sets to process.")
    except Exception as e:
        print(f"Failed to fetch sets: {e}")
        return

    all_card_rows = []
    MAX_RETRIES_PER_SET = 3

    # Use multiprocessing for the extraction
    with Pool(processes=cpu_count()) as pool:
        for idx, s_id in enumerate(set_ids):
            retry_count = 0
            success = False
            
            while retry_count < MAX_RETRIES_PER_SET and not success:
                try:
                    print(f"[{idx+1}/{len(set_ids)}] Fetching set: {s_id}...", end="\r")
                    
                    # Fetch cards by set
                    cards_in_set = Card.where(q=f'set.id:{s_id}')
                    
                    if cards_in_set:
                        # pool.map returns a list of lists: [[row1, row2], [row3], ...]
                        results = pool.map(process_single_card, cards_in_set)
                        
                        # Flatten the lists and filter out any None results from failed cards
                        for card_rows in results:
                            if card_rows is not None:
                                all_card_rows.extend(card_rows)
                    
                    success = True
                    
                except Exception as e:
                    retry_count += 1
                    time.sleep(retry_count * 2)
                    if retry_count >= MAX_RETRIES_PER_SET:
                        print(f"\nSkipped set {s_id} after {MAX_RETRIES_PER_SET} fails.")

    if all_card_rows:
        df = pd.DataFrame(all_card_rows)
        
        # --- ROBUST DATABASE LOGIC ---
        print("\n\nSaving to SQLite database...")
        df['date_fetched'] = datetime.now().strftime('%Y-%m-%d')
        today_str = df['date_fetched'].iloc[0] 
        
        conn = sqlite3.connect('pokemon_market.db')
        cursor = conn.cursor()

        # 1. Check if table exists before trying to delete
        cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='prices'")
        if cursor.fetchone()[0] == 1:
            cursor.execute("DELETE FROM prices WHERE date_fetched = ?", (today_str,))
            print(f"Cleared existing entries for {today_str} to prevent duplicates.")
        else:
            print("First run: Creating new 'prices' table.")
        
        # 2. Append new data (Creates table if it doesn't exist)
        df.to_sql('prices', conn, if_exists='append', index=False)
        
        # 3. Optimize with Indexing for high-speed queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_card_date ON prices (id, date_fetched)")
        
        conn.commit()
        conn.close()
        
        print(f"Success! {len(df)} rows updated for {today_str}.")
    else:
        print("\nNo data collected.")

if __name__ == '__main__':
    main()