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
api_key = input("input API key: ")

RestClient.configure(api_key)
TCGPlayer.__dataclass_fields__['updatedAt'].type = Optional[str]
TCGPlayer.__dataclass_fields__['updatedAt'].default = None

def process_single_card(card):
    """Worker function to flatten card objects."""
    try:
        row = {
            "id": card.id,
            "name": card.name,
            "number": card.number,
            "rarity": getattr(card, 'rarity', 'Unknown'),
            "hp": getattr(card, 'hp', None),
            "types": ", ".join(card.types) if hasattr(card, 'types') and card.types else None,
            "set_id": card.set.id if hasattr(card, 'set') else None 
        }
        tcg = getattr(card, 'tcgplayer', None)
        if tcg and hasattr(tcg, 'prices'):
            prices = tcg.prices
            row["market_price_normal"] = getattr(prices.normal, 'market', None) if hasattr(prices, 'normal') else None
            row["market_price_holofoil"] = getattr(prices.holofoil, 'market', None) if hasattr(prices, 'holofoil') else None
            row["market_price_reverse"] = getattr(prices.reverseHolofoil, 'market', None) if hasattr(prices, 'reverseHolofoil') else None
        return row
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
                        processed_cards = pool.map(process_single_card, cards_in_set)
                        all_card_rows.extend([r for r in processed_cards if r is not None])
                    
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
        
        # 3. Optimize with Index
        conn.execute("CREATE INDEX IF NOT EXISTS idx_card_date ON prices (id, date_fetched)")
        
        conn.commit()
        conn.close()
        
        print(f"Success! {len(df)} cards updated for {today_str}.")
    else:
        print("\nNo data collected.")

if __name__ == '__main__':
    main()