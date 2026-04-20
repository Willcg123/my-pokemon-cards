from pokemontcgsdk import Card, Set, RestClient
from pokemontcgsdk.tcgplayer import TCGPlayer
from typing import Optional
import pandas as pd
from multiprocessing import Pool, cpu_count
import time

# Configure SDK
RestClient.configure('c6658313-1b80-4d04-a195-fd12fb820f84')
TCGPlayer.__dataclass_fields__['updatedAt'].type = Optional[str]

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
            # Adding set info to the card row for easier filtering later
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

    # Use multiprocessing pool for the card processing
    with Pool(processes=cpu_count()) as pool:
        for idx, s_id in enumerate(set_ids):
            retry_count = 0
            success = False
            
            while retry_count < MAX_RETRIES_PER_SET and not success:
                try:
                    print(f"[{idx+1}/{len(set_ids)}] Fetching cards for set: {s_id}...", end="\r")
                    
                    # Fetch cards filtered by set ID
                    cards_in_set = Card.where(q=f'set.id:{s_id}')
                    
                    if cards_in_set:
                        processed_cards = pool.map(process_single_card, cards_in_set)
                        valid_rows = [r for r in processed_cards if r is not None]
                        all_card_rows.extend(valid_rows)
                    
                    success = True
                    
                except Exception as e:
                    retry_count += 1
                    wait_time = retry_count * 3
                    print(f"\nError fetching set {s_id} (Attempt {retry_count}/{MAX_RETRIES_PER_SET}): {e}")
                    if retry_count < MAX_RETRIES_PER_SET:
                        time.sleep(wait_time)
                    else:
                        print(f"Skipping set {s_id} after {MAX_RETRIES_PER_SET} failures.")

    # Finalizing
    if all_card_rows:
        df = pd.DataFrame(all_card_rows)
        print("\n\n--- Extraction Summary ---")
        print(f"Total Cards Collected: {len(df)}")
        print(f"Total Unique Sets Processed: {df['set_id'].nunique() if 'set_id' in df.columns else 0}")
        print(df.head())
        # Suggestion: Save periodically or at the end
        df.to_csv("pokemon_inventory.csv", index=False)
    else:
        print("No card data was collected.")

if __name__ == '__main__':
    main()