from pokemontcgsdk import Card, RestClient
from pokemontcgsdk.tcgplayer import TCGPlayer
from typing import Optional
import pandas as pd
from multiprocessing import Pool
import time
import requests
from bs4 import BeautifulSoup
import re
import urllib.parse

# 1. Configure API
api_key = input("Enter your Pokémon TCG API key: ").strip()
RestClient.configure(api_key)

# Fix for missing updatedAt in SDK
TCGPlayer.__dataclass_fields__['updatedAt'].type = Optional[str]
TCGPlayer.__dataclass_fields__['updatedAt'].default = None

def get_ebay_sold_price(card_name, card_number, set_name):
    """Scrapes recent eBay sold listings to calculate an estimated market price."""
    # 1. Clean the card number to just the base digits (e.g., '001/053' -> '001')
    clean_number = str(card_number).split('/')[0] if card_number else ""
    
    # 2. Use set_name (like 'Perfect Order') instead of the full set_id
    query = f"{card_name} {clean_number} {set_name} pokemon raw"
    encoded_query = urllib.parse.quote_plus(query)
    
    url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_Sold=1&LH_Complete=1"
    
    # Headers to make the request look like a real browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # If eBay explicitly tells us there are no matches, stop looking
        if "No exact matches found" in soup.text or "0 results" in soup.text:
            return None

        price_elements = soup.find_all('span', class_='s-item__price')
        
        prices = []
        for element in price_elements:
            text = element.text.replace('$', '').replace(',', '')
            match = re.search(r'[\d\.]+', text)
            if match:
                price = float(match.group())
                if price > 0:  # Exclude zero-dollar glitches
                    prices.append(price)
        
        # 3. Smarter list slicing: Skip hidden aggregate element if plenty of results
        if len(prices) > 2:
            valid_prices = prices[1:6] # Skip the hidden item, take the next 5
        else:
            valid_prices = prices[:5]  # Take whatever we can get
        
        if valid_prices:
            average_price = sum(valid_prices) / len(valid_prices)
            return round(average_price, 2)
            
        return None
        
    except Exception:
        return None

def process_single_card(card):
    """Worker function to create a row for prices, falling back to eBay if needed."""
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
            "set_name": card.set.name if hasattr(card, 'set') else None
        }

        tcg = getattr(card, 'tcgplayer', None)
        prices = getattr(tcg, 'prices', None) if tcg else None

        # Attempt official API pricing first
        if prices:
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
                    row = base_info.copy()
                    row["price_type"] = label
                    row["market_price"] = price_obj.market
                    rows.append(row)
        
        # If API fails, fall back to eBay scraping
        if not rows:
            # Add a slight delay to avoid triggering eBay's anti-bot protection
            time.sleep(1)
            
            scraped_price = get_ebay_sold_price(card.name, card.number, base_info["set_id"])
            
            row = base_info.copy()
            row["price_type"] = "eBay Sold Avg"
            row["market_price"] = scraped_price
            rows.append(row)

        return rows
    except Exception:
        return None

def main():
    # Prompt for multiple Set IDs separated by commas
    user_input = input("Enter Set IDs separated by commas (e.g., base1, me3, sv1): ").strip()
    
    if not user_input:
        print("No Set IDs provided. Exiting.")
        return
        
    target_set_ids = [s.strip() for s in user_input.split(',') if s.strip()]
    
    all_card_rows = []
    MAX_RETRIES = 3
    
    # Restrict the pool to 3 workers to prevent rate-limiting by eBay
    WORKER_COUNT = 3
    
    for target_set_id in target_set_ids:
        retry_count = 0
        success = False
        
        while retry_count < MAX_RETRIES and not success:
            try:
                print(f"\nFetching cards for set: {target_set_id}...")
                
                cards_in_set = Card.where(q=f'set.id:{target_set_id}')
                
                if not cards_in_set:
                    print(f"No cards found for set ID '{target_set_id}'. Skipping.")
                    break 
                    
                print(f"Found {len(cards_in_set)} cards in {target_set_id}. Processing prices safely...")
                
                # Process with limited concurrency
                with Pool(processes=WORKER_COUNT) as pool:
                    results = pool.map(process_single_card, cards_in_set)
                    
                    for card_rows in results:
                        if card_rows is not None:
                            all_card_rows.extend(card_rows)
                
                success = True
                
            except Exception as e:
                retry_count += 1
                print(f"Error occurred with {target_set_id}: {e}. Retrying ({retry_count}/{MAX_RETRIES})...")
                time.sleep(retry_count * 2)
                
        if not success and retry_count >= MAX_RETRIES:
            print(f"Failed to fetch set {target_set_id} after {MAX_RETRIES} attempts.")

    # Output the results to a single combined CSV file
    if all_card_rows:
        df = pd.DataFrame(all_card_rows)
        
        if len(target_set_ids) == 1:
            output_filename = f"{target_set_ids[0]}_prices.csv"
        else:
            output_filename = f"combined_{len(target_set_ids)}_sets_prices.csv"
            
        df.to_csv(output_filename, index=False)
        print(f"\nSuccess! Saved a total of {len(df)} rows to '{output_filename}'")
    else:
        print("\nNo valid card data was extracted from any of the provided sets.")

if __name__ == '__main__':
    main()