import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def test_single_card_ebay(card_name, card_number, set_id):
    print("=========================================")
    print(f"Testing Card: {card_name} #{card_number} from Set: {set_id}")

    # Clean the card number (e.g., '001/053' -> '001')
    clean_number = str(card_number).split('/')[0] if card_number else ""
    search_item = f"{card_name} {clean_number} {set_id} pokemon"
    formatted_item = "+".join(search_item.split())
    
    print(f"Search Query: {search_item}")

    price_list = []
    item_results = []
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    # Checking the first 2 pages (adjust the range if you want to test deeper)
    for i in range(1, 2):  
        print(f"Scraping Page {i}...")
        
        # URL using your format (removed the strict PSA filter to allow unreleased/raw ME3 cards to appear)
        url = f"https://www.ebay.com/sch/i.html?_nkw={formatted_item}&_sacat=0&_from=R40&LH_Complete=1&LH_Sold=1&Language=English&rt=nc&_pgn={i}"
        
        try:
            page = requests.get(url, headers=headers, timeout=10).text
            doc = BeautifulSoup(page, "html.parser")

            page_results = doc.find(class_="srp-results srp-list clearfix")
            if not page_results:
                print(f"  No result container found on page {i}. Stopping pagination.")
                break

            listings = page_results.find_all("li", class_="s-item s-item__pl-on-bottom")
            print(f"  Found {len(listings)} raw listings on page {i}.")

            for item in listings:
                try:
                    title_elem = item.find(class_="s-item__title")
                    if not title_elem or "Shop on eBay" in title_elem.text:
                        continue
                    title = title_elem.text
                    
                    price_elem = item.find(class_="s-item__price")
                    if not price_elem:
                        continue
                    price = price_elem.text

                    date_elem = item.find(class_="POSITIVE")
                    date = date_elem.string.replace("Sold ", "").strip() if date_elem else "Unknown Date"
                    
                    link_elem = item.find(class_="s-item__link")
                    link = link_elem['href'].split("?")[0] if link_elem else "No Link"

                    original_price = price
                    if price:
                        price = price.replace("$", "").replace(",", "")
                        if 'to' in price:
                            nums = [float(p) for p in price.split() if p.replace('.', '', 1).isdigit()]
                            if nums:
                                price = round(sum(nums) / len(nums), 2)
                        else:
                            # Regex to extract the raw number
                            match = re.search(r'[\d\.]+', price)
                            if match:
                                price = float(match.group())
                            else:
                                continue
                        
                        # Add valid prices to our list
                        if price > 0:
                            price_list.append(price)

                    item_results.append({
                        "Title": title,
                        "Price": price,
                        "Original Price Text": original_price,
                        "Date": date,
                        "Link": link
                    })
                except Exception as e:
                    continue
        except Exception as e:
            print(f"Error requesting page {i}: {e}")

    # Print Analytics
    if price_list:
        print(f"\nResults for {search_item}:")
        print("  Total Valid Prices Extracted:", len(price_list))
        print("  Highest Price:", max(price_list))
        print("  Lowest Price:", min(price_list))
        print("  Average Price:", round(sum(price_list) / len(price_list), 2))
    else:
        print(f"\n{search_item}: No items found.")

    # Save to Excel File
    if item_results:
        df_item = pd.DataFrame(item_results)
        excel_filename = f"test_{set_id}_{clean_number}_sold_data.xlsx"
        
        with pd.ExcelWriter(excel_filename, engine='xlsxwriter') as writer:
            # Clean up the sheet name to ensure Excel accepts it (max 31 chars, no special characters)
            safe_sheet_name = re.sub(r'[\\/*?:\[\]]', '', search_item)[:31]
            df_item.to_excel(writer, sheet_name=safe_sheet_name, index=False)
        
        print(f"\nSaved raw data for {len(item_results)} listings to {excel_filename}")
    
    print("=========================================\n")

# --- Run Your Tests Here ---

# Test 1: A standard card to ensure your connection works
test_single_card_ebay("Clefairy", "94", "Perfect Order")

# Test 2: Swap these variables out with whatever specific ME3 card is failing in your main script
# test_single_card_ebay("CARD_NAME", "CARD_NUMBER", "me3")