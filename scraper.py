import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()]) # Outputs logs to console

logger = logging.getLogger(__name__)

def get_reviews_from_page(url):

    logger.info(f"Attempting to fetch reviews from: {url}")
    try:
        req = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}) # Using a more common user-agent
        req.raise_for_status()
        logger.debug(f"Successfully fetched page content from {url}. Status code: {req.status_code}")

        time.sleep(2) 
        
        soup = BeautifulSoup(req.text, 'html.parser')
        reviews_script_tag = soup.find("script", id="__NEXT_DATA__")
        
        if not reviews_script_tag:
            logger.warning(f"Could not find the __NEXT_DATA__ script tag on {url}")
            return []
            
        if not reviews_script_tag.string:
            logger.warning(f"__NEXT_DATA__ script tag has no content on {url}")
            return []

        reviews_raw_json = json.loads(reviews_script_tag.string)
        
        # Navigating through the JSON structure safely
        page_props = reviews_raw_json.get("props", {}).get("pageProps", {})
        reviews = page_props.get("reviews")

        if reviews is None: # Check if reviews key exists and is not None
            logger.warning(f"No 'reviews' key found in pageProps for {url}. Page props: {page_props}")
            return []
        
        logger.info(f"Successfully parsed {len(reviews)} reviews from {url}")
        return reviews
        
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching {url}: {http_err} - Status Code: {http_err.response.status_code}")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred while fetching {url}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred while fetching {url}: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected error occurred during requests to {url}: {req_err}")
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON from {url}: {json_err}")
        logger.debug(f"Content that failed to parse: {reviews_script_tag.string[:500]}...") # Log first 500 chars of problematic content
    except AttributeError as attr_err:
        logger.error(f"AttributeError while parsing page {url}: {attr_err}. This might indicate a change in page structure.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_reviews_from_page for {url}: {e}", exc_info=True)
    return []

def scrape_trustpilot_reviews(base_url: str):

    logger.info(f"Starting Trustpilot scraping for base URL: {base_url}")
    reviews_data = []
    page_number = 1
    
    while True:
        # Ensure base_url doesn't end with a slash to avoid double slashes
        url = f"{base_url.rstrip('/')}?page={page_number}"
        logger.info(f"Scraping page number: {page_number}")
        
        reviews_on_page = get_reviews_from_page(url)

        if not reviews_on_page:
            if page_number == 1:
                logger.warning(f"No reviews found on the first page ({url}). Check the base URL or website structure.")
            else:
                logger.info(f"No more reviews found on page {page_number}. Ending scrape.")
            break

        processed_count = 0
        for review in reviews_on_page:
            try:
                # Defensive checks for potentially missing keys
                published_date = review.get("dates", {}).get("publishedDate")
                consumer_info = review.get("consumer", {})
                display_name = consumer_info.get("displayName", "N/A")
                country_code = consumer_info.get("countryCode", "N/A")
                
                data = {
                    'Date': pd.to_datetime(published_date).strftime("%Y-%m-%d") if published_date else "N/A",
                    'Author': display_name,
                    'Body': review.get("text", "N/A"),
                    'Heading': review.get("title", "N/A"),
                    'Rating': review.get("rating", "N/A"),
                    'Location': country_code
                }
                reviews_data.append(data)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing a review on page {page_number}: {e}. Review data: {review}", exc_info=True)
        
        logger.info(f"Successfully processed {processed_count} reviews from page {page_number}.")
        page_number += 1


    logger.info(f"Total reviews collected before deduplication: {len(reviews_data)}")

    if reviews_data:
        df = pd.DataFrame(reviews_data)
        df_deduplicated = df.drop_duplicates(subset=['Body']) # Consider other fields for uniqueness if needed
        reviews_data_deduplicated = df_deduplicated.to_dict('records')
        num_duplicates_removed = len(reviews_data) - len(reviews_data_deduplicated)
        logger.info(f"Removed {num_duplicates_removed} duplicate reviews based on 'Body'.")
        reviews_data = reviews_data_deduplicated
    else:
        logger.info("No reviews collected, skipping deduplication.")
    
    logger.info(f"Scraping finished. Total unique reviews collected: {len(reviews_data)}")
    return reviews_data

if __name__ == '__main__':

    target_base_url = "https://www.trustpilot.com/review/example.com" # Replace with a real one for testing
    
    logger.info(f"--- Starting script for {target_base_url} ---")
    
    if target_base_url == "https://www.trustpilot.com/review/example.com":
        logger.warning("Using a placeholder URL. Please replace 'example.com' with a real domain for actual scraping.")
    else:
        all_reviews = scrape_trustpilot_reviews(target_base_url)
        if all_reviews:
            df_final_reviews = pd.DataFrame(all_reviews)
            logger.info(f"\n--- Collected Reviews ({len(df_final_reviews)}) ---")
            logger.info(df_final_reviews.head())
        else:
            logger.info("No reviews were collected.")
            df_final_reviews = pd.DataFrame() # Create empty DataFrame if no reviews

    logger.info(f"--- Script finished for {target_base_url} ---")