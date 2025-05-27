import requests
from bs4 import BeautifulSoup
import json
import time
# import pandas as pd # No longer directly needed for saving JSON files per page
import logging
import os # Added for directory and path operations
from urllib.parse import urlparse # Added for URL parsing

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()]) # Outputs logs to console

logger = logging.getLogger(__name__)

def get_reviews_from_page(url):
    logger.info(f"Attempting to fetch reviews from: {url}")
    try:
        req = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"})
        req.raise_for_status()
        logger.debug(f"Successfully fetched page content from {url}. Status code: {req.status_code}")

        time.sleep(2) # Respectful delay

        soup = BeautifulSoup(req.text, 'html.parser')
        reviews_script_tag = soup.find("script", id="__NEXT_DATA__")

        if not reviews_script_tag:
            logger.warning(f"Could not find the __NEXT_DATA__ script tag on {url}")
            return []

        if not reviews_script_tag.string:
            logger.warning(f"__NEXT_DATA__ script tag has no content on {url}")
            return []

        reviews_raw_json = json.loads(reviews_script_tag.string)

        page_props = reviews_raw_json.get("props", {}).get("pageProps", {})
        reviews = page_props.get("reviews")

        if reviews is None:
            logger.warning(f"No 'reviews' key found or it is None in pageProps for {url}. Page props (first 500 chars): {str(page_props)[:500]}")
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
        if reviews_script_tag and reviews_script_tag.string:
            logger.debug(f"Content that failed to parse (first 500 chars): {reviews_script_tag.string[:500]}...")
        else:
            logger.debug(f"Content that failed to parse was empty or __NEXT_DATA__ tag not found.")
    except AttributeError as attr_err:
        logger.error(f"AttributeError while parsing page {url}: {attr_err}. This might indicate a change in page structure.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_reviews_from_page for {url}: {e}", exc_info=True)
    return []

def scrape_trustpilot_reviews(base_url: str):
    logger.info(f"Starting Trustpilot scraping for base URL: {base_url}")

    # Extract company name for folder creation
    parsed_url = urlparse(base_url)
    # Example path for "https://www.trustpilot.com/review/example.com" is "/review/example.com"
    # path_segments will be ['review', 'example.com']
    path_segments = [segment for segment in parsed_url.path.split('/') if segment]

    company_name = "unknown_company_trustpilot" # Default value
    if len(path_segments) >= 2 and path_segments[0].lower() == 'review':
        company_name = path_segments[1] # Expected company name from URL like /review/company.name
        logger.info(f"Extracted company name: {company_name}")
    else:
        logger.warning(
            f"Could not extract company name using standard path '/review/company_name' from {base_url} "
            f"(path segments: {path_segments}). Using default folder name: '{company_name}'."
        )

    output_directory = company_name
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: ./{output_directory}")
    except OSError as e:
        logger.error(f"Could not create directory ./{output_directory}: {e}")
        return None # Cannot proceed if directory creation fails

    page_number = 1
    pages_saved_count = 0
    max_pages_to_scrape = 200 # Safety limit to prevent accidental very long scrapes

    while page_number <= max_pages_to_scrape:
        url = f"{base_url.rstrip('/')}?page={page_number}"
        logger.info(f"Scraping page number: {page_number} from URL: {url}")

        reviews_on_page = get_reviews_from_page(url) # This returns a list of review dicts

        if not reviews_on_page: # True if reviews_on_page is an empty list
            if page_number == 1:
                logger.warning(f"No reviews found on the first page ({url}). Check the base URL, website structure, or if the company has any reviews.")
            else:
                # This could mean end of reviews, or an empty page encountered before max_pages.
                logger.info(f"No reviews found on page {page_number}. Assuming end of reviews or an issue. Ending scrape.")
            break # Exit loop

        # If we reach here, reviews_on_page is a non-empty list of reviews
        logger.info(f"Retrieved {len(reviews_on_page)} reviews from page {page_number}.")
        file_path = os.path.join(output_directory, f"page{page_number}.json")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(reviews_on_page, f, indent=4, ensure_ascii=False)
            logger.info(f"Successfully saved reviews to {file_path}")
            pages_saved_count += 1
        except IOError as e:
            logger.error(f"Could not write to file {file_path}: {e}")
        except Exception as e: # Catch other potential errors during json.dump
            logger.error(f"An unexpected error occurred while saving page {page_number} data to JSON: {e}", exc_info=True)

        page_number += 1
    
    if page_number > max_pages_to_scrape:
        logger.info(f"Reached maximum page limit of {max_pages_to_scrape}. Stopping scrape.")

    logger.info(f"Scraping finished. {pages_saved_count} page(s) of reviews saved in directory: ./{output_directory}")
    return output_directory

if __name__ == '__main__':
    # IMPORTANT: Replace "example.com" with a real Trustpilot company review page URL for actual testing.
    # e.g., "https://www.trustpilot.com/review/some-actual-company.com"
    target_base_url = "https://www.trustpilot.com/review/example.com"

    logger.info(f"--- Starting script for {target_base_url} ---")

    if "example.com" in target_base_url:
        logger.warning("Using a placeholder URL (example.com). This will likely not find real reviews or may create an 'example.com' folder with empty page files.")
        logger.warning("Please replace 'example.com' with a real domain for actual scraping.")
        # Forcing a run even with example.com to test directory creation:
        output_dir = scrape_trustpilot_reviews(target_base_url)
        if output_dir:
            logger.info(f"Script executed for placeholder URL. Check directory: ./{output_dir}")
        else:
            logger.info(f"Script execution for placeholder URL failed or did not create an output directory.")
    else:
        output_dir = scrape_trustpilot_reviews(target_base_url)
        if output_dir:
            logger.info(f"Review data (if any) for {target_base_url} has been attempted to be saved in directory: ./{output_dir}")
        else:
            logger.info(f"Scraping process for {target_base_url} did not complete successfully or output directory could not be determined/created.")

    logger.info(f"--- Script finished for {target_base_url} ---")