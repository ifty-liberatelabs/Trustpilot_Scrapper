# utils/total_review_pages.py

import requests
from bs4 import BeautifulSoup
import json
import logging
from typing import Optional

# Import the URL preparation helper from scraper_utils
# We'll ensure this helper is robust and available there.
from .scraper_utils import _prepare_url_for_page

logger = logging.getLogger(__name__)

def _fetch_and_extract_total_pages_from_next_data(url: str, session: requests.Session) -> Optional[int]:
    """
    Fetches a URL, parses __NEXT_DATA__, and extracts totalPages.
    Returns totalPages as int if found, else None.
    """
    logger.debug(f"Attempting to extract total pages from: {url}")
    try:
        # Add a small delay before each request in this utility too
        # import time
        # time.sleep(1) # Or use the delay from scraper_utils if centralized
        
        req = session.get(url)
        req.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        
        # Using 'lxml' as per your example, ensure it's in requirements.txt
        soup = BeautifulSoup(req.text, "lxml")
        script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")

        if not script_tag or not script_tag.string:
            logger.warning(f"Could not find __NEXT_DATA__ script tag or it's empty on {url}.")
            return None

        raw_json = json.loads(script_tag.string)
        page_props = raw_json.get("props", {}).get("pageProps", {})
        filters_data = page_props.get("filters", {}).get("pagination", {})
        total_review_pages = filters_data.get("totalPages")

        if total_review_pages is not None:
            logger.info(f"Found total review pages: {total_review_pages} from {url}")
            return int(total_review_pages)
        else:
            logger.info(f"Total review pages not found in __NEXT_DATA__ from {url}.")
            return None

    except requests.exceptions.HTTPError as http_err:
        # Log specific HTTP errors, e.g., 404 might mean the page doesn't exist
        logger.error(f"HTTP error occurred while fetching {url}: {http_err} - Status: {http_err.response.status_code}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error occurred while fetching {url}: {req_err}")
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON from {url}: {json_err}")
        if 'script_tag' in locals() and script_tag and script_tag.string: # type: ignore
            logger.debug(f"Content that failed to parse: {script_tag.string[:500]}...")
    except (AttributeError, KeyError, TypeError) as e: # More specific errors for parsing issues
        logger.error(f"Error parsing __NEXT_DATA__ structure from {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in _fetch_and_extract_total_pages_from_next_data for {url}: {e}", exc_info=True)
    
    return None


def determine_total_review_pages(base_review_url: str, session: requests.Session) -> Optional[int]:
    """
    Determines the total number of review pages for a company on Trustpilot.
    It first tries page 2 with 'languages=all', then page 1 with 'languages=all'.

    Args:
        base_review_url: The base URL for the company's review page 
                         (e.g., "https://www.trustpilot.com/review/www.example.com").
                         This URL should be the root review path, without query params.
        session: A requests.Session object to use for HTTP requests.

    Returns:
        The total number of pages as an int if found, otherwise None.
    """
    logger.info(f"Determining total review pages for: {base_review_url}")

    # Attempt 1: Check page 2 with languages=all
    # _prepare_url_for_page will take the base_review_url and correctly append query params
    url_page_2 = _prepare_url_for_page(base_review_url, 2, languages="all")
    logger.info(f"Attempt 1: Checking for total pages on: {url_page_2}")
    total_pages = _fetch_and_extract_total_pages_from_next_data(url_page_2, session)

    if total_pages is not None:
        logger.info(f"Total pages ({total_pages}) determined from page 2 logic ({url_page_2}).")
        return total_pages

    # Attempt 2: Check page 1 with languages=all (if page 2 failed or returned None)
    logger.info("Attempt 1 (page 2) did not yield total pages. Moving to Attempt 2 (page 1).")
    url_page_1 = _prepare_url_for_page(base_review_url, 1, languages="all")
    logger.info(f"Attempt 2: Checking for total pages on: {url_page_1}")
    total_pages = _fetch_and_extract_total_pages_from_next_data(url_page_1, session)
    
    if total_pages is not None:
        logger.info(f"Total pages ({total_pages}) determined from page 1 logic ({url_page_1}).")
        return total_pages
    
    logger.warning(f"Could not determine total review pages from either page 2 or page 1 logic for {base_review_url}.")
    return None