import httpx
import asyncio
from bs4 import BeautifulSoup
import json
import logging
from urllib.parse import urlparse, urlencode
from typing import Optional, List, Dict, Any

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log
)

logger = logging.getLogger(__name__)

RETRYABLE_HTTPX_EXCEPTIONS_UTILS = (
    httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ConnectError,
    httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError, httpx.NetworkError,
)
RETRYABLE_STATUS_CODES_UTILS = (429, 500, 502, 503, 504)

def _predicate_should_retry_httpx_status_error_utils(exception_value: BaseException) -> bool:
    if isinstance(exception_value, httpx.HTTPStatusError):
        should_retry = exception_value.response.status_code in RETRYABLE_STATUS_CODES_UTILS
        if should_retry:
            logger.warning(f"ScraperUtils: HTTPStatusError with retryable status code {exception_value.response.status_code} for URL {exception_value.request.url}. Will retry.")
        return should_retry
    return False


def _prepare_url_for_page(base_url_str: str, page_num: int, languages: Optional[str] = "all") -> str:
    parsed_original_url = urlparse(base_url_str)
    clean_base_url_for_query = parsed_original_url._replace(query="", fragment="").geturl()
    query_params_to_set: Dict[str, Any] = {}
    query_params_to_set['page'] = str(page_num)
    if languages:
        query_params_to_set['languages'] = languages
    new_query_string = urlencode(query_params_to_set, doseq=False)
    final_parsed_url = urlparse(clean_base_url_for_query)
    return final_parsed_url._replace(query=new_query_string).geturl()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=(
        retry_if_exception_type(RETRYABLE_HTTPX_EXCEPTIONS_UTILS) |
        retry_if_exception(_predicate_should_retry_httpx_status_error_utils)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True
)
async def get_company_profile_data_async(
    url: str,
    client: httpx.AsyncClient # Expect an httpx.AsyncClient
) -> tuple[Optional[Dict[str, Any]], Optional[int]]:
    logger.info(f"Async: Attempting to fetch company profile data from: {url}")
    try:
        response = await client.get(url)
        response.raise_for_status()
        logger.debug(f"Async: Successfully fetched page content for profile data from {url}. Status code: {response.status_code}")

        soup = BeautifulSoup(response.text, 'lxml') 
        script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json") 

        if not script_tag or not script_tag.string:
            logger.warning(f"Async: Could not find __NEXT_DATA__ script tag or it's empty on {url} for profile data.")
            return None, None

        raw_json = json.loads(script_tag.string) 
        page_props = raw_json.get("props", {}).get("pageProps", {})
        business_unit_data = page_props.get("businessUnit")
        
        total_review_pages = None
        filters_data = page_props.get("filters", {}).get("pagination", {})
        if filters_data and "totalPages" in filters_data:
            total_review_pages = filters_data["totalPages"]
            logger.info(f"Async: Found total review pages from profile load ({url}): {total_review_pages}")

        if not business_unit_data:
            logger.warning(f"Async: No 'businessUnit' key found in pageProps for profile data from {url}. Page props (first 500 chars): {str(page_props)[:500]}")
            return None, total_review_pages

        profile_info = {
            "id": business_unit_data.get("id"),
            "displayName": business_unit_data.get("displayName"),
            "identifyingName": business_unit_data.get("identifyingName"),
            "numberOfReviews": business_unit_data.get("numberOfReviews"),
            "trustScore": business_unit_data.get("trustScore"),
            "websiteUrl": business_unit_data.get("websiteUrl"),
            "stars": business_unit_data.get("stars")
        }
        
        logger.info(f"Async: Successfully parsed company profile data from {url}")
        return profile_info, total_review_pages

    except json.JSONDecodeError as json_err:
        logger.error(f"Async: Failed to decode JSON for profile data from {url}: {json_err}")
 
        if 'script_tag' in locals() and script_tag and script_tag.string:
             logger.debug(f"Async: Content that failed to parse (profile data): {script_tag.string[:500]}...")
        return None, None 
    except (AttributeError, KeyError, TypeError) as e:
        logger.error(f"Async: Error parsing __NEXT_DATA__ structure for profile data from {url}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Async: An unexpected error occurred in get_company_profile_data_async for {url}: {e}", exc_info=True)
        return None, None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=(
        retry_if_exception_type(RETRYABLE_HTTPX_EXCEPTIONS_UTILS) |
        retry_if_exception(_predicate_should_retry_httpx_status_error_utils)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True
)
async def get_reviews_from_page_async(
    url: str,
    client: httpx.AsyncClient
) -> tuple[List[Dict[str, Any]], Optional[int]]:
    logger.info(f"Async: Attempting to fetch reviews from: {url}")
    try:
        response = await client.get(url)
        response.raise_for_status()
        logger.debug(f"Async: Successfully fetched page content for reviews from {url}. Status code: {response.status_code}")
        

        soup = BeautifulSoup(response.text, 'lxml') # Sync
        reviews_script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json") # Sync

        if not reviews_script_tag or not reviews_script_tag.string:
            logger.warning(f"Async: Could not find __NEXT_DATA__ script tag or it's empty on {url} for reviews.")
            return [], None

        reviews_raw_json = json.loads(reviews_script_tag.string) # Sync
        page_props = reviews_raw_json.get("props", {}).get("pageProps", {})
        reviews = page_props.get("reviews")
        
        total_pages_from_this_page = None
        filters_data = page_props.get("filters", {}).get("pagination", {})
        if filters_data and "totalPages" in filters_data:
            total_pages_from_this_page = filters_data["totalPages"]

        if reviews is None: # Check for None explicitly, as an empty list is valid (no reviews on page)
            logger.warning(f"Async: No 'reviews' key found or it is None in pageProps for {url}. Page props (reviews): {str(page_props)[:500]}")
            return [], total_pages_from_this_page # Return empty list for reviews

        logger.info(f"Async: Successfully parsed {len(reviews)} reviews from __NEXT_DATA__ on {url}")
        return reviews, total_pages_from_this_page

    except json.JSONDecodeError as json_err:
        logger.error(f"Async: Failed to decode JSON for reviews from {url}: {json_err}")
        if 'reviews_script_tag' in locals() and reviews_script_tag and reviews_script_tag.string:
            logger.debug(f"Async: Content that failed to parse (reviews): {reviews_script_tag.string[:500]}...")
        return [], None # Or re-raise
    except (AttributeError, KeyError, TypeError) as e:
        logger.error(f"Async: Error parsing __NEXT_DATA__ structure for reviews from {url}: {e}")
        return [], None
    except Exception as e:
        logger.error(f"Async: An unexpected error occurred in get_reviews_from_page_async for {url}: {e}", exc_info=True)
        return [], None