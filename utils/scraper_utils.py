import httpx
import asyncio # Not strictly used but often good for async util files
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
        # Explicitly DO NOT let tenacity retry 403 here; it should propagate to fetch_page_fresh_ip
        if exception_value.response.status_code == 403:
            logger.debug(f"ScraperUtils: HTTPStatusError 403 for {exception_value.request.url}. Not retrying at this level; allowing propagation.")
            return False 
            
        should_retry = exception_value.response.status_code in RETRYABLE_STATUS_CODES_UTILS
        if should_retry:
            logger.warning(f"ScraperUtils: HTTPStatusError with retryable status code {exception_value.response.status_code} for URL {exception_value.request.url}. Tenacity will retry.")
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
    reraise=True # Essential: if tenacity exhausts retries, it re-raises the last exception
)
async def get_company_profile_data_async(
    url: str,
    client: httpx.AsyncClient,
    user_agent_string: Optional[str] = None 
) -> tuple[Optional[Dict[str, Any]], Optional[int]]:
    
    headers_for_request = client.headers.copy()
    if user_agent_string:
        headers_for_request["User-Agent"] = user_agent_string
    
    
    try:
        response = await client.get(url, headers=headers_for_request) 
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
            logger.warning(f"Async: No 'businessUnit' key found in pageProps for profile data from {url}. Page props sample: {str(page_props)[:500]}")
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

    except json.JSONDecodeError as json_err: # Specific error for JSON parsing failure
        logger.error(f"Async: Failed to decode JSON for profile data from {url}: {json_err}")
        if 'response' in locals() and hasattr(response, 'text'):
             logger.debug(f"Async: Content that failed to parse (profile data): {response.text[:500]}...")
        return None, None 
    except (AttributeError, KeyError, TypeError) as e_parse: # Specific errors for __NEXT_DATA__ structure issues
        logger.error(f"Async: Error parsing __NEXT_DATA__ structure for profile data from {url}: {e_parse}")
        return None, None
    # No generic "except Exception:" here. HTTPStatusError (like 403) or other httpx.RequestError
    # that tenacity doesn't retry (or exhausts retries for) will propagate up.


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=(
        retry_if_exception_type(RETRYABLE_HTTPX_EXCEPTIONS_UTILS) |
        retry_if_exception(_predicate_should_retry_httpx_status_error_utils)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True # Essential: if tenacity exhausts retries, it re-raises the last exception
)
async def get_reviews_from_page_async(
    url: str,
    client: httpx.AsyncClient,
    user_agent_string: Optional[str] = None
) -> tuple[List[Dict[str, Any]], Optional[int]]:

    headers_for_request = client.headers.copy()
    if user_agent_string:
        headers_for_request["User-Agent"] = user_agent_string

    # logger.info(f"Async: Attempting to fetch reviews from: {url} (UA: {headers_for_request.get('User-Agent')})")
    try:
        response = await client.get(url, headers=headers_for_request)
        response.raise_for_status() # Let this raise HTTPStatusError
        # logger.debug(f"Async: Successfully fetched page content for reviews from {url}. Status code: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'lxml')
        reviews_script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")

        if not reviews_script_tag or not reviews_script_tag.string:
            logger.warning(f"Async: Could not find __NEXT_DATA__ script tag or it's empty on {url} for reviews.")
            return [], None

        reviews_raw_json = json.loads(reviews_script_tag.string)
        page_props = reviews_raw_json.get("props", {}).get("pageProps", {})
        reviews = page_props.get("reviews")
        
        total_pages_from_this_page = None
        filters_data = page_props.get("filters", {}).get("pagination", {})
        if filters_data and "totalPages" in filters_data:
            total_pages_from_this_page = filters_data["totalPages"]

        if reviews is None: 
            logger.warning(f"Async: No 'reviews' key found or it is None in pageProps for {url}. Page props sample: {str(page_props)[:500]}")
            return [], total_pages_from_this_page

        # logger.info(f"Async: Successfully parsed {len(reviews)} reviews from __NEXT_DATA__ on {url}") # Worker logs this
        return reviews, total_pages_from_this_page

    except json.JSONDecodeError as json_err: # Specific error for JSON parsing failure
        logger.error(f"Async: Failed to decode JSON for reviews from {url}: {json_err}")
        if 'response' in locals() and hasattr(response, 'text'):
            logger.debug(f"Async: Content that failed to parse (reviews): {response.text[:500]}...")
        return [], None
    except (AttributeError, KeyError, TypeError) as e_parse: # Specific errors for __NEXT_DATA__ structure issues
        logger.error(f"Async: Error parsing __NEXT_DATA__ structure for reviews from {url}: {e_parse}")
        return [], None