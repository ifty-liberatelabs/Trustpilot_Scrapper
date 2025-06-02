import httpx
import asyncio 
from bs4 import BeautifulSoup
import json
import logging
from typing import Optional
from utils.scraper_utils import _prepare_url_for_page 

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_exception,
    before_sleep_log
)

logger = logging.getLogger(__name__)

RETRYABLE_HTTPX_EXCEPTIONS_TOTAL_PAGES = (
    httpx.ReadTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.NetworkError,
)
RETRYABLE_STATUS_CODES_TOTAL_PAGES = (403, 429, 500, 502, 503, 504) # Added 403 here for tenacity if proxy fails

def _predicate_should_retry_httpx_status_error_total_pages(exception_value: BaseException) -> bool:
    if isinstance(exception_value, httpx.HTTPStatusError):
        should_retry = exception_value.response.status_code in RETRYABLE_STATUS_CODES_TOTAL_PAGES
        if should_retry:
            logger.warning(f"TotalPages: HTTPStatusError with retryable status code {exception_value.response.status_code} for URL {exception_value.request.url}. Tenacity will retry.")
        return should_retry
    return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=(
        retry_if_exception_type(RETRYABLE_HTTPX_EXCEPTIONS_TOTAL_PAGES) |
        retry_if_exception(_predicate_should_retry_httpx_status_error_total_pages)
    ),
    before_sleep=before_sleep_log(logger, logging.INFO),
    reraise=True
)
async def _fetch_and_extract_total_pages_from_next_data_async(
    url: str,
    client: httpx.AsyncClient,
    user_agent_string: Optional[str] = None # Allow passing UA
) -> Optional[int]:
    """
    Fetches a URL asynchronously, parses __NEXT_DATA__, and extracts totalPages.
    Returns totalPages as int if found, else None.
    """
    headers_for_request = client.headers.copy()
    if user_agent_string:
        headers_for_request["User-Agent"] = user_agent_string

    logger.debug(f"Async: Attempting to extract total pages from: {url} (UA: {headers_for_request.get('User-Agent')})")
    try:
        response = await client.get(url, headers=headers_for_request)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")

        if not script_tag or not script_tag.string:
            logger.warning(f"Async: Could not find __NEXT_DATA__ script tag or it's empty on {url} (total pages detection).")
            return None

        raw_json = json.loads(script_tag.string)
        page_props = raw_json.get("props", {}).get("pageProps", {})
        filters_data = page_props.get("filters", {}).get("pagination", {})
        total_review_pages = filters_data.get("totalPages")

        if total_review_pages is not None:
            logger.info(f"Async: Found total review pages: {total_review_pages} from __NEXT_DATA__ on {url}")
            return int(total_review_pages)
        else:
            logger.info(f"Async: Total review pages not found in __NEXT_DATA__ pagination from {url}.")
            return None

    except json.JSONDecodeError as json_err:
        logger.error(f"Async: Failed to decode JSON from {url} (total pages detection): {json_err}")
        if 'response' in locals() and hasattr(response, 'text'):
            logger.debug(f"Async: Content that failed to parse: {response.text[:500]}...")
        return None
    except (AttributeError, KeyError, TypeError) as e:
        logger.error(f"Async: Error parsing __NEXT_DATA__ structure from {url} (total pages detection): {e}")
        return None
    except Exception as e:
        logger.error(f"Async: An unexpected error in _fetch_and_extract_total_pages for {url}: {type(e).__name__} - {e}", exc_info=True)
        return None


async def determine_total_review_pages_async(
    base_review_url: str,
    client: httpx.AsyncClient,
) -> Optional[int]:

    logger.info(f"Async: Determining total review pages for: {base_review_url}")

    url_page_2 = _prepare_url_for_page(base_review_url, 2, languages="all")
    logger.info(f"Async: Attempt 1 (total pages): Checking on: {url_page_2}")
    try:
        total_pages = await _fetch_and_extract_total_pages_from_next_data_async(url_page_2, client) #, user_agent_string=current_ua_for_total_pages)
        if total_pages is not None:
            logger.info(f"Async: Total pages ({total_pages}) determined from page 2 logic ({url_page_2}).")
            return total_pages
    except Exception as e: 
        logger.error(f"Async: Attempt 1 (page 2 for total pages) failed after retries for {url_page_2}: {type(e).__name__} - {str(e)}")

    logger.info("Async: Attempt 1 (page 2 for total pages) did not yield total pages. Moving to Attempt 2 (page 1).")
    url_page_1 = _prepare_url_for_page(base_review_url, 1, languages="all")
    logger.info(f"Async: Attempt 2 (total pages): Checking on: {url_page_1}")
    try:
        total_pages = await _fetch_and_extract_total_pages_from_next_data_async(url_page_1, client) #, user_agent_string=current_ua_for_total_pages)
        if total_pages is not None:
            logger.info(f"Async: Total pages ({total_pages}) determined from page 1 logic ({url_page_1}).")
            return total_pages
    except Exception as e:
        logger.error(f"Async: Attempt 2 (page 1 for total pages) failed after retries for {url_page_1}: {type(e).__name__} - {str(e)}")

    logger.warning(f"Async: Could not determine total review pages from either page 2 or page 1 logic for {base_review_url}.")
    return None