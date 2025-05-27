import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from urllib.parse import urlparse, parse_qs, urlencode

logger = logging.getLogger(__name__)


def get_company_profile_data(url: str, session: requests.Session):
    logger.info(f"Attempting to fetch company profile data from: {url}")
    try:
        req = session.get(url)
        req.raise_for_status()
        logger.debug(f"Successfully fetched page content for profile data from {url}. Status code: {req.status_code}")
        time.sleep(1)

        soup = BeautifulSoup(req.text, 'html.parser')
        script_tag = soup.find("script", id="__NEXT_DATA__")

        if not script_tag or not script_tag.string:
            logger.warning(f"Could not find __NEXT_DATA__ script tag or it's empty on {url} for profile data.")
            return None, None

        raw_json = json.loads(script_tag.string)
        page_props = raw_json.get("props", {}).get("pageProps", {})
        business_unit_data = page_props.get("businessUnit")
        
        total_review_pages = None
        filters_data = page_props.get("filters", {}).get("pagination", {})
        if filters_data and "totalPages" in filters_data:
            total_review_pages = filters_data["totalPages"]
            logger.info(f"Found total review pages from initial load: {total_review_pages}")

        if not business_unit_data:
            logger.warning(f"No 'businessUnit' key found in pageProps for profile data from {url}. Page props (first 500 chars): {str(page_props)[:500]}")
            return None, total_review_pages

        profile_info = {
            "id": business_unit_data.get("id"),
            "displayName": business_unit_data.get("displayName"),
            "identifyingName": business_unit_data.get("identifyingName"),
            "numberOfReviews": business_unit_data.get("numberOfReviews"),
            "trustScore": business_unit_data.get("trustScore"),
            "websiteUrl": business_unit_data.get("websiteUrl"),
            "websiteTitle": business_unit_data.get("websiteTitle"),
            "profileImageUrl": business_unit_data.get("profileImageUrl"),
            "customHeaderUrl": business_unit_data.get("customHeaderUrl"),
            "promotion": business_unit_data.get("promotion"),
            "hideCompetitorModule": business_unit_data.get("hideCompetitorModule"),
            "stars": business_unit_data.get("stars")
        }
        
        logger.info(f"Successfully parsed company profile data from {url}")
        return profile_info, total_review_pages

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching profile data from {url}: {http_err} - Status Code: {http_err.response.status_code}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected error occurred during requests for profile data from {url}: {req_err}")
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON for profile data from {url}: {json_err}")
        if 'script_tag' in locals() and script_tag and script_tag.string:
            logger.debug(f"Content that failed to parse (profile data): {script_tag.string[:500]}...")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_company_profile_data for {url}: {e}", exc_info=True)
    return None, None

def get_reviews_from_page(url: str, session: requests.Session):
    logger.info(f"Attempting to fetch reviews from: {url}")
    try:
        req = session.get(url)
        req.raise_for_status()
        logger.debug(f"Successfully fetched page content for reviews from {url}. Status code: {req.status_code}")
        time.sleep(2)

        soup = BeautifulSoup(req.text, 'html.parser')
        reviews_script_tag = soup.find("script", id="__NEXT_DATA__")

        if not reviews_script_tag or not reviews_script_tag.string:
            logger.warning(f"Could not find __NEXT_DATA__ script tag or it's empty on {url} for reviews.")
            return [], None

        reviews_raw_json = json.loads(reviews_script_tag.string)
        page_props = reviews_raw_json.get("props", {}).get("pageProps", {})
        reviews = page_props.get("reviews")
        
        total_pages_from_this_page = None
        filters_data = page_props.get("filters", {}).get("pagination", {})
        if filters_data and "totalPages" in filters_data:
            total_pages_from_this_page = filters_data["totalPages"]

        if reviews is None:
            logger.warning(f"No 'reviews' key found or it is None in pageProps for {url}. Page props (reviews): {str(page_props)[:500]}")
            return [], total_pages_from_this_page

        logger.info(f"Successfully parsed {len(reviews)} reviews from __NEXT_DATA__ on {url}")
        return reviews, total_pages_from_this_page

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while fetching reviews from {url}: {http_err} - Status Code: {http_err.response.status_code}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An unexpected error occurred during requests for reviews from {url}: {req_err}")
    except json.JSONDecodeError as json_err:
        logger.error(f"Failed to decode JSON for reviews from {url}: {json_err}")
        if 'reviews_script_tag' in locals() and reviews_script_tag and reviews_script_tag.string:
            logger.debug(f"Content that failed to parse (reviews): {reviews_script_tag.string[:500]}...")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_reviews_from_page for {url}: {e}", exc_info=True)
    return [], None