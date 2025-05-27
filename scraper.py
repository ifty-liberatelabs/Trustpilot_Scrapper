import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
from urllib.parse import urlparse, parse_qs, urlencode

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])

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
        if 'script_tag' in locals() and script_tag and script_tag.string: # ensure script_tag is defined
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
        if 'reviews_script_tag' in locals() and reviews_script_tag and reviews_script_tag.string: # ensure reviews_script_tag is defined
            logger.debug(f"Content that failed to parse (reviews): {reviews_script_tag.string[:500]}...")
    except Exception as e:
        logger.error(f"An unexpected error occurred in get_reviews_from_page for {url}: {e}", exc_info=True)
    return [], None

def scrape_trustpilot_reviews(base_url: str):
    logger.info(f"Starting Trustpilot scraping for base URL: {base_url}")

    parsed_url_for_name = urlparse(base_url)
    path_segments = [segment for segment in parsed_url_for_name.path.split('/') if segment]
    company_name = "unknown_company_trustpilot"
    if len(path_segments) >= 2 and path_segments[0].lower() == 'review':
        company_name = path_segments[1]
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
        return None

    page_number = 1
    pages_saved_count = 0
    effective_max_pages = 200 # Fallback

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

        profile_url_parsed = urlparse(base_url)
        profile_query_params = parse_qs(profile_url_parsed.query)
        profile_query_params['page'] = ['1'] 
        profile_new_query_string = urlencode(profile_query_params, doseq=True)
        url_for_profile_data = profile_url_parsed._replace(query=profile_new_query_string).geturl()
        
        logger.info(f"Fetching initial company profile data from: {url_for_profile_data}")
        company_data, total_pages_from_profile = get_company_profile_data(url_for_profile_data, session)
        
        if total_pages_from_profile is not None:
            effective_max_pages = total_pages_from_profile
            logger.info(f"Set effective max pages to scrape to {effective_max_pages} based on profile page data.")
        else:
            logger.warning(f"Could not determine total pages from profile page. Using fallback max_pages_to_scrape: {effective_max_pages}")

        if company_data:
            profile_file_path = os.path.join(output_directory, "page0_company_profile.json")
            try:
                with open(profile_file_path, 'w', encoding='utf-8') as f:
                    json.dump(company_data, f, indent=4, ensure_ascii=False)
                logger.info(f"Successfully saved company profile data to {profile_file_path}")
            except IOError as e:
                logger.error(f"Could not write company profile data to file {profile_file_path}: {e}")
        else:
            logger.warning(f"Could not retrieve company profile data from {url_for_profile_data}. Proceeding with review scraping.")

        parsed_base_for_reviews = urlparse(base_url)

        while page_number <= effective_max_pages:
            current_page_query_params = parse_qs(parsed_base_for_reviews.query)
            current_page_query_params['page'] = [str(page_number)]
            
            new_query_string_for_reviews = urlencode(current_page_query_params, doseq=True)
            url_to_scrape_reviews = parsed_base_for_reviews._replace(query=new_query_string_for_reviews).geturl()

            logger.info(f"Scraping reviews page number: {page_number} from URL: {url_to_scrape_reviews}")
            reviews_on_page, total_pages_this_loop = get_reviews_from_page(url_to_scrape_reviews, session)

            if total_pages_this_loop is not None and total_pages_this_loop != effective_max_pages:
                if total_pages_this_loop < effective_max_pages : # Only update if it's a smaller, more accurate number
                    logger.info(f"Total pages updated from review page {page_number} to: {total_pages_this_loop}. Was: {effective_max_pages}")
                    effective_max_pages = total_pages_this_loop

            if not reviews_on_page:
                if page_number == 1 and not company_data : # Only warn if we also didn't get company data (which implies page 1 also failed)
                    logger.warning(f"No reviews found on the first review page ({url_to_scrape_reviews}).")
                elif page_number > 1 : # If not first page, assume end of reviews
                    logger.info(f"No reviews found on page {page_number} ({url_to_scrape_reviews}). Assuming end of reviews.")
                break # Break in either case of no reviews (unless it's page 1 and we got profile)

            logger.info(f"Retrieved {len(reviews_on_page)} reviews from page {page_number}.")
            file_path = os.path.join(output_directory, f"page{page_number}_reviews.json")
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(reviews_on_page, f, indent=4, ensure_ascii=False)
                logger.info(f"Successfully saved reviews to {file_path}")
                pages_saved_count += 1
            except IOError as e:
                logger.error(f"Could not write reviews to file {file_path}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error saving reviews page {page_number} data to JSON: {e}", exc_info=True)

            if page_number >= effective_max_pages:
                logger.info(f"Reached the determined total number of pages ({effective_max_pages}). Ending review scrape.")
                break
            page_number += 1
        
        if page_number > effective_max_pages and pages_saved_count < effective_max_pages and effective_max_pages != 200:
             logger.info(f"Loop ended. Scraped {pages_saved_count} pages up to page {page_number-1}. Effective max was {effective_max_pages}.")

    total_files_saved = pages_saved_count + (1 if company_data else 0)
    logger.info(f"Scraping finished. {total_files_saved} file(s) saved in directory: ./{output_directory}")
    return output_directory

if __name__ == '__main__':
    
    target_base_url = "https://www.trustpilot.com/review/www.mexipass.com?languages=all"

    logger.info(f"--- Starting script for {target_base_url} ---")

    is_placeholder_url = ("example.com" in target_base_url and
                          not any(q_param in target_base_url for q_param in ['?', '&']))


    if is_placeholder_url:
        logger.warning("Using a placeholder URL (example.com) without additional query parameters. This will likely not find real reviews or company profile data.")
        logger.warning("Please replace 'example.com' with a real domain for actual scraping, or add necessary query parameters.")
    
    output_dir = scrape_trustpilot_reviews(target_base_url)
    
    if output_dir:
        logger.info(f"Data (if any) for {target_base_url} has been attempted to be saved in directory: ./{output_dir}")
    else:
        logger.info(f"Scraping process for {target_base_url} did not complete successfully or output directory could not be determined/created.")

    logger.info(f"--- Script finished for {target_base_url} ---")