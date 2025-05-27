import requests
import json
import os
import logging
from urllib.parse import urlparse, parse_qs, urlencode

from utils.scraper_utils import get_company_profile_data, get_reviews_from_page

logger = logging.getLogger(__name__)




def run_scrape_trustpilot_reviews(base_url: str):
    logger.info(f"Starting Trustpilot scraping service for base URL: {base_url}")

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


    base_output_dir = "scraped_data" 
    output_directory = os.path.join(base_output_dir, company_name)
    
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: ./{output_directory}")
    except OSError as e:
        logger.error(f"Could not create directory ./{output_directory}: {e}")
        return {
            "status": "error",
            "message": f"Could not create directory ./{output_directory}",
            "output_directory": None,
            "error_details": str(e)
        }

    page_number = 1
    pages_saved_count = 0
    effective_max_pages = 200
    company_data_saved = False

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
                company_data_saved = True
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
                if total_pages_this_loop < effective_max_pages :
                    logger.info(f"Total pages updated from review page {page_number} to: {total_pages_this_loop}. Was: {effective_max_pages}")
                    effective_max_pages = total_pages_this_loop

            if not reviews_on_page:
                if page_number == 1 and not company_data_saved:
                    logger.warning(f"No reviews found on the first review page ({url_to_scrape_reviews}) and no company data acquired.")
                elif page_number > 1 :
                    logger.info(f"No reviews found on page {page_number} ({url_to_scrape_reviews}). Assuming end of reviews.")
                break 

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

    total_files_saved = pages_saved_count + (1 if company_data_saved else 0)
    
    if total_files_saved == 0 and not company_data_saved:
        message = f"Scraping attempted for {base_url}. No data could be saved. Check logs for errors."
        status = "warning"
    else:
        message = f"Scraping finished. {total_files_saved} file(s) saved in directory: ./{output_directory}"
        status = "success"

    logger.info(message)
    return {
        "status": status,
        "message": message,
        "output_directory": output_directory,
        "total_files_saved": total_files_saved,
        "company_profile_saved": company_data_saved,
        "review_pages_saved_count": pages_saved_count
    }