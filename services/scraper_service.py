import requests
import json
import os
import logging
from urllib.parse import urlparse
from typing import Optional 

from utils.scraper_utils import get_company_profile_data, get_reviews_from_page, _prepare_url_for_page
from utils.total_review_pages import determine_total_review_pages

logger = logging.getLogger(__name__)

def run_scrape_trustpilot_reviews(base_url: str, num_pages_to_scrape_override: Optional[int] = None):
    logger.info(f"Starting Trustpilot scraping service for base input URL: {base_url}")
    if num_pages_to_scrape_override is not None:
        logger.info(f"User requested to scrape a maximum of {num_pages_to_scrape_override} page(s).")
    else:
        logger.info("No specific page limit requested by user; will attempt to scrape all available pages.")


    parsed_url_for_name = urlparse(base_url)
    path_segments = [segment for segment in parsed_url_for_name.path.split('/') if segment]
    company_name = "unknown_company_trustpilot"
    if len(path_segments) >= 2 and path_segments[0].lower() == 'review':
        company_name = path_segments[1]
        logger.info(f"Extracted company name: {company_name}")
    else:
        logger.warning(
            f"Could not extract company name from {base_url}. Using default: '{company_name}'."
        )

    base_output_dir = "scraped_data"
    output_directory = os.path.join(base_output_dir, company_name)

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: ./{output_directory}")
    except OSError as e:
        logger.error(f"Could not create directory ./{output_directory}: {e}")
        return {
            "status": "error", "message": f"Could not create directory ./{output_directory}",
            "output_directory": None, "error_details": str(e)
        }

    pages_saved_count = 0
    company_data_saved = False

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })

        logger.info(f"Attempting to determine total review pages using utility for: {base_url}")
        total_pages_from_util = determine_total_review_pages(base_url, session)

        effective_max_pages = 20000000
        total_pages_determined_source = f"fallback ({effective_max_pages} pages)"

        if total_pages_from_util is not None:
            effective_max_pages = total_pages_from_util
            total_pages_determined_source = f"utility (site indicates {effective_max_pages} pages)"
            logger.info(f"Site has {effective_max_pages} review pages based on utility.")
        else:
            logger.warning(f"Could not determine total pages from site using utility. Using fallback: {effective_max_pages}.")

        if num_pages_to_scrape_override is not None and num_pages_to_scrape_override > 0:
            logger.info(f"User override: scrape up to {num_pages_to_scrape_override} page(s).")
            if num_pages_to_scrape_override < effective_max_pages:
                effective_max_pages = num_pages_to_scrape_override
                total_pages_determined_source += f" (capped by user to {effective_max_pages})"
                logger.info(f"Adjusting: will scrape a maximum of {effective_max_pages} pages due to user request.")
            else:
                logger.info(f"User request ({num_pages_to_scrape_override}) is >= site/fallback max ({effective_max_pages}). Will scrape up to {effective_max_pages} pages.")

        logger.info(f"Final decision: scraping up to {effective_max_pages} page(s). Source: {total_pages_determined_source}")

        url_for_profile_data = _prepare_url_for_page(base_url, 1, languages="all")
        logger.info(f"Fetching company profile data from: {url_for_profile_data}")
        company_data_to_save, _ = get_company_profile_data(url_for_profile_data, session)
        if company_data_to_save:
            profile_file_path = os.path.join(output_directory, "page0_company_profile.json")
            try:
                with open(profile_file_path, 'w', encoding='utf-8') as f:
                    json.dump(company_data_to_save, f, indent=4, ensure_ascii=False)
                logger.info(f"Successfully saved company profile data to {profile_file_path}")
                company_data_saved = True
            except IOError as e:
                logger.error(f"Could not write company profile data to file {profile_file_path}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error saving company profile data to JSON: {e}", exc_info=True)
        else:
            logger.warning(f"Could not retrieve company profile data from {url_for_profile_data}.")


        page_number = 1
        while page_number <= effective_max_pages:
            url_to_scrape_reviews = _prepare_url_for_page(base_url, page_number, languages="all")

            logger.info(f"Scraping reviews page number: {page_number} (target max: {effective_max_pages}) from URL: {url_to_scrape_reviews}")
            reviews_on_page, total_pages_this_loop = get_reviews_from_page(url_to_scrape_reviews, session)

            if not reviews_on_page:
                if page_number == 1:
                    logger.warning(f"No reviews found on the first review page ({url_to_scrape_reviews}). Company profile saved: {company_data_saved}.")
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
                logger.info(f"Reached the target number of pages ({effective_max_pages}). Ending review scrape.")
                break
            page_number += 1

        if page_number <= effective_max_pages and pages_saved_count < effective_max_pages and not total_pages_determined_source.startswith("fallback"):
             logger.info(f"Loop ended before reaching effective_max_pages. Scraped {pages_saved_count} pages up to page {page_number-1}. Effective max was {effective_max_pages}.")


    total_files_saved = pages_saved_count + (1 if company_data_saved else 0)
    if total_files_saved == 0:
        message = f"Scraping attempted for {base_url}. No data could be saved. Check logs."
        status = "error"
    else:
        message = (f"Scraping finished for {base_url}. {total_files_saved} file(s) saved in ./{output_directory}. "
                   f"Targeted up to {effective_max_pages} page(s). Source of page count: {total_pages_determined_source}.")
        status = "success"

    logger.info(message)
    return {
        "status": status, "message": message, "output_directory": output_directory,
        "total_files_saved": total_files_saved, "company_profile_saved": company_data_saved,
        "review_pages_saved_count": pages_saved_count,
        "effective_max_pages_used": effective_max_pages,
        "total_pages_source_info": total_pages_determined_source
    }