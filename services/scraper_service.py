import httpx # For the shared client
import json
import os
import logging
import asyncio
import random
from urllib.parse import urlparse
from typing import Optional, List, Dict, Any

# Import for .env loading
from dotenv import load_dotenv

# Import your newly async utility functions
from utils.scraper_utils import get_company_profile_data_async, get_reviews_from_page_async, _prepare_url_for_page
from utils.total_review_pages import determine_total_review_pages_async
import aiofiles

from tenacity import RetryError 

logger = logging.getLogger(__name__)

# This is the worker that processes individual pages.
async def trustpilot_page_scraping_worker(
    worker_id: int,
    page_queue: asyncio.Queue,
    client: httpx.AsyncClient, 
    base_url_str: str,
    folder_name: str,
    total_pages_overall: int,
    saved_files_list: list,
    failed_pages_list: list,
    global_page_counter: list,
    global_delay_event: asyncio.Event
):
    logger.info(f"Trustpilot Async Worker {worker_id}: Starting...")
    pages_processed_in_this_worker_batch = 0
    WORKER_BATCH_SIZE = 5 

    while True:
        page_num = None
        try:
            if not global_delay_event.is_set(): 
                logger.info(f"Trustpilot Async Worker {worker_id}: Global delay active, waiting...")
                await global_delay_event.wait()
                logger.info(f"Trustpilot Async Worker {worker_id}: Global delay ended, resuming.")

            page_num = await page_queue.get()
            if page_num is None: 
                logger.info(f"Trustpilot Async Worker {worker_id}: Received stop signal. Exiting.")
                break

            url_to_scrape = _prepare_url_for_page(base_url_str, page_num, languages="all")

            try:
                reviews_on_page, _ = await get_reviews_from_page_async(url_to_scrape, client)

                if not reviews_on_page: 
                    logger.info(f"Trustpilot Async Worker {worker_id}: No reviews found on page {page_num} ({url_to_scrape}).")
                    if page_num > 1: 
                         failed_pages_list.append({"page": page_num, "worker_id": worker_id, "error_type": "NoReviewsFound", "error_message": "No reviews on page."})
                else:
                    logger.info(f"Trustpilot Async Worker {worker_id}: Retrieved {len(reviews_on_page)} reviews from page {page_num}.")
                    file_path = os.path.join(folder_name, f"page{page_num}_reviews.json")
                    async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(reviews_on_page, indent=4, ensure_ascii=False))
                    logger.info(f"Trustpilot Async Worker {worker_id}: Successfully saved reviews to {file_path}")
                    saved_files_list.append(file_path)
                
                global_page_counter[0] += 1 

            except RetryError as e: 
                last_exc = e.last_attempt.exception()
                logger.error(f"Trustpilot Async Worker {worker_id}: All retries failed for page {page_num}. Last error: {type(last_exc).__name__} - {str(last_exc)}")
                failed_pages_list.append({"page": page_num, "worker_id": worker_id, "error_type": type(last_exc).__name__, "error_message": str(last_exc)[:200]})
            except Exception as e: 
                logger.error(f"Trustpilot Async Worker {worker_id}: Error processing page {page_num}: {type(e).__name__} - {str(e)}")
                failed_pages_list.append({"page": page_num, "worker_id": worker_id, "error_type": type(e).__name__, "error_message": str(e)[:200]})

            await asyncio.sleep(random.uniform(1.0, 2.0)) 
            pages_processed_in_this_worker_batch += 1

            if pages_processed_in_this_worker_batch >= WORKER_BATCH_SIZE:
                batch_delay = random.uniform(3.0, 5.0) 
                logger.info(f"Trustpilot Async Worker {worker_id}: Completed batch of {pages_processed_in_this_worker_batch}. Sleeping for {batch_delay:.2f}s...")
                await asyncio.sleep(batch_delay)
                pages_processed_in_this_worker_batch = 0
        
        except asyncio.CancelledError:
            logger.info(f"Trustpilot Async Worker {worker_id}: Cancelled.")
            break
        except Exception as e:
            logger.exception(f"Trustpilot Async Worker {worker_id}: Unhandled critical exception in worker loop.")
            break
        finally:
            if page_num is not None:
                page_queue.task_done()
            elif page_num is None and hasattr(page_queue, 'task_done'): 
                page_queue.task_done()


# This function will be called by FastAPI's BackgroundTasks
async def run_scrape_trustpilot_reviews(
    base_url: str,
    num_pages_to_scrape_override: Optional[int] = None,
    num_concurrent_workers: int = 5 
):
    logger.info(f"Async Starting Trustpilot scraping service for: {base_url} with {num_concurrent_workers} workers.")

    # --- Load .env file and get proxy URL ---
    load_dotenv()
    proxy_url_from_env = os.getenv("HTTP_PROXY_URL")
    # This variable will hold the string URL for the 'proxy' argument
    client_proxy_setting = None 

    if proxy_url_from_env:
        client_proxy_setting = proxy_url_from_env # Use the URL string directly
        logger.info(f"Attempting to use proxy (singular argument): {client_proxy_setting}")
    else:
        logger.info("HTTP_PROXY_URL not found in .env. Proceeding without proxy.")
    # --- End of proxy configuration ---

    parsed_url_for_name = urlparse(base_url)
    path_segments = [segment for segment in parsed_url_for_name.path.split('/') if segment]
    company_name = "unknown_company_trustpilot"
    if len(path_segments) >= 2 and path_segments[0].lower() == 'review':
        company_name = path_segments[1]
    base_output_dir = "scraped_data"
    output_directory = os.path.join(base_output_dir, company_name)
    os.makedirs(output_directory, exist_ok=True)
    logger.info(f"Output directory: ./{output_directory}")

    company_data_saved = False
    saved_files_results = [] 
    failed_pages_details = []

    async_client_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Connection": "keep-alive"
    }

    # Pass client_proxy_setting to the AsyncClient's 'proxy' argument
    async with httpx.AsyncClient(
        headers=async_client_headers, 
        timeout=30.0, # Default timeout for operations
        follow_redirects=True, 
        proxy=client_proxy_setting # MODIFIED: Use singular 'proxy' and pass the URL string
    ) as client:
        logger.info(f"httpx.AsyncClient initialized. Proxy (singular argument) {'ACTIVE' if client_proxy_setting else 'INACTIVE'}.")
        
        logger.info(f"Async: Attempting to determine total review pages for: {base_url}")
        try:
            total_pages_from_util = await determine_total_review_pages_async(base_url, client)
        except Exception as e:
            logger.error(f"Async: Error determining total pages: {type(e).__name__} - {str(e)}", exc_info=True)
            total_pages_from_util = None
        
        effective_max_pages = 20000000 # Fallback, can be adjusted
        total_pages_determined_source = f"fallback ({effective_max_pages} pages)"
        if total_pages_from_util is not None:
            effective_max_pages = total_pages_from_util
            total_pages_determined_source = f"utility (site indicates {effective_max_pages} pages)"
        if num_pages_to_scrape_override is not None and num_pages_to_scrape_override > 0:
            if num_pages_to_scrape_override < effective_max_pages:
                effective_max_pages = num_pages_to_scrape_override
                total_pages_determined_source += f" (capped by user to {effective_max_pages})"
        logger.info(f"Async: Final decision: scraping up to {effective_max_pages} page(s). Source: {total_pages_determined_source}")

        url_for_profile_data = _prepare_url_for_page(base_url, 1, languages="all")
        logger.info(f"Async: Fetching company profile data from: {url_for_profile_data}")
        try:
            company_data_to_save, _ = await get_company_profile_data_async(url_for_profile_data, client)
            if company_data_to_save:
                profile_file_path = os.path.join(output_directory, "page0_company_profile.json")
                async with aiofiles.open(profile_file_path, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(company_data_to_save, indent=4, ensure_ascii=False))
                logger.info(f"Async: Successfully saved company profile data to {profile_file_path}")
                company_data_saved = True
            else:
                logger.warning(f"Async: Could not retrieve company profile data from {url_for_profile_data}.")
        except Exception as e:
            logger.error(f"Async: Error fetching/saving company profile: {type(e).__name__} - {str(e)}", exc_info=True)

        if effective_max_pages > 0 and (company_data_saved or total_pages_from_util is not None):
            page_queue = asyncio.Queue(maxsize=num_concurrent_workers * 2) 
            global_page_counter = [0] 
            global_delay_event = asyncio.Event()
            global_delay_event.set() 

            async def queue_filler_and_global_delay_manager_trustpilot():
                logger.info("Trustpilot Async Queue filler: Starting to queue pages.")
                for i in range(1, effective_max_pages + 1):
                    await page_queue.put(i)
                    if global_page_counter[0] > 0 and global_page_counter[0] % 50 == 0: 
                        if global_delay_event.is_set(): 
                            global_delay_event.clear() 
                            delay_50_pages = random.uniform(5.0, 10.0) 
                            logger.info(f"--- TP ASYNC GLOBAL: Processed approx {global_page_counter[0]} pages. Global 50-page delay for {delay_50_pages:.2f}s ---")
                            await asyncio.sleep(delay_50_pages)
                            global_delay_event.set() 
                            logger.info(f"--- TP ASYNC GLOBAL: Global 50-page delay ended. ---")
                for _ in range(num_concurrent_workers):
                    await page_queue.put(None) 
                logger.info("Trustpilot Async Queue filler: All pages and sentinels queued.")

            filler_task = asyncio.create_task(queue_filler_and_global_delay_manager_trustpilot())
            worker_tasks = []
            for i in range(num_concurrent_workers):
                task = asyncio.create_task(
                    trustpilot_page_scraping_worker(
                        worker_id=i + 1, page_queue=page_queue, client=client,
                        base_url_str=base_url,
                        folder_name=output_directory,
                        total_pages_overall=effective_max_pages,
                        saved_files_list=saved_files_results,
                        failed_pages_list=failed_pages_details,
                        global_page_counter=global_page_counter,
                        global_delay_event=global_delay_event,
                    )
                )
                worker_tasks.append(task)

            await filler_task
            logger.info("Trustpilot Async Queue filler task completed.")
            await page_queue.join() 
            logger.info("Trustpilot Async All items from page queue processed.")
            
            worker_gather_results = await asyncio.gather(*worker_tasks, return_exceptions=True)
            for i, res in enumerate(worker_gather_results):
                if isinstance(res, Exception): 
                    logger.error(f"Trustpilot Async Worker {i+1} terminated with unhandled exception: {res}", exc_info=True)
            logger.info("Trustpilot Async All worker tasks completed.")
        else:
            logger.info("Async: Skipping review page scraping due to no pages to scrape or initial profile fetch failure/no pages found.")

    pages_saved_count = len(saved_files_results)
    total_files_saved = pages_saved_count + (1 if company_data_saved else 0)
    final_status = "success" if total_files_saved > 0 else "no_data_saved"
    if failed_pages_details:
        final_status = "partial_success" if total_files_saved > 0 else "error"
    
    summary_message = (f"Async Scraping for {base_url} finished. Status: {final_status}. "
                       f"Targeted up to {effective_max_pages} page(s). "
                       f"Company Profile Saved: {company_data_saved}. "
                       f"Review Pages Saved: {pages_saved_count}. "
                       f"Failed Pages: {len(failed_pages_details)}. "
                       f"Data in ./{output_directory}. "
                       f"Proxy Used: {'Yes, via ' + client_proxy_setting if client_proxy_setting else 'No'}.")
    logger.info(summary_message)

    return {
        "status": final_status, "message": summary_message, "output_directory": output_directory,
        "total_files_saved": total_files_saved, "company_profile_saved": company_data_saved,
        "review_pages_saved_count": pages_saved_count,
        "failed_pages_count": len(failed_pages_details),
        "failed_pages_list_summary": failed_pages_details[:10], 
        "effective_max_pages_used": effective_max_pages,
        "total_pages_source_info": total_pages_determined_source,
        "proxy_enabled": bool(client_proxy_setting) 
    }