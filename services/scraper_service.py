import asyncio
import json
import logging
import os
import random
from contextlib import suppress
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from utils.proxy_pool import PROXY_POOL

from utils.helpers import fetch_page_fresh_ip 

import aiofiles
import httpx
from dotenv import load_dotenv
from tenacity import RetryError

from utils.scraper_utils import (
    _prepare_url_for_page,
    get_company_profile_data_async,

)
from utils.total_review_pages import (
    determine_total_review_pages_async,
)

logger = logging.getLogger(__name__)

if not logger.handlers: 
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 "
    "Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 "
    "Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Edge/125.0.2535.51",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) "
    "Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
]

IP_ECHO_URL = "https://api.ipify.org?format=text"


async def trustpilot_page_scraping_worker(
    worker_id: int,
    page_queue: asyncio.Queue,
    base_url_str: str,
    folder_name: str,
    total_pages_overall: int,
    saved_files_list: list,
    failed_pages_list: list,
    global_page_counter: list,
    global_delay_event: asyncio.Event,
):
    logger.info("TP Worker %s: started", worker_id)
    WORKER_BATCH_SIZE = 5 
    pages_in_batch = 0

    while True:
        page_num = await page_queue.get()
        try:
            if page_num is None:
                logger.info("TP Worker %s: stop signal received", worker_id)
                return
            if not global_delay_event.is_set():
                logger.debug("TP Worker %s: waiting for global delay", worker_id)
                await global_delay_event.wait()
                logger.debug("TP Worker %s: global delay ended", worker_id)


            url_to_scrape = _prepare_url_for_page(
                base_url_str, page_num, languages="all"
            )
            
            # Select an initial User-Agent for the first attempt by fetch_page_fresh_ip
            initial_ua = random.choice(USER_AGENTS)
            logger.info("TP Worker %s: page %s (Initial UA=%s)", worker_id, page_num, initial_ua)

            reviews_on_page, proxy_used = await fetch_page_fresh_ip(
                url_to_scrape,
                initial_ua,
                USER_AGENTS # Pass the list for retries
            )
            logger.info(
                "TP Worker %s: page %s processing finished (via %s)",
                worker_id,
                page_num,
                proxy_used if proxy_used else "default connection",
            )

            if not reviews_on_page:
                logger.warning(
                    "TP Worker %s: no reviews on page %s after attempts", worker_id, page_num
                )
                if page_num > 1: # Avoid flagging page 1 if it genuinely has no reviews but profile exists
                    failed_pages_list.append(
                        {
                            "page": page_num,
                            "worker_id": worker_id,
                            "error_type": "NoReviewsFoundAfterAttempts",
                            "error_message": "No reviews on page after processing with fetch_page_fresh_ip.",
                        }
                    )
            else:
                out_file = os.path.join(folder_name, f"page{page_num}_reviews.json")
                async with aiofiles.open(out_file, "w", encoding="utf-8") as f:
                    await f.write(
                        json.dumps(reviews_on_page, indent=4, ensure_ascii=False)
                    )
                saved_files_list.append(out_file)
                logger.info(
                    "TP Worker %s: saved %s reviews → %s",
                    worker_id,
                    len(reviews_on_page),
                    out_file,
                )

            global_page_counter[0] += 1
            pages_in_batch += 1

            if pages_in_batch >= WORKER_BATCH_SIZE:
                pages_in_batch = 0
                delay = random.uniform(3, 5)
                logger.debug("TP Worker %s: batch sleep %.2fs", worker_id, delay)
                await asyncio.sleep(delay)

            await asyncio.sleep(random.uniform(1.0, 2.0))

        except RetryError as e: # This catches RetryError if reraised by fetch_page_fresh_ip or tenacity in utils
            last = e.last_attempt.exception()
            failed_pages_list.append(
                {
                    "page": page_num,
                    "worker_id": worker_id,
                    "error_type": type(last).__name__,
                    "error_message": str(last)[:200],
                }
            )
            logger.error("TP Worker %s: retries exhausted on page %s. Last error: %s", worker_id, page_num, str(last))
        except Exception as e: # Catch-all for other unexpected errors in the worker
            failed_pages_list.append(
                {
                    "page": page_num,
                    "worker_id": worker_id,
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:200],
                }
            )
            logger.exception("TP Worker %s: unexpected error on page %s", worker_id, page_num)
        finally:
            page_queue.task_done()

async def run_scrape_trustpilot_reviews(
    base_url: str,
    num_pages_to_scrape_override: Optional[int] = None,
    num_concurrent_workers: int = 3,
):
    logger.info("=== Trustpilot scrape: %s ===", base_url)

    load_dotenv()
    proxy_url_from_env = os.getenv("HTTP_PROXY_URL")
    proxy_for_main_client = None
    if proxy_url_from_env:
        proxy_for_main_client = proxy_url_from_env # Use singular proxy for this client
        logger.info("Main client proxy active: %s", proxy_for_main_client)
    else:
        logger.info("No proxy configured for main client (HTTP_PROXY_URL empty or not set)")

    parsed = urlparse(base_url)
    segs = [s for s in parsed.path.split("/") if s]
    company_slug = segs[1] if len(segs) >= 2 and segs[0].lower() == "review" else "unknown_company"
    output_dir = os.path.join("scraped_data", company_slug)
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Output dir: ./%s", output_dir)

    async def log_exit_ip(response: httpx.Response):
        if IP_ECHO_URL in str(response.request.url): return
        try:
            # This client will use the proxy_for_main_client if set
            async with httpx.AsyncClient(proxy=proxy_for_main_client, timeout=10.0, follow_redirects=True) as ip_client:
                ip_resp = await ip_client.get(IP_ECHO_URL)
                exit_ip = (ip_resp.text or "").strip()
                logger.info(
                    "IP-CHECK (MainClient) | %s %s → %s | exit_ip=%s",
                    response.request.method, response.request.url, response.status_code, exit_ip,
                )
        except Exception as exc:
            logger.debug("IP-CHECK (MainClient) failed: %s: %s", type(exc).__name__, exc)

    default_ua = random.choice(USER_AGENTS) # Pick one UA for the main client
    client_headers = {
        "User-Agent": default_ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    async with httpx.AsyncClient(
        headers=client_headers,
        timeout=30.0,
        follow_redirects=True,
        proxy=proxy_for_main_client, # Singular proxy for this client
        event_hooks={"response": [log_exit_ip] if proxy_for_main_client else None}, # Only hook if proxy is used
    ) as client: # This client is used for initial page count and profile
        logger.info("Main httpx client ready (UA=%s, Proxy: %s)", default_ua, proxy_for_main_client or "None")

        logger.info("Discovering total review pages …")
        try:
            total_pages_found = await determine_total_review_pages_async(base_url, client)
        except Exception:
            logger.exception("Failed to read total pages, fall back to very high value")
            total_pages_found = None

        max_pages_to_scrape = total_pages_found or 20_000_000 # Default high if not found
        source_of_page_count = f"utility ({total_pages_found} pages)" if total_pages_found else "fallback (20,000,000 pages)"
        
        if num_pages_to_scrape_override is not None and num_pages_to_scrape_override > 0:
            if num_pages_to_scrape_override < max_pages_to_scrape :
                 max_pages_to_scrape = num_pages_to_scrape_override
                 source_of_page_count += f" (capped by user to {max_pages_to_scrape})"
            else: # User override is >= total_pages_found (or fallback if total_pages_found is None)
                 source_of_page_count += f" (user override {num_pages_to_scrape_override} >= determined/fallback, using determined/fallback)"

        logger.info("Will scrape up to %s page(s). Source: %s", max_pages_to_scrape, source_of_page_count)


        company_profile_saved = False
        first_page_url = _prepare_url_for_page(base_url, 1, languages="all")
        try:
            profile_ua = random.choice(USER_AGENTS) # Use a random UA for profile fetching too
            # Pass the specific UA to the function
            profile_data, _ = await get_company_profile_data_async(
                first_page_url, client, user_agent_string=profile_ua
            )
            if profile_data:
                profile_path = os.path.join(output_dir, "page0_company_profile.json")
                async with aiofiles.open(profile_path, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(profile_data, indent=4, ensure_ascii=False))
                logger.info("Saved company profile → %s", profile_path)
                company_profile_saved = True
            else:
                logger.warning("Could not retrieve company profile data (returned None).")
        except Exception:
            logger.exception("Failed to fetch/save company profile")

        if max_pages_to_scrape == 0 or (not company_profile_saved and total_pages_found is None):
            logger.warning("Skipping review scrape: max_pages is 0, or no profile and unknown page count.")
            # Construct a minimal summary for this case
            summary_status = "skipped_no_pages_or_profile"
            logger.info(
                "FINISHED | status=%s | files_saved=0 | failed_pages=0 | dir=./%s",
                summary_status, output_dir,
            )
            return {
                "status": summary_status, "output_directory": output_dir,
                "files_saved": 0, "failed_pages": 0,
                "proxy_for_main_client": proxy_for_main_client or "none",
                "worker_proxies_used_from_pool": True # Since workers would use them
            }

        page_q = asyncio.Queue(maxsize=num_concurrent_workers * 2)
        global_counter = [0]
        global_delay_event = asyncio.Event()
        global_delay_event.set()

        async def queue_filler():
            logger.info("Queue filler: Starting to queue pages (1 to %s).", max_pages_to_scrape)
            for p in range(1, max_pages_to_scrape + 1):
                await page_q.put(p)
                # Global delay logic needs to be robust against global_counter not being updated if workers fail early
                if global_counter[0] and global_counter[0] % 50 == 0: 
                    # Check if not already delaying to avoid multiple logs/sleeps if counter increments slowly
                    if global_delay_event.is_set(): 
                        global_delay_event.clear()
                        delay = random.uniform(5, 10)
                        logger.info("--- GLOBAL 50-page pause %.2fs (pages processed: %s) ---", delay, global_counter[0])
                        await asyncio.sleep(delay)
                        global_delay_event.set()
                        logger.info("--- GLOBAL 50-page pause ended. ---")
            for _ in range(num_concurrent_workers):
                await page_q.put(None)
            logger.info("Queue filler: All pages and sentinels queued.")

        saved_files, failed_pages = [], []
        filler_task = asyncio.create_task(queue_filler())
        worker_tasks = [
            asyncio.create_task(
                trustpilot_page_scraping_worker(
                    i + 1,
                    page_q,
                    # client, # Client is NOT passed to worker anymore
                    base_url,
                    output_dir,
                    max_pages_to_scrape,
                    saved_files,
                    failed_pages,
                    global_counter,
                    global_delay_event,
                )
            )
            for i in range(num_concurrent_workers)
        ]

        await filler_task
        logger.info("Queue filler task completed.")
        try:
            await page_q.join() # Wait for queue to be fully processed
            logger.info("All items from page queue processed by workers.")
        except Exception as e:
            logger.error(f"Error during page_q.join(): {e}", exc_info=True)
        
        # Wait for all worker tasks to complete and gather results/exceptions
        worker_results = await asyncio.gather(*worker_tasks, return_exceptions=True)
        for i, result in enumerate(worker_results):
            if isinstance(result, Exception):
                logger.error(f"Worker {i+1} ended with an unhandled exception: {result}", exc_info=result)
        logger.info("All worker tasks have finished or been cancelled.")


    total_review_pages_saved = len(saved_files)
    total_files_overall = total_review_pages_saved + (1 if company_profile_saved else 0)
    
    current_status = "error" # Default status
    if total_files_overall > 0:
        current_status = "partial_success" if failed_pages else "success"
    elif failed_pages: # No files saved, but there were failures
        current_status = "error"
    else: # No files saved, no failures (e.g. 0 pages to scrape)
        current_status = "no_data_saved_or_needed"


    logger.info(
        "FINISHED | status=%s | files_saved=%s (profile: %s, reviews: %s) | failed_pages=%s | dir=./%s",
        current_status,
        total_files_overall,
        company_profile_saved,
        total_review_pages_saved,
        len(failed_pages),
        output_dir,
    )
    return {
        "status": current_status,
        "output_directory": output_dir,
        "files_saved": total_files_overall,
        "company_profile_saved": company_profile_saved,
        "review_pages_saved_count": total_review_pages_saved,
        "failed_pages_count": len(failed_pages),
        "proxy_for_main_client": proxy_for_main_client or "none",
        "worker_proxies_used_from_pool": True if PROXY_POOL else False, # Assuming PROXY_POOL implies usage
        "failed_pages_summary_sample": failed_pages[:5] # Sample of first 5 failures
    }