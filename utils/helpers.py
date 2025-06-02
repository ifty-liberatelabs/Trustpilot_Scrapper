import random
import asyncio
import httpx
import datetime
import pathlib
import aiofiles


from utils.proxy_pool import PROXY_POOL
from utils.scraper_utils import get_reviews_from_page_async


MAX_RETRIES_403 = 10
BACKOFF_S       = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
LOG_PATH        = pathlib.Path("logs/scraper_retry_log.md")

async def _ensure_log_directory_exists():
    """Ensures the directory for the log file exists."""
    if not LOG_PATH.parent.exists():
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


async def _append_retry_log(page_url: str, proxy: str, ua: str,
                            attempt_no: int, status: str):
    """
    Append one markdown row to the retry log.
    Example: |2025-06-02 12:34:56|https://…page=123|proxy:1.2.3.4:5555|UA-Edge/125|2/3|✅|
    """
    await _ensure_log_directory_exists() # Ensure directory exists before writing
    
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"|{ts}|{page_url}|{proxy}|{ua}|{attempt_no}/{MAX_RETRIES_403}|{status}|\n"
    hdr  = "|UTC time|URL|Proxy|UA|Attempt|Result|\n|---|---|---|---|---|---|\n"
    
    async with aiofiles.open(LOG_PATH, "a+", encoding='utf-8') as f:
        await f.seek(0, 2)  # Seek to the end of the file
        if await f.tell() == 0:  # Check if the file is empty
            await f.write(hdr) # Write header if file is new/empty
        await f.write(line)

async def fetch_page_fresh_ip(page_url: str, initial_user_agent: str, user_agents_list: list):
    """
    Fetch a Trustpilot review page using a *fresh* TCP connection and rotating User-Agent on retries.
    Only pages that needed ≥2 attempts due to 403 errors are logged in scraper_retry_log.md
    with a single row that states whether the retry finally succeeded (✅)
    or gave up (❌). Other exceptions are reraised.
    """
    needs_logging_for_403_cycle = False
    proxy_used = None
    ua_used = initial_user_agent

    for attempt in range(MAX_RETRIES_403):
        proxy_used = random.choice(PROXY_POOL)
        if attempt > 0:  # For retries (i.e., not the first attempt)
            if user_agents_list: # Check if the list is not empty
                ua_used = random.choice(user_agents_list)
            # else, ua_used remains the one from the previous attempt or initial_user_agent
            
        transport = httpx.AsyncHTTPTransport(
            retries=0,
            limits=httpx.Limits(max_keepalive_connections=0, max_connections=1)
        )

        try:
            async with httpx.AsyncClient(
                headers={"User-Agent": ua_used},
                timeout=30.0,
                follow_redirects=True,
                proxy=proxy_used,
                transport=transport,
            ) as client:
                reviews, _ = await get_reviews_from_page_async(
                    page_url, client, user_agent_string=ua_used
                )
            
            if needs_logging_for_403_cycle:
                await _append_retry_log(page_url, proxy_used, ua_used,
                                        attempt + 1, "✅")
            return reviews, proxy_used

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 403:
                needs_logging_for_403_cycle = True
                if attempt == MAX_RETRIES_403 - 1:
                    await _append_retry_log(page_url, proxy_used, ua_used,
                                            attempt + 1, "❌")
                    raise 
                await asyncio.sleep(BACKOFF_S[attempt]) 
            else:
                if needs_logging_for_403_cycle:
                     await _append_retry_log(page_url, proxy_used, ua_used,
                                            attempt + 1, "❌")
                raise 
        except Exception: 
            if needs_logging_for_403_cycle:
                await _append_retry_log(page_url, proxy_used, ua_used,
                                        attempt + 1, "❌")
            raise
    return [], None # Should generally not be reached if MAX_RETRIES_403 >=1