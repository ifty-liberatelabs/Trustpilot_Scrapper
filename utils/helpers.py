import random
import asyncio
import httpx
import datetime
import pathlib
import aiofiles


from utils.proxy_pool import PROXY_POOL
from utils.scraper_utils import get_reviews_from_page_async


MAX_SPECIAL_RETRIES = 10
BACKOFF_S           = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
LOG_PATH            = pathlib.Path("logs/scraper_retry_log.md")

async def _ensure_log_directory_exists():
    """Ensures the directory for the log file exists."""
    if not LOG_PATH.parent.exists():
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

async def _append_retry_log(page_url: str, proxy: str, ua: str,
                            attempt_no: int, trigger_status_code: int, status_icon: str): # Added trigger_status_code
    """
    Append one markdown row to the special retry log.
    """
    await _ensure_log_directory_exists()
    
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"|{ts}|{page_url}|{proxy}|{ua}|{attempt_no}/{MAX_SPECIAL_RETRIES}|{trigger_status_code}|{status_icon}|\n" # Added trigger status
    hdr  = "|UTC time|URL|Proxy|UA|Attempt|Trigger Status|Result|\n|---|---|---|---|---|---|---|\n"
    
    async with aiofiles.open(LOG_PATH, "a+", encoding='utf-8') as f:
        await f.seek(0, 2)
        if await f.tell() == 0:
            await f.write(hdr)
        await f.write(line)

async def fetch_page_fresh_ip(page_url: str, initial_user_agent: str, user_agents_list: list):
    """
    Fetch a Trustpilot review page using a *fresh* TCP connection.
    Retries specifically for 403 or 502 errors by changing IP and User-Agent.
    Logs these special retry cycles.
    """
    in_special_retry_cycle = False
    triggering_status_for_cycle = None # To store what status (403/502) started this cycle
    proxy_used = None
    ua_used = initial_user_agent

    for attempt in range(MAX_SPECIAL_RETRIES):
        proxy_used = random.choice(PROXY_POOL) if PROXY_POOL else None
        if attempt > 0:
            if user_agents_list:
                ua_used = random.choice(user_agents_list)
            
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
            
            if in_special_retry_cycle: # Log success if it's a recovery from a previous 403/502
                await _append_retry_log(page_url, str(proxy_used), ua_used,
                                        attempt + 1, triggering_status_for_cycle, "✅")
            return reviews, proxy_used

        except httpx.HTTPStatusError as exc:
            # MODIFIED: Check for both 403 and 502 for special retry
            if exc.response.status_code in [403, 502]: 
                if not in_special_retry_cycle: # First time hitting 403/502 in this call for this page
                    in_special_retry_cycle = True
                    triggering_status_for_cycle = exc.response.status_code

                if attempt == MAX_SPECIAL_RETRIES - 1: # Last attempt failed for 403/502
                    await _append_retry_log(page_url, str(proxy_used), ua_used,
                                            attempt + 1, triggering_status_for_cycle, "❌")
                    raise # Give up and re-raise the error
                
                # If not the last attempt, sleep and loop will continue for the next attempt
                await asyncio.sleep(BACKOFF_S[attempt]) 
            else: # Other HTTPStatusErrors (not 403 or 502)
                if in_special_retry_cycle: # If it failed with a different error during a 403/502 retry cycle
                     await _append_retry_log(page_url, str(proxy_used), ua_used,
                                            attempt + 1, triggering_status_for_cycle, "❌ (Other HTTPError)")
                raise
        
        except Exception as e_other:
            if in_special_retry_cycle: 
                await _append_retry_log(page_url, str(proxy_used), ua_used,
                                        attempt + 1, triggering_status_for_cycle, f"❌ ({type(e_other).__name__})")
            raise 
            
    return [], None 
