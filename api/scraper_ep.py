from fastapi import APIRouter, BackgroundTasks, status as http_status, Form
from pydantic import HttpUrl
from typing import Optional

from schemas.scraper_schema import ScrapeAcceptedResponse
from services.scraper_service import run_scrape_trustpilot_reviews
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post(
    "/scrape",
    response_model=ScrapeAcceptedResponse,
    status_code=http_status.HTTP_202_ACCEPTED,
    summary="Initiate Trustpilot Review Scraping",
    description=(
        "Accepts a Trustpilot company review URL and an optional number of pages "
        "to scrape. Initiates the scraping process in the background. "
        "If 'Number of pages' is blank or 0, all available review pages will be scraped. "
        "Otherwise, it will scrape up to the specified number of pages. "
        "The API returns an immediate acknowledgment. "
        "Scraped data (company profile and review pages) will be saved to the "
        "server's file system in a directory named after the company under 'scraped_data'. "
        "Check server logs for detailed progress and completion status."
    )
)
async def scrape_reviews_in_background(
    background_tasks: BackgroundTasks,
    base_url: HttpUrl = Form(..., description="Trustpilot company review URL (e.g., https://www.trustpilot.com/review/example.com)"),
    num_pages_to_scrape: Optional[int] = Form(None, description="Number of pages to scrape. Leave blank or 0 to scrape all available pages.", ge=0)
):


    logger.info(f"Received scrape request for URL: {str(base_url)}. Number of pages override: {num_pages_to_scrape}. Will process in background.")

    user_page_limit = num_pages_to_scrape
    if num_pages_to_scrape == 0:
        user_page_limit = None
        logger.info("User specified 0 pages, interpreting as scrape all available pages.")


    try:
        background_tasks.add_task(run_scrape_trustpilot_reviews, str(base_url), user_page_limit)
    except Exception as e:
        logger.error(f"Failed to add scraping task for {str(base_url)} to background: {e}", exc_info=True)
        return ScrapeAcceptedResponse(
            status="error_scheduling_task",
            message=f"Failed to schedule scraping task for {str(base_url)}. Please check server logs."
        )

    return ScrapeAcceptedResponse(
        status="accepted",
        message=f"Scraping task for {str(base_url)} (pages: {'all' if user_page_limit is None else user_page_limit}) has been accepted and is running in the background. "
                f"Data will be saved to the server. Check server logs for progress/completion."
    )