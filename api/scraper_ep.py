from fastapi import APIRouter, BackgroundTasks, status as http_status
from schemas.scraper_schema import ScrapeRequest, ScrapeAcceptedResponse
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
        "Accepts a Trustpilot company review URL and initiates the scraping process "
        "in the background. The API returns an immediate acknowledgment. "
        "Scraped data (company profile and review pages) will be saved to the "
        "server's file system in a directory named after the company under 'scraped_data'. "
        "Check server logs for detailed progress and completion status."
    )
)
async def scrape_reviews_in_background(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks
):

    logger.info(f"Received scrape request for URL: {str(request.base_url)}. Will process in background.")

    try:
        background_tasks.add_task(run_scrape_trustpilot_reviews, str(request.base_url))
    except Exception as e:

        logger.error(f"Failed to add scraping task for {str(request.base_url)} to background: {e}", exc_info=True)
        return ScrapeAcceptedResponse(
            status="error_scheduling_task",
            message=f"Failed to schedule scraping task for {str(request.base_url)}. Please check server logs."
        )

    return ScrapeAcceptedResponse(
        status="accepted",
        message=f"Scraping task for {str(request.base_url)} has been accepted and is running in the background. "
                f"Data will be saved to the server. Check server logs for progress/completion."
    )
