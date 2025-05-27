from fastapi import FastAPI
import logging
import uvicorn

from api import scraper_ep

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__) 

app = FastAPI(
    title="Trustpilot Scraper API",
    description="API to scrape reviews from Trustpilot.",
    version="1.0.0"
)


app.include_router(scraper_ep.router, prefix="/api/v1/trustpilot", tags=["Trustpilot Scraper"])

if __name__ == "__main__":
    logger.info("Starting Uvicorn server programmatically for Trustpilot Scraper API.")
    uvicorn.run(app, host="0.0.0.0", port=8000)