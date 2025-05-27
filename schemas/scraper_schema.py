from pydantic import BaseModel, HttpUrl
from typing import Optional

class ScrapeRequest(BaseModel):
    base_url: HttpUrl

class ScrapeAcceptedResponse(BaseModel):
    status: str
    message: str
