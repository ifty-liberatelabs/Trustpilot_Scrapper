
from pydantic import BaseModel, HttpUrl
from typing import Optional

class ScrapeAcceptedResponse(BaseModel):
    status: str
    message: str
