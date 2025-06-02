# Trustpilot Scraper API

## Overview

This project is a Python-based asynchronous web scraping application designed to extract company profiles and customer reviews from Trustpilot. It exposes a FastAPI endpoint to initiate scraping tasks, which are then processed in the background. The scraper employs several techniques to mitigate rate limiting and blocking, including proxy rotation, User-Agent rotation, and configurable delays. Scraped data is saved locally in JSON format.

## Features

* **API-Driven Scraping:** Initiate scraping via a simple POST request.
* **Background Task Processing:** API responds immediately (HTTP 202 Accepted) while scraping occurs in the background, handled by FastAPI's `BackgroundTasks`.
* **Asynchronous Operations:** Built with `asyncio`, `httpx` for efficient, non-blocking I/O.
* **Proxy Rotation:**
    * Utilizes a configurable pool of proxies (`utils/proxy_pool.py`) for review page scraping.
    * Rotates proxies on specific errors (403 Forbidden, 502 Bad Gateway) during page fetching.
    * Option to use a default proxy from environment variables for initial setup tasks.
* **User-Agent Rotation:**
    * Uses a list of diverse User-Agents.
    * Rotates User-Agents for each page request and on retries for 403/502 errors.
* **Fresh TCP Connections:** Creates new TCP connections for each retry attempt within the specialized fetching logic to avoid connection-based fingerprinting.
* **Configurable Delays & Throttling:**
    * Random per-page delays.
    * Random batch delays for workers.
    * Global delay mechanism after a set number of pages are processed.
    * Exponential backoff for retries.
* **Robust Error Handling & Retries:**
    * Specialized retry logic for HTTP 403 and 502 errors (in `utils/helpers.py`).
    * General-purpose retries for other network issues and specific HTTP status codes using the `tenacity` library (in `utils/scraper_utils.py` and `utils/total_review_pages.py`).
* **Data Persistence:** Saves company profiles and review pages as JSON files in a structured directory.
* **Detailed Logging:**
    * Comprehensive application logging for monitoring progress and errors.
    * Dedicated retry log (`logs/scraper_special_retry_log.md`) for 403/502 retry cycles, detailing proxy, UA, and outcome.
* **Dynamic Page Count Discovery:** Attempts to determine the total number of review pages for a company.
* **Selective Page Scraping:** Option to specify the number of review pages to scrape or scrape all available pages.

## Project Structure

```
project_root/
├── .venv/                     # Virtual environment (recommended)
├── scraped_data/              # Default output directory for scraped JSON files
│   └── <company_slug>/
│       ├── page0_company_profile.json
│       └── pageX_reviews.json
├── logs/                      # Directory for log files
│   └── scraper_special_retry_log.md # Log for 403/502 retry attempts
├── api/
│   ├── __init__.py
│   └── scraper_ep.py          # FastAPI endpoint definition
├── schemas/
│   ├── __init__.py
│   └── scraper_schema.py      # Pydantic models for API responses
├── services/
│   ├── __init__.py
│   └── scraper_service.py     # Core scraping orchestration, worker management
├── utils/
│   ├── __init__.py
│   ├── helpers.py             # Specialized fetching logic (fetch_page_fresh_ip)
│   ├── proxy_pool.py          # List of proxies for rotation
│   ├── scraper_utils.py       # Data extraction, URL preparation, general tenacity retries
│   └── total_review_pages.py  # Logic for determining total review pages
├── main.py                    # FastAPI application entry point
├── requirements.txt           # Python dependencies
└── .env                       # Environment variables (e.g., HTTP_PROXY_URL)
```

## Technology Stack

* **Python:** 3.12.3
* **Web Framework:** FastAPI
* **ASGI Server:** Uvicorn
* **HTTP Client:** `httpx` (for asynchronous requests)
* **HTML Parsing:** `BeautifulSoup4` (with `lxml` parser)
* **Data Validation:** `Pydantic`
* **Retry Logic:** `tenacity`
* **Asynchronous File I/O:** `aiofiles`
* **Environment Variables:** `python-dotenv`

## Setup and Installation

### 1. Prerequisites

* Python 3.13.3
* `pip` (Python package installer).

### 2. Create a Virtual Environment (Recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies

Install all required packages from `requirements.txt`:

```bash
pip install -r requirements.txt
```
A typical `requirements.txt` might include:
```
fastapi
uvicorn[standard]
httpx
beautifulsoup4
lxml
tenacity
aiofiles
python-dotenv
# Add any other specific versions if necessary
```

### 4. Environment Variables

Create a `.env` file in the project root directory. This file is used to store sensitive information or configurations like a default proxy URL.

Example `.env` file:
```dotenv
HTTP_PROXY_URL=http://username:password@your_default_proxy_host:port
```
* `HTTP_PROXY_URL`: (Optional) A default proxy to be used by the main `httpx.AsyncClient` in `scraper_service.py` for initial tasks (like total page discovery) and the IP echo check. If not set, these initial requests will be made directly. The scraping workers primarily use the `PROXY_POOL`.

## Configuration

Several aspects of the scraper can be configured:

### 1. Proxy Pool (`utils/proxy_pool.py`)

This file defines the `PROXY_POOL` list, which is used by the scraping workers for rotating IP addresses. Each proxy should be a string in the format `http://username:password@host:port` or `http://host:port`.

Example:
```python
# utils/proxy_pool.py
PROXY_POOL = [
    "[http://user1:pass1@proxy1.example.com:8000](http://user1:pass1@proxy1.example.com:8000)",
    "[http://user2:pass2@proxy2.example.com:8001](http://user2:pass2@proxy2.example.com:8001)",
    # Add more proxies
]
# Set to PROXY_POOL = [] to disable using the pool (workers will then make direct requests or use HTTP_PROXY_URL if configured in httpx client).
```

### 2. User-Agents (`services/scraper_service.py`)

A list named `USER_AGENTS` in `services/scraper_service.py` contains various User-Agent strings. These are rotated for requests. You can add or modify this list.

### 3. Retry Parameters for 403/502 Errors (`utils/helpers.py`)

* `MAX_SPECIAL_RETRIES`: Number of times to retry a request if a 403 or 502 error is encountered by `fetch_page_fresh_ip`. Default is 10.
* `BACKOFF_S`: A list of sleep durations (in seconds) between these special retries. The length of this list should ideally match `MAX_SPECIAL_RETRIES - 1`. Default is `[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`.

### 4. Tenacity Retry Parameters (`utils/scraper_utils.py`, `utils/total_review_pages.py`)

These files use `@retry` decorators from the `tenacity` library for general network errors and specific HTTP status codes (excluding 403s and potentially 502s, depending on configuration, as these are primarily handled by `helpers.py`'s propagation). Parameters like `stop_after_attempt` and `wait_exponential` can be adjusted within these decorators.

### 5. Concurrent Workers

* The default number of concurrent workers for scraping review pages is set in `services/scraper_service.py` in the `run_scrape_trustpilot_reviews` function signature (e.g., `num_concurrent_workers: int = 3`).
* This default is also reflected in `api/scraper_ep.py` when the background task is added. This can be adjusted based on your system resources and proxy capacity.

## Running the Application

1.  **Activate Virtual Environment:**
    ```bash
    source .venv/bin/activate # Or .venv\Scripts\activate on Windows
    ```
2.  **Start Uvicorn Server:**
    From the project root directory:
    ```bash
    uvicorn main:app --reload --host 0.0.0.0 --port 8000
    ```
    * `--reload`: Enables auto-reloading when code changes (useful for development).
    * `--host 0.0.0.0`: Makes the server accessible on your network.
    * `--port 8000`: Specifies the port.

3.  **Access API Documentation:**
    Once the server is running, open your browser and navigate to `http://127.0.0.1:8000/docs`. You will see the Swagger UI, which provides interactive API documentation.

## API Usage

### Endpoint: `POST /api/v1/trustpilot/scrape`

* **Summary:** Initiate Trustpilot Review Scraping.
* **Description:** Accepts a Trustpilot company review URL and an optional number of pages to scrape. Initiates the scraping process in the background.
* **Status Code:** `202 ACCEPTED` on successful task scheduling.

#### Request Parameters (Form Data):

* `base_url` (string, required, HttpUrl format): The base Trustpilot company review URL.
    * Example: `https://www.trustpilot.com/review/examplecompany.com`
* `num_pages_to_scrape` (integer, optional, `ge=0`): The number of review pages to scrape.
    * If omitted, `None`, or `0`, the scraper will attempt to find and scrape all available review pages.
    * If a positive integer is provided, it will scrape up to that many pages (or fewer if fewer exist).

#### Example Request (using `curl`):

```bash
curl -X POST "[http://127.0.0.1:8000/api/v1/trustpilot/scrape](http://127.0.0.1:8000/api/v1/trustpilot/scrape)" \
     -H "accept: application/json" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "base_url=https%3A%2F%2Fwww.trustpilot.com%2Freview%2Fexample.com&num_pages_to_scrape=5"
```

#### Expected Response (202 Accepted):

```json
{
  "status": "accepted",
  "message": "Scraping task for [https://www.trustpilot.com/review/example.com](https://www.trustpilot.com/review/example.com) (pages: 5) has been accepted with 3 workers and is running in the background. Data will be saved to the server. Check server logs for progress/completion."
}
```
The number of workers in the message will reflect the configured default.

## Output

Scraped data is saved to the local file system in the `scraped_data` directory (created in the project root if it doesn't exist).

* **Directory Structure:** `scraped_data/<company_slug>/`
    * `<company_slug>` is derived from the Trustpilot URL (e.g., `example.com`).
* **Company Profile:** Saved as `page0_company_profile.json`. Contains information like display name, number of reviews, trust score, etc.
* **Review Pages:** Each page of reviews is saved as `pageX_reviews.json` (e.g., `page1_reviews.json`, `page2_reviews.json`, ...). Each file contains a list of review objects extracted from that page.

## Logging

* **Application Logs:** General progress, informational messages, warnings, and errors are logged to the console (stdout). The logging level and format are configured in `main.py`.
* **Special Retry Log (`logs/scraper_special_retry_log.md`):**
    * This Markdown file logs attempts made by the `fetch_page_fresh_ip` function when it encounters a 403 or 502 error.
    * It records the timestamp, URL, proxy used, User-Agent, attempt number, the triggering status code (403 or 502), and the outcome (✅ for success after retry, ❌ for failure after all retries).
    * The `logs` directory is created automatically if it doesn't exist.

## Core Scraping Logic & Anti-Blocking Measures

The scraper employs a layered approach to fetch data while trying to avoid detection and rate limits.

### 1. `fetch_page_fresh_ip` (`utils/helpers.py`)

This is the core function responsible for fetching individual review pages with enhanced anti-blocking measures.
* **Input:** Target URL, initial User-Agent, and a list of all available User-Agents.
* **Special Retries for 403/502:**
    * If an HTTP 403 (Forbidden) or 502 (Bad Gateway) error is received, this function initiates a special retry cycle.
    * It retries up to `MAX_SPECIAL_RETRIES` times (default: 10).
    * **Proxy Rotation:** For each retry attempt, a new proxy is randomly selected from `PROXY_POOL`.
    * **User-Agent Rotation:** For each retry attempt (after the first), a new User-Agent is randomly selected from the `user_agents_list`.
    * **Fresh TCP Connection:** A new `httpx.AsyncHTTPTransport` with `max_keepalive_connections=0` is used for each attempt, ensuring a fresh connection and avoiding reuse of potentially flagged connections.
    * **Backoff Delays:** Waits for an increasing duration (`BACKOFF_S`) between these special retries.
* **Logging:** Retry attempts and their outcomes for 403/502 errors are logged to `logs/scraper_special_retry_log.md`.
* **Delegation:** It calls `get_reviews_from_page_async` (from `utils/scraper_utils.py`) to perform the actual HTTP GET request and parse the content.

### 2. `get_reviews_from_page_async` & `get_company_profile_data_async` (`utils/scraper_utils.py`)

These functions are responsible for:
* Making the HTTP GET request using the provided `httpx.AsyncClient` and User-Agent.
* Calling `response.raise_for_status()` to check for HTTP errors.
* Parsing the HTML response (using BeautifulSoup) to find the `__NEXT_DATA__` script tag.
* Extracting and parsing the JSON data containing reviews or profile information.
* **Tenacity Retries:** Decorated with `@retry` from the `tenacity` library.
    * This provides general retries for network errors (timeouts, connection errors) and specific HTTP status codes like 429 (Too Many Requests) and 5xx server errors. The predicate in `scraper_utils.py` is configured to *not* have `tenacity` retry 403s (and potentially 502s if you modified it), allowing these to propagate to `fetch_page_fresh_ip`.
    * Uses exponential backoff.
    * If `tenacity` exhausts its retries, it re-raises the last exception. This allows `fetch_page_fresh_ip` to catch it if it's a 403/502, or the worker to log it as a failure.

### 3. Delays and Throttling (`services/scraper_service.py`)

Managed within the `trustpilot_page_scraping_worker` and the `queue_filler` in `run_scrape_trustpilot_reviews`:
* **Per-Page Delay:** A short, random delay after each page is processed by a worker.
* **Batch Delay:** Workers pause for a longer random duration after processing a small batch of pages.
* **Global Delay:** All workers are paused periodically (e.g., every 50 pages globally) for a significant duration to reduce overall request velocity.

### 4. Main Client vs. Worker Clients

* **Main Client (`scraper_service.py`):** A single `httpx.AsyncClient` instance is created in `run_scrape_trustpilot_reviews`.
    * It uses the proxy from `HTTP_PROXY_URL` (if set in `.env`).
    * It's used for initial tasks: `determine_total_review_pages_async` and fetching the company profile via `get_company_profile_data_async`.
    * The `log_exit_ip` event hook is attached to this client to monitor its exit IP (if the proxy is set).
* **Worker Clients (`utils/helpers.py`):** The `fetch_page_fresh_ip` function creates a *new, short-lived* `httpx.AsyncClient` for *each attempt* it makes to fetch a page.
    * These clients use proxies from `PROXY_POOL`.
    * This ensures fresh connections and allows different proxies/UAs per attempt.

## Error Handling Summary

* **403 Forbidden / 502 Bad Gateway (Special Handling):**
    1.  `scraper_utils.py` functions encounter these. `tenacity` (if configured as in the last version of `scraper_utils.py`) will not retry these specific codes itself, allowing the `HTTPStatusError` to propagate.
    2.  The `HTTPStatusError` is caught by `helpers.py/fetch_page_fresh_ip`.
    3.  `fetch_page_fresh_ip` initiates its special retry loop (new IP/UA, `BACKOFF_S` delays).
    4.  If all special retries fail, the error is re-raised.
    5.  The worker in `scraper_service.py` catches this final error and logs it as a failed page.
* **Other Retryable HTTP Errors (e.g., 429, other 5xx):**
    1.  `scraper_utils.py` functions encounter these.
    2.  `tenacity` retries them up to its configured limit using the same IP/UA.
    3.  If all `tenacity` retries fail, the error is re-raised.
    4.  This re-raised error is caught by the `except Exception:` in `helpers.py/fetch_page_fresh_ip`. If `fetch_page_fresh_ip` was already in a special retry cycle (e.g., for a prior 403/502 that then led to a different error on retry), it logs it as a failure for that cycle; otherwise, it just re-raises.
    5.  The worker in `scraper_service.py` catches this final error.
* **Network Errors (Timeouts, Connection Errors):**
    1.  Typically handled by `tenacity` in `scraper_utils.py`.
    2.  If `tenacity` retries fail, the error propagates and is caught by `except Exception:` in `helpers.py/fetch_page_fresh_ip`.
    3.  Then caught by the worker.
* **Parsing Errors (JSON, HTML structure):**
    * Caught within `scraper_utils.py` functions, which then return `None` or empty lists. The worker logs these as "no reviews found."
* **Proxy Payment Errors (e.g., `httpx.ProxyError: 402 Payment Required`):**
    * These are caught by the `except Exception:` in `helpers.py/fetch_page_fresh_ip` and then by the worker. The primary solution is to fix the proxy account.

## Potential Issues & Limitations

* **Proxy Quality and Availability:** The effectiveness of the scraper heavily depends on the quality, quantity, and freshness of the proxies in `PROXY_POOL`. Free or low-quality proxies are often quickly detected and blocked.
* **Target Website Changes:** Trustpilot (or any target website) can change its HTML structure, `__NEXT_DATA__` format, or anti-bot mechanisms at any time, which could break the scraper. Regular maintenance and updates would be required.
* **Advanced Anti-Bot Systems:** While this scraper uses several common techniques, highly sophisticated anti-bot systems might still detect and block it (e.g., through TLS fingerprinting, JavaScript challenges, behavioral analysis).
* **Scalability:** For very large-scale scraping, saving to individual JSON files might become inefficient. A database solution would be more appropriate.
* **Ethical Considerations:** Always ensure your scraping activities are compliant with Trustpilot's Terms of Service and relevant legal regulations. Scrape responsibly and avoid overloading the target servers.

## Future Enhancements (TODOs)

* **More Sophisticated Proxy Management:**
    * Implement logic to test proxies and temporarily remove failing ones from the active pool.
    * Support for different proxy protocols (SOCKS5).
    * Integration with proxy rotation services/APIs.
* **Database Integration:** Store scraped data in a database (e.g., PostgreSQL, MongoDB) for better querying and management.
* **User Interface:** A simple web UI (perhaps using FastAPI's HTML templating or a separate frontend framework) to trigger scrapes, view progress, and browse results.
* **Improved Error Reporting/Alerting:** More structured error summaries or even alerts for high failure rates.
* **JavaScript Rendering:** For sites heavily reliant on JavaScript that don't expose data in `__NEXT_DATA__`, integrate a headless browser solution (e.g., Playwright with `httpx-playwright`).
* **Configuration Management:** Move more settings (like `MAX_SPECIAL_RETRIES`, `BACKOFF_S`, worker counts) to the `.env` file or a dedicated configuration file.
* **Health Check for Proxies:** Periodically check the health of proxies in the pool.
