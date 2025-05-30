import httpx
import asyncio
import os
from dotenv import load_dotenv

print(f"httpx version being inspected: {httpx.__version__}")

load_dotenv()
proxy_url_from_env = os.getenv("HTTP_PROXY_URL")
single_proxy_url = None

if proxy_url_from_env:
    single_proxy_url = proxy_url_from_env
    print(f"Attempting to use proxy (singular): {single_proxy_url}")
else:
    print("HTTP_PROXY_URL not found in .env. Proceeding without proxy.")

async def main():
    try:
        # Consider adding a default timeout to the client if needed, e.g., timeout=30.0
        async with httpx.AsyncClient(proxy=single_proxy_url) as client:
            print("httpx.AsyncClient initialized successfully with proxy (singular) argument.")
            
            target_url = "https://httpbin.org/ip"
            # target_url = "http://httpbin.org/ip" # You can also try with HTTP to see if it makes a difference
            print(f"Testing request to {target_url}...")
            try:
                # Add a specific timeout for the request, can be generous for testing
                response = await client.get(target_url, timeout=30.0)
                print(f"Test request status code: {response.status_code}")
                response.raise_for_status() # This will raise an HTTPStatusError for 4xx/5xx responses
                print(f"Test request response content: {response.json()}")
            except httpx.HTTPStatusError as exc:
                print(f"HTTPStatusError during test request to {exc.request.url}: {exc.response.status_code}")
                print(f"Response content: {exc.response.text[:500]}...") # Print first 500 chars of response
            except httpx.RequestError as exc:
                print(f"RequestError during test request to {exc.request.url}: {type(exc).__name__} - {str(exc)}")
            except Exception as e:
                # Catch any other exceptions
                print(f"Generic exception during test request: {type(e).__name__} - {str(e)}")
                print(f"repr(e): {repr(e)}") # More detailed representation of the exception

    except TypeError as e:
        print(f"TypeError during AsyncClient initialization: {e}")
    except Exception as e:
        print(f"Other error (e.g., during client init or unhandled): {type(e).__name__} - {str(e)}")
        print(f"repr(e): {repr(e)}")

if __name__ == "__main__":
    asyncio.run(main())