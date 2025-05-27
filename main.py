from scraper import scrape_trustpilot_reviews

base_url = 'https://www.trustpilot.com/review/www.mexipass.com'

reviews = scrape_trustpilot_reviews(base_url)

for review in reviews:
    print(review)
