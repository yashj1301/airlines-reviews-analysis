
from scripts.scraper.scraper import AirlineReviewScraper
from scripts.loader.loader import AirlineReviewLoader
import time
import asyncio

# required parameters
airline_name = "Air India"

#scraping the reviews
airline_scraper = AirlineReviewScraper(airline_name)

start_time = time.time()
print(f"[SCRAPER] Starting the scraping process... {time.strftime('%d-%m-%Y %H:%M:%S UTC%z',time.localtime(start_time))}")

asyncio.run(airline_scraper.extract_all_reviews(review_type="all"))

end_time = time.time()
print(f"[SCRAPER] Scraping process finished. {time.strftime('%d-%m-%Y %H:%M:%S UTC%z',time.localtime(end_time))}")
print(f"[SCRAPER] Time taken to scrape reviews: {end_time - start_time:.2f} seconds.")

#loading the reviews to S3
airline_loader = AirlineReviewLoader(airline_name, data_type="raw")
bucket_name = "airlines-reviews"

start_time = time.time()
print("")
print(f"[LOADER] Starting the upload process... {time.strftime('%d-%m-%Y %H:%M:%S UTC%z',time.localtime(start_time))}")

#casting vals
airline_loader.airline_reviews = airline_scraper.airline_reviews
airline_loader.seat_reviews = airline_scraper.seat_reviews
airline_loader.lounge_reviews = airline_scraper.lounge_reviews

airline_loader.load_data(bucket_name,load_type="upload", review_type="all")

end_time = time.time()
print(f"[LOADER] Upload process finished. {time.strftime('%d-%m-%Y %H:%M:%S UTC%z',time.localtime(end_time))}")
print(f"[LOADER] Time taken to upload reviews: {end_time - start_time:.2f} seconds.")


