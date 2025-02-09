from scripts.scraper.scraper import ReviewScraper  # Import the ReviewScraper class
import os
from scripts.loader.loader import DataLoader  # Import the DataLoader class
import pandas as pd

# Define the base URL of the website to scrape
base_url = "https://www.airlinequality.com/airline-reviews/british-airways"

# Initialize the scraper object
scraper = ReviewScraper(base_url)

# Scrape the reviews
print("Starting the scraping process...")
review_data = scraper.scrape_all_reviews(start_page=5,end_page=10)

# data scraping checks
if not review_data.empty:
    
    os.system('cls')
    print("Scraping completed successfully!")
    print("Data Properties: ")
    print(review_data.head()) # Print the first few rows of the scraped data
    print(review_data.tail()) # Print the first few rows of the scraped data
    print(f"Records: {review_data.shape[0]}\nFeatures:{review_data.shape[1]}")   
else:
    print("No data was scraped. Check the URL or the scraper logic.")

# Now load the data incrementally
loader = DataLoader(file_path="data/scraped_reviews_raw.csv")
loader.incremental_load(review_data)

