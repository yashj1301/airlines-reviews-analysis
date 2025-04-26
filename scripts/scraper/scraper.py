import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup as bs
import math
import time


class AirlineReviewScraper:

    def __init__(self, airline_name):
        """
        Initialize the ReviewScraper with the base URL, an empty DataFrame,
        and an empty class-level attribute for bs4 object.
        Args:
            airline_name (str): Name of the airline to scrape reviews for.
            review_type (str): Type of reviews to scrape ('seat', 'lounge', 'airline','all').
        """
        self.airline_name = airline_name
        
        # variables to store each type of review
        self.airline_reviews = None
        self.seat_reviews = None
        self.lounge_reviews = None

    async def fetch_page(self,session, url):
        """Fetches a single webpage asynchronously."""
        async with session.get(url) as response:
            return await response.text()

    async def fetch_all_pages(self,fetch_function, BASE_URL, total_pages, *args):
        """
        Generic function to fetch data asynchronously from multiple pages.
        
        :param fetch_function: Function to call (either extract_columns or extract_reviews).
        :param BASE_URL: The URL template for requests.
        :param total_pages: Number of pages to scrape.
        :param args: Additional arguments required by the fetch function.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_function(session, BASE_URL.format(i), *args) for i in range(1, total_pages + 1)]
            results = await asyncio.gather(*tasks)  # Fetch all pages concurrently

        return results

    async def extract_headers(self,session, url):
        """Extracts column names from a single page."""
        html = await self.fetch_page(session, url)
        soup = bs(html, "html.parser")

        columns = set()  # Store unique column names for this page
        review_sections = soup.find_all("div", class_="body")  # Find all reviews

        for review in review_sections:
            for row in review.find_all("tr"):  # Extract column names dynamically
                key_tag = row.find("td")  # First <td> contains column name
                if key_tag:
                    column_name = key_tag.get_text(strip=True)
                    columns.add(column_name)  # Add to the set

        return columns

    async def extract_reviews(self,session, url, all_columns):
        """Extracts review data dynamically from a single page."""
        html = await self.fetch_page(session, url)
        soup = bs(html, "html.parser")

        reviews_data = []  # Store structured review data

        # Find all reviews on the page
        review_sections = soup.find_all("article", itemprop="review")

        for review in review_sections:
            review_data = {col: None for col in all_columns}  # Initialize all values as None

            # Extract reviewer ID
            reviewer_div = review.find("div", id=True)  # Find the div that has an ID

            if reviewer_div:  # Ensure div exists
                review_id = reviewer_div.get("id")  # Extract the ID
                if review_id and review_id.startswith("anchor"):  # Check if it starts with 'anchor'
                    review_id = review_id.replace("anchor", "")  # Remove 'anchor' prefix
                else:
                    review_id = None  # If ID exists but is not formatted correctly
            else:
                review_id = None  # If no div with ID is found

            review_data["Review ID"] = review_id  # Store in dictionary

            
            # Extract review title
            title_tag = review.find("h2", class_="text_header")
            review_data["Review Title"] = title_tag.get_text(strip=True) if title_tag else None

            # Extract review metadata (author, date, etc.)
            meta_tag = review.find("h3", class_="text_sub_header userStatusWrapper")
            review_data["Review Meta"] = meta_tag.get_text(strip=True) if meta_tag else None

            # Extract review content
            content_tag = review.find("div", class_="text_content")
            review_data["Review Content"] = content_tag.get_text(strip=True) if content_tag else None

            # Extract overall rating
            rating_container = review.find("div", itemprop="reviewRating")  # Locate overall rating container
            review_data["Overall Rating"] = rating_container.get_text(strip=True).split("/")[0] if rating_container else None
    
            # Extract Star Ratings
            for row in review.find_all("tr"):
                key_tag = row.find("td")  # First <td> contains column name
                value_tag = row.find("td", class_="review-value") or row.find("td", class_="stars")

                if key_tag:
                    key = key_tag.get_text(strip=True)  # Parameter name

                    if value_tag:
                        if "stars" in value_tag.get("class", []):  # If it's a star rating column
                            stars_filled = len(value_tag.find_all("span", class_="star fill"))  # Count filled stars ⭐
                            review_data[key] = stars_filled  # Assign numerical value (e.g., 4 stars → 4)
                        else:
                            review_data[key] = value_tag.get_text(strip=True)  # Assign text value

            reviews_data.append(review_data)  # Store review data

        return reviews_data
    
    async def scrape_all_reviews(self, review_type):
        """Extracts all reviews dynamically and creates a structured DataFrame.
        
        :param type: Type of reviews to extract ('all', 'seat','lounge','airline').

        :return: Structured DataFrame containing all reviews.
        """

        BASE_URL = "https://www.airlinequality.com/{type}/{airline}/page/{{}}/".format(type=review_type.lower()+"-reviews",
                                                                                   airline=self.airline_name.replace(" ","-").lower())
 
        #fetching number of pages to scrape
        async with aiohttp.ClientSession() as session:
            response = await session.get(BASE_URL.format(1))
            html = await response.text()
            soup = bs(html, "html.parser")
            nav = soup.find("span", itemprop="reviewCount")
            total_pages = math.ceil(int(nav.get_text(strip=True).split()[0])/10)
        
        print(f"Extracting {review_type} reviews of {self.airline_name} from {total_pages} pages...")

        # Step 1: Extract all unique column names dynamically
        all_columns = set().union(*await self.fetch_all_pages(self.extract_headers, BASE_URL, total_pages))

        # Add fixed columns that will always be present
        all_columns.update(["Review ID", "Review Title", "Review Meta", "Review Content", "Overall Rating"])

        # Step 2: Extract all reviews from all pages
        all_reviews = await self.fetch_all_pages(self.extract_reviews, BASE_URL, total_pages, all_columns)
        
        # Flatten the list
        all_reviews_flat = [review for page_reviews in all_reviews for review in page_reviews]

        #print confirmation
        print(f"Extracted {len(all_reviews_flat)} {review_type} reviews for {self.airline_name}.")

        # Convert to Pandas DataFrame
        df = pd.DataFrame(all_reviews_flat)

        return df
    
    async def extract_all_reviews(self, review_type):
        """
        Main function to extract reviews based on the review type.

        Args:
            type(str): Review Type to extract -> ['all','airline','seat','lounge']
        
        :return: DataFrame containing the extracted reviews.
        """

        if review_type=="all":
            self.airline_reviews, self.seat_reviews, self.lounge_reviews = \
            await asyncio.gather(
                self.scrape_all_reviews('airline'),
                self.scrape_all_reviews('seat'),
                self.scrape_all_reviews('lounge')
                )
            
        elif review_type =="airline": self.airline_reviews = await self.scrape_all_reviews('airline')
        elif review_type =="seat":    self.seat_reviews = await self.scrape_all_reviews('seat')
        elif review_type =="lounge":  self.lounge_reviews = await self.scrape_all_reviews('lounge')