import requests
from bs4 import BeautifulSoup as bscraper
import pandas as pd
import time


class ReviewScraper:

    def __init__(self, base_url):
        """
        Initialize the ReviewScraper with the base URL, an empty DataFrame,
        and an empty class-level attribute for bs4 object.
        Args:
            base_url (str): The base URL of the review website.
        """
        self.base_url = base_url
        self.data = pd.DataFrame()
        self.soup = None  # Class-level attribute for BeautifulSoup object

    def fetch_html(self, url):
        """
        Fetch the HTML content of a given URL and initialize the soup object.
        Args:
            url (str): The URL to fetch.
        Returns:
            str: The HTML content of the page.
        """
        try:
            print(f"Fetching URL: {url}")
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            self.soup = bscraper(response.text, 'html.parser')  # Initialize soup
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching URL {url}: {e}")
            self.soup = None
            return None

    def extract_review_data(self, tag, attr, value):
        """
        Extract specific review data (categorical or star ratings) from the soup object.
        Args:
            tag (str): HTML tag to search for.
            attr (str): Attribute to locate the header within the review-stats section.
            value (str): Class name or type of value to extract (e.g., "stars").
        Returns:
            tuple: Column name and list of values.
        """
        if not self.soup:
            print("Soup object not initialized.")
            return None, []

        try:
            review_stats = self.soup.find_all("div", "review-stats")  # Limit search to review-stats section
            data = []
            column_name = None

            for code in review_stats:
                for table in code.find_all("table", "review-ratings"):  # Look inside review-ratings table
                    header = table.find(tag, attr)

                    if header:
                        column_name = header.get_text(strip=True)  # Extract column name
                        if value == "stars":
                            # Extract star ratings
                            stars_td = header.find_next("td", "stars")
                            if stars_td:
                                last_star_fill = (
                                    stars_td.find_all("span", "star fill")[-1]
                                    if stars_td.find_all("span", "star fill")
                                    else None
                                )
                                data.append(
                                    int(last_star_fill.get_text())
                                ) if last_star_fill else data.append(None)
                            else:
                                data.append(None)  # Default value if stars are not found
                        else:
                            # Extract normal categorical columns
                            next_td = header.find_next(tag, value)
                            data.append(next_td.get_text(strip=True) if next_td else None)
                    else:
                        data.append(None)  # Default value if header is not found

            return column_name, data
        
        except Exception as e:
            print(f"Error extracting review data: {e}")
            return None, []

    def parse_reviews(self):
        """
        Parse reviews and associated metadata from the soup object.
        Returns:
            pd.DataFrame: DataFrame containing extracted reviews and metadata.
        """
        if not self.soup:
            print("Soup object not initialized.")
            return pd.DataFrame()
        
        try:
            # Extract primary review data            
            review_id = [review_div['id'] for review_div in self.soup.find_all('div', class_='body') if 'id' in review_div.attrs]
            review_header = [header.get_text(strip=True) for header in self.soup.find_all("h2", "text_header")]
            review_meta = [meta.get_text(strip=True) for meta in self.soup.find_all("h3", "text_sub_header userStatusWrapper")]
            reviews = [text.get_text(strip=True) for text in self.soup.find_all("div", "text_content")]
            rating_value = [
                int(rate.get_text(strip=True).split('/')[0])
                if rate.find_all("span", itemprop="ratingValue") else None
                for rate in self.soup.find_all("div", "rating-10")
            ][1:]

            # Extract additional metadata
            aircraft, val_air = self.extract_review_data("td", "aircraft", "review-value")
            route, val_route = self.extract_review_data("td", "route", "review-value")
            travel, val_travel = self.extract_review_data("td", "type_of_traveller", "review-value")
            seat, val_seat = self.extract_review_data("td", "cabin_flown", "review-value")
            date_flown, val_date = self.extract_review_data("td", "date_flown", "review-value")
            recommended, val_recommend = self.extract_review_data("td", "recommended", "review-value")

            # Extract star rating columns
            seat_comfort, val_seat_comf = self.extract_review_data("td", "seat_comfort", "stars")
            cabin, val_cabin = self.extract_review_data("td", "cabin_staff_service", "stars")
            food, val_food = self.extract_review_data("td", "food_and_beverages", "stars")
            inflight, val_inflight = self.extract_review_data("td", "inflight_entertainment", "stars")
            ground_service, val_ground_service = self.extract_review_data("td", "ground_service", "stars")
            wifi_conn, val_wifi = self.extract_review_data("td", "wifi_and_connectivity", "stars")
            value_money, val_value_money = self.extract_review_data("td", "value_for_money", "stars")

            #Create a DataFrame
            df = pd.DataFrame({
                'Review ID': review_id,
                'Review Title': review_header,
                'Review Meta': review_meta,
                'Reviews': reviews,
                aircraft: val_air,
                route: val_route,
                travel: val_travel,
                seat: val_seat,
                date_flown: val_date,
                'Overall Rating': rating_value,
                seat_comfort: val_seat_comf,
                cabin: val_cabin,
                food: val_food,
                inflight: val_inflight,
                ground_service: val_ground_service,
                wifi_conn: val_wifi,
                value_money: val_value_money,
                recommended: val_recommend
            })

            return df

            '''# Debug: Print lengths of all extracted lists
            print("Lengths of extracted data:")
            print(f"Review Title: {len(review_header)}")
            print(f"Review Meta: {len(review_meta)}")
            print(f"Reviews: {len(reviews)}")
            print(f"Overall Rating: {len(rating_value)}")
            print(f"Aircraft: {len(val_air)}")
            print(f"Route: {len(val_route)}")
            print(f"Travel Type: {len(val_travel)}")
            print(f"Seat: {len(val_seat)}")
            print(f"Date Flown: {len(val_date)}")
            print(f"Recommended: {len(val_recommend)}")
            print(f"Seat Comfort: {len(val_seat_comf)}")
            print(f"Cabin: {len(val_cabin)}")
            print(f"Food: {len(val_food)}")
            print(f"Inflight: {len(val_inflight)}")
            print(f"Ground Service: {len(val_ground_service)}")
            print(f"WiFi Connectivity: {len(val_wifi)}")
            print(f"Value for Money: {len(val_value_money)}")

            return pd.DataFrame'''
            
        except Exception as e:
            print(f"Error parsing reviews: {e}")
            return pd.DataFrame()

    def get_total_pages(self):
        """
        Extract the total number of pages from the soup object.
        Returns:
            int: Total number of pages.
        """
        if not self.soup:
            print("Soup object not initialized.")
            return 1
        
        try:
            nav = self.soup.find_all('article', 'comp comp_reviews-pagination querylist-pagination position-')
            total_pages = [
                int(page.find_all("a", attrs={'href': True, 'class': False})[-2].get_text())
                for page in nav
            ][0]
            return total_pages
        except (IndexError, ValueError) as e:
            print(f"Error extracting total pages: {e}. Defaulting to 1.")
            return 1

    def scrape_all_reviews(self, start_page=1, end_page=None):
        """
        Scrape reviews across specified or all pages, recording time taken for each page.
        Args:
            pages (int, optional): Number of pages to scrape. Defaults to all pages.
        Returns:
            pd.DataFrame: Combined DataFrame of all reviews.
        """

        #checking if first page can be fetched; if not, no need to proceed.
        first_page_html = self.fetch_html(self.base_url)
        if not first_page_html:
            return pd.DataFrame()

        #get total pages (overall) and pages to scrape
        total_pages = self.get_total_pages()
        pages_to_scrape = end_page if end_page else total_pages
        print(f"Scraping {pages_to_scrape-start_page+1} pages out of {total_pages} total pages.")

        #extraction starts here
        all_data = []
        for page_number in range(start_page, pages_to_scrape + 1):
            url = f"{self.base_url}/page/{page_number}"
            print(f"Scraping page {page_number}...")

            try:
                start_time = time.time()
                self.fetch_html(url)  # Fetch and parse the page
                page_data = self.parse_reviews()
                if not page_data.empty:
                    all_data.append(page_data)
                end_time = time.time()
                duration = round(end_time - start_time, 2)
                print(f"Scraped page {page_number} ({duration}s)")
            except Exception as e:
                print(f"Error scraping page {page_number}: {e}")
                continue

        self.data = pd.concat(all_data, ignore_index=True)
        return self.data