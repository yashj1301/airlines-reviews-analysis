<u><h2>Documentation for `scraper.py`</h2></u>

<h3>Overview</h3>

The `scraper.py` script defines the "ReviewScraper" class, which is used to scrape customer reviews from a website. It handles:

1. Fetching HTML content from specified URLs.
2. Parsing customer review data, including metadata and star ratings.
3. Handling errors and incomplete data gracefully.
4. Combining the scraped data into a well-structured pandas DataFrame.

<h3>Requirements</h3>
The following Python libraries are required:

1. `requests`: For sending HTTP requests to fetch webpage content.
2. `BeautifulSoup` from bs4: For parsing and navigating HTML content.
3. `pandas`: For structuring and manipulating tabular data.
4. `time`: For recording the time taken for operations.

<h3>Class Definition</h3>

class `ReviewScraper`: The class encapsulates all functionality for scraping and processing customer reviews.

<b>1. Class Constructor</b>

        def __init__(self, base_url):
        """
        Initialize the ReviewScraper with the base URL and an empty DataFrame.
        Args:
        base_url (str): The base URL of the review website.
        """

<u>Purpose</u>: Sets up the initial state for the scraper, including:<ul>
    <li> base_url: Base URL of the review site.
    <li>data: Placeholder for the final scraped DataFrame.
    <li>soup: A BeautifulSoup object for HTML parsing, initially None.</ul>

**2. Method: fetch_html**

        def fetch_html(self, url):
            """
            Fetch the HTML content of a given URL and initialize the soup object.
            Args:
                url (str): The URL to fetch.
            Returns:
                str: The HTML content of the page.
            """
<u>Purpose</u>: Fetches the HTML content of a given URL and initializes the soup object for parsing.

<u>Error Handling</u>: If the request fails (e.g., network issue or server error), it logs the error and sets soup to None.

**3. Method: extract_review_data**

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

<u>Purpose</u>: Extracts specific review data, including categorical data (e.g., Aircraft, Route) and star ratings (e.g., Seat Comfort, Cabin).
Searches within the review-stats section of the HTML to narrow the scope.

<u>Logic</u>: Handles both normal data (value) and star ratings (stars) differently.
Provides default values (None) for missing elements to ensure consistency.

**4. Method: parse_reviews**

        def parse_reviews(self):
            """
            Parse reviews and associated metadata from the soup object.
            Returns:
                pd.DataFrame: DataFrame containing extracted reviews and metadata.
            """

Purpose: Combines data from various sections of the HTML (titles, metadata, reviews, star ratings) into a structured DataFrame.

<u>Key Steps</u>:

<ol type=a>
<li> Extracts primary review data (e.g., Review Title, Review Meta, Reviews, Overall Rating).</li>
<li>Extracts additional metadata (e.g., Aircraft, Route, Travel Type).
<li>Extracts star ratings (e.g., Seat Comfort, Cabin, Food).
<li>Combines all data into a pandas DataFrame.</li></ol>

<u>Error Handling</u>: Logs and returns an empty DataFrame if an error occurs during parsing.

**5. Method: get_total_pages**

    def get_total_pages(self):
        """
        Extract the total number of pages from the soup object.
        Returns:
            int: Total number of pages.
        """
<u>Purpose</u>: Determines the total number of review pages from the pagination section of the HTML.

<u>Error Handling</u>: Defaults to 1 page if the pagination structure is missing or incorrectly parsed.

**6. Method: scrape_all_reviews**

    def scrape_all_reviews(self, pages=None):
        """
        Scrape reviews across specified or all pages, recording time taken for each page.
        Args:
            pages (int, optional): Number of pages to scrape. Defaults to all pages.
        Returns:
            pd.DataFrame: Combined DataFrame of all reviews.
        """

<u>Purpose</u>: Orchestrates the scraping process by iterating over multiple pages.
Combines data from all scraped pages into a single DataFrame.

<u>Key Steps</u>:<ol type=a>
<li>Fetches the first page to determine the total number of pages.
<li>Iteratively fetches and parses each page, logging time taken for each.
<li>Appends parsed data to the data attribute.</li></ol>

<u>Error Handling</u>: If a page fails to scrape, logs the error and continues with the next page.

<h3>How to Use the ReviewScraper Class</h3>

<ol>
<li>Initialize the Scraper:</li>

        base_url = "https://www.airlinequality.com/airline-reviews/british-airways"
        scraper = ReviewScraper(base_url)

<li>Scrape Reviews:

<ol type=a>
<li>Scrape all pages:</li>

    all_reviews = scraper.scrape_all_reviews()
    print(all_reviews.head())

<li>Scrape a limited number of pages:</li>

    limited_reviews = scraper.scrape_all_reviews(pages=5)
    print(limited_reviews.head())

