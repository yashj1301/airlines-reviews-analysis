## Documentation for `scraper.py`

### Overview  
The `scraper.py` module defines the **`AirlineReviewScraper`** class, which asynchronously fetches and parses customer reviews from Skytrax by airline, seat, and lounge categories. 

It uses `aiohttp` for concurrent HTTP requests and `BeautifulSoup` to extract both review metadata and detailed star-rating tables, returning each dataset as a pandas DataFrame.

---

### Requirements  
- **aiohttp**  
- **asyncio** (standard library)  
- **pandas**  
- **beautifulsoup4**  
- **math** (standard library)  
- **time** (standard library)  

---

### Class Definition  
```python
class AirlineReviewScraper:
    def __init__(self, airline_name: str): …
    async def fetch_page(self, session, url: str) -> str: …
    async def fetch_all_pages(self, fetch_function, BASE_URL: str, total_pages: int, *args) -> list: …
    async def extract_headers(self, session, url: str) -> set: …
    async def extract_reviews(self, session, url: str, all_columns: set) -> list: …
    async def scrape_all_reviews(self, review_type: str) -> pd.DataFrame: …
    async def extract_all_reviews(self, review_type: str) -> None: …
```

#### 1. Constructor

```python
def __init__(self, airline_name: str):
    """
    Initializes the scraper.
    - Stores `airline_name`.
    - Prepares placeholders: `airline_reviews`, `seat_reviews`, `lounge_reviews`.
    """
```

#### 2. `fetch_page(self, session, url)`

```python
async def fetch_page(self, session, url: str) -> str:
    """
    Performs a single HTTP GET asynchronously and returns HTML text.
    """
```

__Parameters__

- `session`: an `aiohttp.ClientSession` instance.
- `url (str)`: the page URL to fetch.

__Returns__

- `str`: raw HTML of the page.

#### 3. `fetch_all_pages(self, fetch_function, BASE_URL, total_pages, *args)`

```python
async def fetch_all_pages(self,
                          fetch_function,
                          BASE_URL: str,
                          total_pages: int,
                          *args) -> list:
    """
    Runs `fetch_function` concurrently over pages 1..total_pages.
    Uses additional arguments to implement functions with varying parameters
    i.e. extract_headers() & extract_reviews().
    """
```

This function is created to run a function concurrently over each page. It is designed to handle multiple varying parameters, based on the function being called. 

__Parameters__

- `fetch_function`: an async method (extract_headers or extract_reviews).
- `BASE_URL (str)`: URL template containing "{} for page numbers.
- `total_pages (int)`: number of pages to fetch.
- `*args`: extra arguments to pass into fetch_function.

__Returns__

`list`: list of results from each page (sets for headers or lists for reviews)

#### 4. `extract_headers(self, session, url)`

```python
async def extract_headers(self, session, url: str) -> set:
    """
    Parses one page to collect unique table column names.
    """
```

This function is run for all pages, and the set of the headers (unique values) is used as headers in the final dataframe. 

__Parameters__

- `session`: an `aiohttp.ClientSession` instance.
- `url (str)`: the page URL to fetch.

__Returns__

- `set`: unique column names from all `<tr>` rows.

#### 5. `extract_reviews(self, session, url, all_columns)`

```python
async def extract_reviews(self,
                          session,
                          url: str,
                          all_columns: set) -> list:
    """
    Extracts row-by-row review data into dicts keyed by `all_columns`.
    """
```

This function goes into each review, extracts its data and appends to the list which is returned through it. 

__Parameters__

- `session`: an `aiohttp.ClientSession` instance.
- `url (str)`: the page URL to fetch.
- `all_columns (set)`: full set of expected columns.

__Returns__

- `list of dicts`: each dict represents one review with all columns populated or None.

#### 6. `scrape_all_reviews(self, review_type)`

```python
async def scrape_all_reviews(self, review_type: str) -> pd.DataFrame:
    """
    Coordinates header extraction and review scraping for `review_type`.
    - Determines total_pages from the first page.
    - Gathers headers, then reviews concurrently.
    - Flattens results and returns a DataFrame.
    """
```

This is the downstream function which is used to scrape the actual data from the website. It extracts all headers and reviews, and shapes them into a single dataframe. 

To make the scraping conditional, we use an additional function `extract_all_reviews()` which simply calls this function based on the review type. 

__Parameters__

- `review_type (str)`: "airline", "seat", or "lounge".

__Returns__

- `pd.DataFrame`: rows of scraped reviews with dynamic columns.

#### 7. `extract_all_reviews(self, review_type)`

```python
async def extract_all_reviews(self, review_type: str) -> None:
    """
    Top-level entry point.
    - If `review_type == "all"`, runs all three scrapes in parallel.
    - Otherwise runs a single-category scrape.
    - Populates: `self.airline_reviews`, `self.seat_reviews`, `self.lounge_reviews`.
    """
```

This function is the top-level function used to trigger the scraping. Its argument `review_type` is used to apply condition to the scraping - only scrape the required data. It populates the class attributes `airline_reviews`, `seat_reviews` and `lounge_reviews`. 

### Usage Example

```python
import time
import asyncio
from scripts.scraper.scraper import AirlineReviewScraper

scraper = AirlineReviewScraper("British Airways")

start = time.time()
asyncio.run(scraper.extract_all_reviews("all"))
end = time.time()

print(f"Elapsed: {end-start:.2f}s")
print(scraper.airline_reviews.head())
print(scraper.seat_reviews.head())
print(scraper.lounge_reviews.head())
```