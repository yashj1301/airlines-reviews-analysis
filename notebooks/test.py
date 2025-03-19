import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup as bs

BASE_URL = "https://www.airlinequality.com/airline-reviews/british-airways/page/{}/"

async def fetch_page(session, url):
    """Fetches a webpage asynchronously."""
    async with session.get(url) as response:
        return await response.text()
    
async def extract_column_names_from_page(session, url):
    """Extracts column names from a single page."""
    html = await fetch_page(session, url)
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

async def extract_all_column_names(total_pages=393):
    """Extracts column names from all pages."""
    async with aiohttp.ClientSession() as session:
        tasks = [extract_column_names_from_page(session, BASE_URL.format(i)) for i in range(1, total_pages + 1)]
        all_columns_per_page = await asyncio.gather(*tasks)  # Run all page extractions concurrently

    # Finding common and unique columns across all pages
    all_columns = set().union(*all_columns_per_page)  # All unique columns
    common_columns = set.intersection(*all_columns_per_page)  # Only columns that appear on every page

    return all_columns

async def extract_reviews_from_page(session, url, all_columns):
    """Extracts review data dynamically from a single page, fixing star ratings & overall rating."""
    html = await fetch_page(session, url)
    soup = bs(html, "html.parser")

    reviews_data = []  # Store structured review data

    # Find all reviews on the page
    review_sections = soup.find_all("article", itemprop="review")

    for review in review_sections: # Extract data for each review
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

        review_data["Reviewer ID"] = review_id  # Store in dictionary

        
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
                        stars_filled = len(value_tag.find_all("span", class_="star fill"))  # Count filled stars
                        review_data[key] = stars_filled  # Assign numerical value (e.g., 4 stars → 4)
                    else:
                        review_data[key] = value_tag.get_text(strip=True)  # Assign text value

        reviews_data.append(review_data)  # Store review data

    return reviews_data

async def scrape_all_reviews(total_pages=393):
    """Extracts all reviews dynamically and creates a structured DataFrame."""
    
    # Step 1: Extract all unique column names dynamically
    all_columns = await extract_all_column_names(total_pages)

    # Add fixed columns that will always be present
    all_columns.update(["Reviewer ID", "Review Title", "Review Meta", "Review Content", "Overall Rating"])

    async with aiohttp.ClientSession() as session:
        tasks = [extract_reviews_from_page(session, BASE_URL.format(i), all_columns) for i in range(1, total_pages + 1)]
        all_reviews = await asyncio.gather(*tasks)  # Run all page extractions concurrently

    # Flatten the list (since gather returns a list of lists)
    all_reviews_flat = [review for page_reviews in all_reviews for review in page_reviews]

    # Convert to Pandas DataFrame
    df = pd.DataFrame(all_reviews_flat)

    return df

# Example usage: Scrape all reviews dynamically
df_reviews = asyncio.run(scrape_all_reviews(393))

# Display the DataFrame
print(df_reviews.info())
print(df_reviews.head())