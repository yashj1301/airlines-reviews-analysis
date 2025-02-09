<u><h2>Documentation for loader.py</h2></u>

<h3>Overview</h3>

The `loader.py` script defines the **DataLoader** class, which handles incremental data loading. It is responsible for loading, saving, and appending new review data to a CSV file, ensuring that there are no duplicate records. It performs the following tasks:

1. Checks if an existing data file is available.
2. Loads existing data if present.
3. Saves new data to the file, ensuring that only new records are appended.
4. Handles the case where the data file does not exist, creating a new file.

<h3>Requirements</h3> 

The following Python libraries are required:
- `pandas`: For data manipulation and reading/writing CSV files.
- `os`: For checking file existence.

<h3>Class Definition</h3>
class DataLoader: The class encapsulates the logic for loading and saving review data.

<h4>1. Class Constructor</h4>

    def __init__(self, file_path):
    """
    Initializes the DataLoader with the file path to save/load data.
    Args:
    file_path (str): The path to the CSV file where data will be saved.
    """
    
<u>Purpose</u>: Sets up the initial state for the data loader, including file_path: Path to the CSV file for saving/loading data.

<h4>2. Method: load_existing_data</h4>

        def load_existing_data(self):
        """
        Loads existing data from the specified file if it exists.
        Returns:
            pd.DataFrame: Existing data (empty DataFrame if file doesn't exist).
        """
<u>Purpose</u>: Loads the existing data from the specified CSV file. If the file doesn't exist, it returns an empty DataFrame.

<u>Logic</u>: Checks if the file exists and reads it into a DataFrame. If no file is found, an empty DataFrame is returned.

<h4>3. Method: save_data</h4>

    def save_data(self, data):
        """
        Saves the new data to the specified file, appending if the file exists.
        Args:
            data (pd.DataFrame): The new data to save.
        """
<u>Purpose</u>: Saves the new data to the CSV file, appending if the file already exists.

<u>Logic</u>: If the file already contains data, it appends new data to it, ensuring no duplicates are added by using Review ID as a unique identifier.

<u>Error Handling</u>: Handles cases where the file is missing by creating a new file.

<h4>4. Method: incremental_load</h4>

    def incremental_load(self, new_data):
        """
        Perform incremental load by checking for new data and appending it.
        Args:
            new_data (pd.DataFrame): The new data to load incrementally.
        """
        
<u>Purpose</u>: Manages the incremental load of data by checking for new records and appending them to the existing dataset.

<u>Logic</u>: Calls the save_data method to append the new data. The main purpose of this method is to ensure that only new data is added during incremental scraping sessions.

<h3> How to Use the <code>DataLoader</code> Class </h3> 

<ol> 
<li><b>Initialize the DataLoader:</b></li>

    file_path = "scraped_reviews.csv"
    loader = DataLoader(file_path)

<li><b>Load Existing Data (if any):</b></li> 

<ol type=I> 
<li>Load all existing data from the file:</li>

        existing_data = loader.load_existing_data()
        print(existing_data.head())

<li>Incremental Loading of New Data:</li> 

<ol type=a> 
<li>After scraping new data, perform incremental loading:</li>
    
    loader.incremental_load(new_data)
</ol> 
</ol>

<b>Example:</b>

<code>

import pandas as pd
from loader import DataLoader

<raw style="color:green"> # Create the DataLoader object </raw>
file_path = "scraped_reviews.csv"
loader = DataLoader(file_path)

<raw style="color:green"> # Simulate new data scraped </raw>
new_data = pd.DataFrame({
    'Review ID': [101, 102, 103],
    'Review Title': ['Great flight', 'Comfortable seats', 'Good service'],
    'Review Meta': ['USA', 'UK', 'Canada'],
    'Reviews': ['Excellent experience', 'Very comfortable', 'Nice crew'],
    # Add more columns as necessary
})

<raw style="color:green"> # Perform incremental load</raw>
loader.incremental_load(new_data)

</code>

<i><b>Additional Notes:</b></i>

1. <u>Error Handling</u>: The script handles missing files by creating a new one. It also prevents duplicates by checking the Review ID.
2. <u>Scalability</u>: This script is flexible and can scale with larger datasets as it uses pandas to efficiently append data and handle large volumes.
3. <u>Data Integrity</u>: The use of a primary key (Review ID) ensures that data integrity is maintained across incremental loading.