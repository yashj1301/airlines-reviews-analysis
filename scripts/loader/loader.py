import pandas as pd
import os

class DataLoader:
    def __init__(self, file_path):
        """
        Initializes the DataLoader with the file path to save/load data.
        Args:
            file_path (str): The path to the CSV file where data will be saved.
        """
        self.file_path = file_path

    def load_existing_data(self):
        """
        Loads existing data from the specified file if it exists.
        Returns:
            pd.DataFrame: Existing data (empty DataFrame if file doesn't exist).
        """
        if os.path.exists(self.file_path):
            print(f"Loading existing data from {self.file_path}...")
            return pd.read_csv(self.file_path)
        else:
            print(f"No existing data found. Creating a new file at {self.file_path}.")
            return pd.DataFrame()

    def save_data(self, data):
        """
        Saves the new data to the specified file, appending if the file exists.
        Args:
            data (pd.DataFrame): The new data to save.
        """
    
        # Check if the file exists
        existing_data = self.load_existing_data()
        
        if existing_data.empty:
            print("No existing data found, saving all data...")
            data.to_csv(self.file_path, index=False)
        else:
            # Filter out existing data to avoid duplicates based on 'Review ID'
            print("Merging new data with existing data...")
            combined_data = pd.concat([existing_data, data]).drop_duplicates(subset="Review ID", keep="last")
            combined_data.to_csv(self.file_path, index=False)

    def incremental_load(self, new_data):
        """
        Perform incremental load by checking for new data and appending it.
        Args:
            new_data (pd.DataFrame): The new data to load incrementally.
        """
        self.save_data(new_data)
