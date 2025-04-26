import pandas as pd
import boto3
from botocore.exceptions import ClientError
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()  # Load AWS credentials from .env

s3 = boto3.client("s3")

class AirlineReviewLoader:

    def __init__(self, airline_name, data_type="raw"):
        
        """
        Initializes the AirlineReviewLoader with airline name and data type.
        Args:
            airline_name (str): Name of the airline.
            data_type (str): Type of data ('raw' or 'tf').
        """

        self.airline_name = airline_name
        self.data_type = data_type  # 'raw' or 'tf'
        
        # dataframes for each type of review
        self.airline_reviews = None
        self.seat_reviews = None
        self.lounge_reviews = None


    def ensure_bucket_exists(self, bucket_name, flag=0):

        """Checks if the S3 bucket exists, and creates it if not.
        
        Args:
            bucket_name (str): Name of the S3 bucket.
            flag (int): Flag to indicate whether to create the bucket (1) or not (0)."""
        
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists.")
        
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            
            if error_code == 404:
                print(f"Bucket '{bucket_name}' does not exist.")

                if flag == 1:
                    
                    print(f"Creating bucket '{bucket_name}'...")
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={
                            'LocationConstraint': os.getenv("AWS_DEFAULT_REGION")
                                                }
                    )
                    print(f"Bucket '{bucket_name}' created successfully.")
            
            else: raise e
    
    def upload_df(self, bucket_name, object_key, df):

        """Uploads a Pandas DataFrame as a CSV file to S3.
        
        Args:
            bucket_name (str): Name of the S3 bucket.
            object_key (str): S3 object key (path) where the CSV will be stored.
            df (pd.DataFrame): DataFrame to upload."""

        if df is None or df.empty:
            print(f"Warning: {object_key} is empty, skipping upload.")
            return

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        print(f" Uploaded to s3://{bucket_name}/{object_key}")

    def download_df(self, bucket_name, review_type):

        """Downloads a review CSV from S3 into a DataFrame.
        
        Args:
            bucket_name (str): Name of the S3 bucket.
            review_type (str): Type of reviews to download ('airline', 'seat', 'lounge').
        """

        file_name_map = {
            "airline": "airline_reviews.csv",
            "seat": "seat_reviews.csv",
            "lounge": "lounge_reviews.csv",
        }

        
        if review_type not in file_name_map:
            print(f" Invalid review type '{review_type}'.")
            return None

        object_key = f"{self.airline_name.lower().replace(' ', '_')}/{self.data_type}/{file_name_map[review_type]}"

        try:

            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            content = response["Body"].read().decode("utf-8")
            df = pd.read_csv(StringIO(content))
            print(f" Downloaded from s3://{bucket_name}/{object_key}")
            return df
        
        except Exception as e:
            print(f"Error downloading {review_type} stats: {e}")
            return None

    def load_data(self, bucket_name, load_type, review_type="all"):
    
        """
        Loads airline review stats data to/from S3.

        Args:
            bucket_name (str): Name of the S3 bucket.
            load_type (str): Type of load operation ('upload' or 'download').
            review_type (str): Type of reviews to load ('all', 'airline','seat','lounge').
        """            
        
        if load_type not in ["upload", "download"]:
            print(f"Invalid load type '{load_type}'. Must be 'upload' or 'download'.")
            return

        if review_type not in ["all", "airline", "seat", "lounge"]:
            print(f"Invalid stat type '{review_type}'. Must be 'all', 'airline', 'seat', or 'lounge'. ")
            return

        # Perform the upload operation
        if load_type == "upload":

            # Ensure the S3 bucket exists
            self.ensure_bucket_exists(bucket_name,flag=1)

            print(f"Uploading {self.airline_name}'s {self.data_type} {review_type} data to S3...")
            
            base_folder = f"{self.airline_name.lower().replace(' ', '_')}/{self.data_type}/"

            if self.airline_reviews is not None and review_type in ["all", "airline"]:
                print("Uploading airline reviews...")
                self.upload_df(bucket_name, base_folder + "airline_reviews.csv", self.airline_reviews)
                print("Airline reviews uploaded.")

            if self.seat_reviews is not None and review_type in ["all", "seat"]:
                print("Uploading seat reviews...")
                self.upload_df(bucket_name, base_folder + "seat_reviews.csv", self.seat_reviews)
                print("Seat reviews uploaded.")

            if self.lounge_reviews is not None and review_type in ["all", "lounge"]:
                print("Uploading lounge reviews...")
                self.upload_df(bucket_name, base_folder + "lounge_reviews.csv", self.lounge_reviews)
                print("Lounge reviews uploaded.")

            print(f"All {self.data_type} data uploaded to s3://{bucket_name}/{base_folder}")

        # Perform the download operation
        elif load_type == "download":

            # Ensure the S3 bucket exists
            self.ensure_bucket_exists(bucket_name)

            print(f"Downloading {self.airline_name}'s {self.data_type} data from S3...")

            if review_type in ["all", "airline"]:
                print("Downloading airline reviews...")
                self.airline_reviews = self.download_df(bucket_name, "airline")
                print("Airline reviews downloaded.")

            if review_type in ["all", "seat"]:
                print("Downloading seat reviews...")
                self.seat_reviews = self.download_df(bucket_name, "seat")
                print("Seat reviews downloaded.")

            if review_type in ["all", "lounge"]:
                print("Downloading lounge reviews...")
                self.lounge_reviews = self.download_df(bucket_name, "lounge")
                print("Lounge reviews downloaded.")
                
            print(f"All {self.data_type} data downloaded from s3://{bucket_name}/{self.airline_name.lower().replace(' ', '_')}/{self.data_type}/")
