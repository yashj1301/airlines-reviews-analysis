## Documentation for `loader.py`

### Overview  
The `loader.py` module defines the **`AirlineReviewLoader`** class, which handles uploading and downloading of airline review DataFrames to and from an AWS S3 bucket. It can create the bucket on‐demand (for uploads only), serialize DataFrames to CSV in memory, and read them back into pandas.

---

### Requirements  
- **pandas**  
- **boto3**  
- **python-dotenv**  
- **botocore** (for `ClientError`)  
- **io** (standard library)  
- **os** (standard library)  

Make sure to have a `.env` file (git-ignored) with the AWS credentials and region, and call `load_dotenv()` at the top of the script.

---

### Class Definition  
```python
class AirlineReviewLoader:
    def __init__(self, airline_name: str, data_type: str = "raw"): …
    def ensure_bucket_exists(self, bucket_name: str, flag: int = 0): …
    def upload_df(self, bucket_name: str, object_key: str, df: pd.DataFrame): …
    def download_df(self, bucket_name: str, review_type: str) -> Optional[pd.DataFrame]: …
    def load_data(self, bucket_name: str, load_type: str, review_type: str = "all"): …
```

#### 1. Constructor

```python
def __init__(self, airline_name: str, data_type: str = "raw"):
    """
    Args:
      airline_name: Name of the airline (e.g. "British Airways").
      data_type:   "raw" or "tf" (transformed).
    
    Outcomes:
      - Normalizes airline_name (kept as-is for display).
      - Initializes placeholders:
          self.airline_reviews,
          self.seat_reviews,
          self.lounge_reviews = None
    """
```

#### 2. `ensure_bucket_exists(self, bucket_name, flag=0)`

```python
def ensure_bucket_exists(self, bucket_name: str, flag: int = 0):
    """
    Checks if `bucket_name` exists in S3.
    
    Args:
      bucket_name: the S3 bucket name.
      flag: 1 to create the bucket if missing; 0 to only check.
    
    Outcomes:
      - Prints existence status.
      - If not found and flag==1, creates the bucket in AWS_DEFAULT_REGION.
    """
```
This is the first step in the loading of data. It checks the existence of the bucket, and if not found (error 404), it will create bucket. 
Bucket creation is conditional - only if upload function is implemented (decided by flag=1), then the bucket will be created. 

#### 3. `upload_df(self, bucket_name, object_key, df)`

```python
def upload_df(self, bucket_name: str, object_key: str, df: pd.DataFrame):
    """
    Uploads a DataFrame as CSV to S3.
    
    Args:
      bucket_name: the S3 bucket.
      object_key:  path/key inside the bucket.
      df:          the DataFrame to upload.
    
    Outcomes:
      - Skips if df is None or empty.
      - Converts df to CSV in a StringIO buffer and calls s3.put_object().
      - Prints the S3 URL on success.
    """
```

This function is a standalone function simply used to upload a dataframe (`df`) to the path (`object key`) inside a S3 Bucket. 

#### 4. `download_df(self, bucket_name, review_type)`

```python
def download_df(self, bucket_name: str, review_type: str) -> Optional[pd.DataFrame]:
    """
    Downloads a CSV from S3 into a DataFrame.
    
    Args:
      bucket_name: the S3 bucket.
      review_type: one of "airline","seat","lounge".
    
    Returns:
      pd.DataFrame on success; None on error.
    
    Outcomes:
      - Prints the S3 URL on success.
      - Prints an error and returns None if download fails.
    """
```

This function is used to download a dataframe from an S3 bucket. It simply uses the `bucket name`, `review type` and `data type` to construct the object key, and downloads the required dataframe to a variable. 

#### 5. `load_data(self, bucket_name, load_type, review_type="all")`

```python
def load_data(self, bucket_name: str, load_type: str, review_type: str = "all"):
    """
    Top-level method to upload/download one or all review types.

    Args:
      bucket_name:  the S3 bucket.
      load_type:    "upload" or "download".
      review_type:  "all", "airline", "seat", or "lounge".
    
    Behavior:
      - Validates arguments.
      - On upload:
          - Calls ensure_bucket_exists(flag=1)
          - Iterates through each non-None DataFrame attribute,
            prints progress, and uploads via upload_df().
      - On download:
          - Calls ensure_bucket_exists(flag=0)
          - Iterates through requested types,
            downloads via download_df(), and assigns to attributes.
      - Prints summary on completion.
    """
```

This is the top-level function which will be used to trigger the uploading / downloading (using argument `load_type`) of data from S3. 

### Usage Example 

```python
from loader import AirlineReviewLoader

# 1. Prepare loader for raw data upload
loader = AirlineReviewLoader("British Airways", data_type="raw")
loader.airline_reviews = df_airline
loader.seat_reviews    = df_seat
loader.lounge_reviews  = df_lounge

# 2. Upload all raw reviews
loader.load_data(bucket_name="airline-reviews", load_type="upload", review_type="all")

# 3. Later, download back into memory
loader = AirlineReviewLoader("British Airways", data_type="raw")
loader.load_data(bucket_name="airline-reviews", load_type="download", review_type="all")
print(loader.lounge_reviews.head())
```