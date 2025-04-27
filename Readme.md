# Project – Airline Reviews Analysis

## Overview  
This project scrapes customer reviews from Skytrax, transforms and loads them into AWS S3, and ultimately drives a dashboard to analyze passenger feedback. It uses asynchronous requests to maximize scraping throughput and pandas/NumPy for data cleanup.

---

## Objectives 🎯  
1. **Extract** customer reviews dynamically (airline, seat, lounge)  
2. **Clean & transform** the raw HTML into structured tabular data  
3. **Analyze** sentiment, keywords, and rating trends (TBD)  
4. **Visualize** the results in Power BI / Looker / Tableau (TBD)  

---

## Tech Stack & Tools 🛠️  
- **Web Scraping**: `aiohttp`, `asyncio`, `beautifulsoup4`  
- **Data Handling**: `pandas`, `numpy`  
- **Storage**: AWS S3 (`boto3`, `python-dotenv`)  
- **CI/CD & Orchestration**: GitHub Actions, AWS MWAA (Airflow) (TBI)
- **Visualization**: Power BI / Looker / Tableau (TBD)  

---

## Current Status 🚧  
- ✅ Scraper module: extracts and saves raw review tables  
- ✅ Loader module: uploads raw CSVs to S3; downloads on demand  
- 🛠 Transformer module: **in progress**  
- 🛠 End-to-end CI/CD + Airflow DAG: **TBI**  

---

## Next Steps  
- Prepare the transformer module. 
- install all modules as packages using a `setup.py` file. 
- prepare driver scripts in a new `/tests/` folder. 

Once the above 3 are completed, we will be finished with the __Pre-Containerization Phase__. 