import sys
import os
import schedule
import time
import psycopg2
from ScrapServiceSell import ScrapServiceSell
from sarouty import SaroutyScraper
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_sql_script():
    logging.info("Starting to run SQL script.")
    connection = None
    cursor = None
    
    # Connect to the PostgreSQL database
    try:
        logging.info("Attempting to connect to PostgreSQL database...")
        connection = psycopg2.connect(
            dbname='railway',
            user='postgres',
            password='oJebrMLiLkUPXtWnFHnXylOjmvUfOCoO',
            host='autorack.proxy.rlwy.net',
            port='47171'
        )
        logging.info("Successfully connected to database")
        cursor = connection.cursor()
        
        # Load and execute the SQL script
        logging.info("Reading SQL script from output.sql...")
        with open('output.sql', 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()
            logging.info("SQL script loaded, executing statements...")
            cursor.execute(sql_script)
        
        # Commit the transaction
        connection.commit()
        logging.info("Transaction committed successfully")
        
    except psycopg2.Error as pe:
        logging.error(f"PostgreSQL error: {pe.pgerror}")
        logging.error(f"PostgreSQL error details: {pe.diag.message_detail if hasattr(pe, 'diag') else 'No details'}")
        raise
    except Exception as e:
        logging.error(f"Error executing SQL script: {str(e)}")
        raise
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
            logging.info("Database cursor closed")
        if connection:
            connection.close()
            logging.info("Database connection closed")

def job():
    try:
        logging.info("=== Starting new batch job ===")
        all_results = []
        
        # Run ScrapServiceSell for Yakeey
        logging.info("Initializing Yakeey scraper...")
        service_sell_scraper = ScrapServiceSell(max_threads=5)
        
        logging.info("Starting ScrapServiceSell pipeline")
        service_sell_results = service_sell_scraper.run_pipeline(start_page=0, end_page=69)
        logging.info(f"ScrapServiceSell completed. Found {len(service_sell_results)} properties")
        all_results.extend(service_sell_results)

        # Save and convert Yakeey results
        with open('property_details.json', 'w', encoding='utf-8') as f:
            json.dump(service_sell_results.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
        from sql_converte import main as sql_converter_main
        sql_converter_main()
        run_sql_script()

        # Run Sarouty scraper
        logging.info("Initializing Sarouty scraper...")
        sarouty_scraper = SaroutyScraper(max_threads=10)
        
        logging.info("Starting Sarouty scrape")
        sarouty_results = sarouty_scraper.continuous_scrape()
        logging.info(f"Sarouty scraping completed. Found {len(sarouty_results)} properties")
        all_results.extend(sarouty_results)

        # Save and convert Sarouty results
        with open('property_details.json', 'w', encoding='utf-8') as f:
            json.dump(sarouty_results, f, ensure_ascii=False, indent=2)       
        sql_converter_main()
        run_sql_script()

        # Run Mubawab scraper
        import subprocess
        logging.info("Running Mubawab script...")
        subprocess.run(["python", "mubawab.py"], check=True)
        sql_converter_main()
        run_sql_script()

        logging.info("=== Batch job completed successfully ===")
        
    except Exception as e:
        logging.error(f"=== Batch job failed: {str(e)} ===")
        raise

if __name__ == "__main__":  
    schedule.every().day.at("23:00").do(job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)