import asyncio
import aiohttp
import json
import time
import math
import re
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import os
from datetime import datetime, timedelta
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Set, Optional, Any
import logging
from collections import deque
from urllib.parse import urljoin
import random
from tqdm import tqdm
import warnings
import asyncio.exceptions
import aiofiles
import signal
import sys
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.mubawab.ma"
CACHE_DIR = os.path.join("temp_cache")
CACHE_EXPIRY = timedelta(hours=24)
MAX_CONCURRENT_REQUESTS = 10  # Increased from 5
MIN_DELAY = 0.5  # Reduced from 1
MAX_DELAY = 2  # Reduced from 3
TIMEOUT = 30
BATCH_SIZE = 20  # Increased from 10
CHECKPOINT_INTERVAL = 5  # Save checkpoint every 5 pages
MAX_RETRIES = 5  # Increased from 3
RESULTS_PER_PAGE = 33
CHECKPOINT_FILE = "scraper_checkpoint.json"
OUTPUT_FILE = "property_details.json"

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

warnings.filterwarnings("ignore", category=FutureWarning)

class MubawabScraper:
    # Class-level attribute for cache status
    cache_enabled = False
    
    def __init__(self):
        self.session = None
        self.cache = {}
        self.processed_urls: Set[str] = set()
        self.rate_limiter = deque(maxlen=10)
        self.results = []
        self.existing_data = {}
        self.running = True
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.last_checkpoint = 0
        self.checkpoint_data = {}
        
        # Check for caching dependencies
        try:
            from aiohttp_client_cache import CachedSession, SQLiteBackend
            self.CachedSession = CachedSession
            self.SQLiteBackend = SQLiteBackend
            self.__class__.cache_enabled = True
        except ImportError:
            logger.warning("Cache dependencies not found. Running without cache.")
            self.__class__.cache_enabled = False
        
        # Create cache directory if it doesn't exist
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

    async def init_session(self):
        """Initialize the HTTP session with or without caching"""
        if self.session is not None:
            return

        if self.cache_enabled:
            try:
                cache = self.SQLiteBackend(
                    cache_name=os.path.join(CACHE_DIR, 'mubawab_cache'),
                    expire_after=CACHE_EXPIRY
                )
                self.session = self.CachedSession(
                    cache=cache,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT)
                )
                logger.info("Initialized cached session")
            except Exception as e:
                logger.error(f"Failed to initialize cache: {e}")
                self.cache_enabled = False
                
        if not self.cache_enabled:
            # Fallback to regular session if cache is not available
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=aiohttp.ClientTimeout(total=TIMEOUT)
            )
            logger.info("Initialized regular session")

    async def close(self):
        """Close the session properly"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_with_retry(self, url: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
        """Make HTTP GET request with retry logic and adaptive rate limiting"""
        for attempt in range(max_retries):
            try:
                # Implement adaptive rate limiting
                now = time.time()
                if self.rate_limiter and now - self.rate_limiter[0] < MIN_DELAY:
                    await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                
                async with self.semaphore:  # Use semaphore for concurrency control
                    async with self.session.get(url) as response:
                        self.rate_limiter.append(time.time())
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:  # Too Many Requests
                            wait_time = int(response.headers.get('Retry-After', 60))
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"HTTP {response.status} for {url}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                                continue
                            return None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                if attempt == max_retries - 1:
                    return None
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return None

    async def get_total_pages(self) -> int:
        """Get total number of pages to scrape"""
        url = f"{BASE_URL}/fr/sc/appartements-a-vendre:o:n"
        html = await self.get_with_retry(url)
        if not html:
            logger.error("Failed to get total pages, defaulting to 1")
            return 1
            
        soup = BeautifulSoup(html, 'lxml')  # Using lxml for better performance
        
        num_results_tag = soup.select_one("span#numResults")
        if num_results_tag:
            total_results = int(re.search(r"(\d+)", num_results_tag.text).group(1))
            return math.ceil(total_results / RESULTS_PER_PAGE)
        return 1

    async def scrape_listing_page(self, page_num: int) -> List[Dict]:
        """Scrape a single listing page"""
        url = f"{BASE_URL}/fr/sc/appartements-a-vendre:o:n:p:{page_num}"
        html = await self.get_with_retry(url)
        if not html:
            logger.error(f"Failed to fetch page {page_num}")
            return []
            
        soup = BeautifulSoup(html, 'lxml')
        results = []
        
        # Use a more efficient selector
        listings = soup.select("div.listingBox")
        logger.info(f"Found {len(listings)} listings on page {page_num}")
        
        for listing in listings:
            try:
                # First try to get link from linkref attribute
                link = listing.get('linkref')
                if not link:
                    # Then try to find the first anchor tag with href
                    link_tag = listing.find("a", href=True)
                    if not link_tag:
                        continue
                    link = link_tag.get('href')
                    if not link:
                        continue
                
                listing_data = self.parse_listing(listing)
                if listing_data:
                    results.append(listing_data)
            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                continue
        
        return results

    def parse_listing(self, listing) -> Optional[Dict]:
        """Parse individual listing data"""
        try:
            # Get link more efficiently
            link_tag = listing.find("a", href=True)
            if not link_tag:
                return None
                
            link = link_tag.get('href')
            if not link:
                return None
                
            link = urljoin(BASE_URL, link)
            
            url_hash = hashlib.sha256(link.encode()).hexdigest()[:16]
            
            # Skip if already processed
            if url_hash in self.existing_data:
                return None
                
            title_tag = listing.select_one("h2.listingTit")
            if not title_tag:
                return None
                
            title = title_tag.text.strip()
            
            location = listing.select_one("span.listingH3")
            city, quartier = self.parse_location(location.text if location else "")
            
            return {
                "id": url_hash,
                "link": link,
                "title": title,
                "city": city,
                "quartier": quartier,
                "createdAt": datetime.now().isoformat(),
                "updatedAt": datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f"Error in parse_listing: {str(e)}")
            return None

    async def scrape_detail_page(self, listing_data: Dict) -> Optional[Dict]:
        """Scrape detailed property information"""
        if listing_data['id'] in self.existing_data:
            return self.existing_data[listing_data['id']]
            
        html = await self.get_with_retry(listing_data['link'])
        if not html:
            logger.error(f"Failed to fetch details for {listing_data['link']}")
            return None
            
        soup = BeautifulSoup(html, 'lxml')
        
        details = {
            **listing_data,
            "description": self.get_description(soup),
            "price": self.get_price(soup),
            "surface": self.get_surface(soup),
            "bedrooms": self.get_bedrooms(soup),
            "bathrooms": self.get_bathrooms(soup),
            "features": self.get_features(soup),
            "images": self.get_images(soup),
            "condition": self.get_condition(soup),
            "status": "unknown",
            "country": "Maroc",
            "site": "mubawab",
            "sell": True,
            "longTerm": False,
            "propertyTypeName": "Appartement",
            "modifiedAt": datetime.now().isoformat()
        }
        
        return details

    async def save_batch(self, batch: List[Dict]):
        """Save a batch of results to file"""
        if not batch:
            return
            
        if not os.path.exists('data'):
            os.makedirs('data')
            
        # Load existing data
        existing_data = []
        if os.path.exists(OUTPUT_FILE):
            try:
                async with aiofiles.open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    existing_data = json.loads(content)
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                
        # Update with new data
        existing_ids = {item['id'] for item in existing_data}
        new_items = [item for item in batch if item['id'] not in existing_ids]
        existing_data.extend(new_items)
        
        # Save updated data
        try:
            async with aiofiles.open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(existing_data, ensure_ascii=False, indent=4))
            logger.info(f"Saved {len(new_items)} new items")
        except Exception as e:
            logger.error(f"Error saving batch: {e}")

    async def save_checkpoint(self, current_page: int):
        """Save checkpoint data to resume scraping later"""
        if not os.path.exists('data'):
            os.makedirs('data')
            
        checkpoint_data = {
            "last_page": current_page,
            "timestamp": datetime.now().isoformat(),
            "processed_ids": list(self.existing_data.keys())
        }
        
        try:
            async with aiofiles.open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(checkpoint_data, ensure_ascii=False, indent=4))
            logger.info(f"Saved checkpoint at page {current_page}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    async def load_checkpoint(self) -> int:
        """Load checkpoint data to resume scraping from last position"""
        if not os.path.exists(CHECKPOINT_FILE):
            return 0
            
        try:
            async with aiofiles.open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                content = await f.read()
                checkpoint_data = json.loads(content)
                
            last_page = checkpoint_data.get("last_page", 0)
            processed_ids = checkpoint_data.get("processed_ids", [])
            
            # Update existing_data with processed IDs
            for item_id in processed_ids:
                self.existing_data[item_id] = True
                
            logger.info(f"Loaded checkpoint from page {last_page}")
            return last_page
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return 0

    async def process_page(self, page_num: int, pbar) -> List[Dict]:
        """Process a single page and its listings"""
        results = []
        listings = await self.scrape_listing_page(page_num)
        
        # Process listings concurrently
        tasks = []
        for listing in listings:
            if listing:
                tasks.append(self.scrape_detail_page(listing))
                
        if tasks:
            detail_results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(detail_results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing listing: {result}")
                    continue
                if result:
                    results.append(result)
                    pbar.update(1)
                    
        return results

    async def run(self):
        """Main scraping routine"""
        try:
            await self.init_session()
            
            # Load existing data
            if os.path.exists(OUTPUT_FILE):
                try:
                    async with aiofiles.open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        data = json.loads(content)
                        self.existing_data = {item['id']: item for item in data}
                        logger.info(f"Loaded {len(self.existing_data)} existing items")
                except Exception as e:
                    logger.error(f"Error loading existing data: {e}")
            
            # Load checkpoint
            start_page = await self.load_checkpoint()
            
            total_pages = await self.get_total_pages()
            logger.info(f"Found {total_pages} pages to scrape, starting from page {start_page + 1}")
            
            # Create progress bar
            with tqdm(total=total_pages - start_page, desc="Scraping pages") as pbar:
                batch = []
                
                # Process pages concurrently in chunks
                chunk_size = 5  # Process 5 pages at a time
                for chunk_start in range(start_page, total_pages, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_pages)
                    
                    # Create tasks for each page in the chunk
                    tasks = []
                    for page in range(chunk_start + 1, chunk_end + 1):
                        tasks.append(self.process_page(page, pbar))
                        
                    # Wait for all tasks in the chunk to complete
                    chunk_results = await asyncio.gather(*tasks)
                    
                    # Flatten results and add to batch
                    for page_results in chunk_results:
                        batch.extend(page_results)
                        
                    # Save batch if it's large enough
                    if len(batch) >= BATCH_SIZE:
                        await self.save_batch(batch)
                        batch = []
                        
                    # Save checkpoint periodically
                    if (chunk_end % CHECKPOINT_INTERVAL == 0) or (chunk_end == total_pages):
                        await self.save_checkpoint(chunk_end)
                        
            # Save final batch
            if batch:
                await self.save_batch(batch)
                
        except asyncio.CancelledError:
            logger.info("Scraping cancelled, saving checkpoint...")
            # Try to save checkpoint before exiting
            try:
                await self.save_checkpoint(self.last_checkpoint)
            except:
                pass
            raise
        finally:
            await self.close()

    def parse_location(self, location_text: str) -> tuple:
        """Parse location text into city and quartier"""
        if not location_text:
            return None, None
            
        # Remove any icons
        location_text = re.sub(r'<[^>]+>', '', location_text)
        
        # Split by comma if present
        parts = location_text.split(',')
        if len(parts) >= 2:
            quartier = parts[0].strip()
            city = parts[1].strip()
        else:
            quartier = None
            city = location_text.strip()
            
        return city, quartier

    def get_description(self, soup) -> str:
        """Extract property description"""
        desc_tag = soup.select_one("div.blockProp p")
        return desc_tag.text.strip() if desc_tag else None

    def get_price(self, soup) -> Any:
        """Extract property price"""
        price_tag = soup.select_one("h3.orangeTit")
        if not price_tag:
            return None
            
        price_text = price_tag.text.strip()
        if "consult" in price_text.lower():
            return "Prix à consulter"
            
        # Extract digits only
        digits = ''.join(filter(str.isdigit, price_text))
        return int(digits) if digits else None

    def get_surface(self, soup) -> Optional[float]:
        """Extract property surface area"""
        surface_tag = soup.select_one("div.adDetailFeature span:-soup-contains('m²')")
        if not surface_tag:
            return None
            
        match = re.search(r'(\d+)', surface_tag.text)
        return float(match.group(1)) if match else None

    def get_bedrooms(self, soup) -> Optional[int]:
        """Extract number of bedrooms"""
        # Look for the div with icon-bed class which contains the bedroom count
        bedroom_div = soup.select_one("div.adDetailFeature:has(i.icon-bed)")
        if not bedroom_div:
            return 0
            
        bedroom_span = bedroom_div.select_one("span")
        if not bedroom_span:
            return 0
            
        bedroom_text = bedroom_span.text.strip()
        match = re.search(r'(\d+)\s*Chambres?', bedroom_text)
        return int(match.group(1)) if match else 0

    def get_bathrooms(self, soup) -> Optional[int]:
        """Extract number of bathrooms"""
        # Look for the div with icon-bath class which contains the bathroom count
        bathroom_div = soup.select_one("div.adDetailFeature:has(i.icon-bath)")
        if not bathroom_div:
            return 0
            
        bathroom_span = bathroom_div.select_one("span")
        if not bathroom_span:
            return 0
            
        bathroom_text = bathroom_span.text.strip()
        match = re.search(r'(\d+)\s*Salle[s]?\s*de\s*bain', bathroom_text)
        return int(match.group(1)) if match else 0

    def get_features(self, soup) -> List[str]:
        """Extract property features"""
        features = []
        feature_tags = soup.select("div.adFeature span")
        for tag in feature_tags:
            feature = tag.text.strip()
            if feature:
                features.append(feature)
        return features

    def get_images(self, soup) -> List[str]:
        """Extract property images"""
        images = []
        for img in soup.select("div#masonryPhoto img[src]"):
            src = img.get('src')
            if src:
                images.append(src)
        return images

    def get_condition(self, soup) -> Optional[str]:
        """Extract property condition"""
        condition_tag = soup.select_one("div.adMainFeatureContent:-soup-contains('Etat') p.adMainFeatureContentValue")
        return condition_tag.text.strip() if condition_tag else None

    def cleanup_cache(self):
        """Clean up old cache files"""
        if os.path.exists(CACHE_DIR):
            try:
                # Remove cache files older than CACHE_EXPIRY
                now = datetime.now()
                for file in os.listdir(CACHE_DIR):
                    file_path = os.path.join(CACHE_DIR, file)
                    if os.path.isfile(file_path):
                        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if now - file_time > CACHE_EXPIRY:
                            os.remove(file_path)
                            logger.info(f"Removed old cache file: {file}")
            except Exception as e:
                logger.error(f"Error cleaning cache: {e}")

async def get_total_pages() -> int:
    """Get total number of pages to scrape"""
    async with aiohttp.ClientSession() as session:
        url = f"{BASE_URL}/fr/sc/appartements-a-vendre:o:n"
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'lxml')
                    
                    num_results_tag = soup.select_one("span#numResults")
                    if num_results_tag:
                        total_results = int(re.search(r"(\d+)", num_results_tag.text).group(1))
                        return math.ceil(total_results / RESULTS_PER_PAGE)
        except Exception as e:
            logger.error(f"Error getting total pages: {e}")
    return 1

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    logger.info("Interrupt received, cleaning up...")
    sys.exit(0)

async def main():
    """Main async entry point"""
    scraper = MubawabScraper()
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
    finally:
        await scraper.close()
        scraper.cleanup_cache()

if __name__ == "__main__":
    start_time = time.time()
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Scraping completed in {elapsed/60:.2f} minutes")