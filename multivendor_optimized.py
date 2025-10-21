import requests
import pandas as pd
import os
from datetime import datetime
import logging
import concurrent.futures
import sys
import time
import re
import random
import threading
import json
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import asyncio
import aiohttp
import aiofiles
from tqdm import tqdm
from PIL import Image
import io

# Multiple vendors support
vendors = [
    "a201806061906228670097971",
    "a2018010822273327434",
    "a202012251445571550594728",
    "a201912310143044600286360",
    "a201903281615367860166324",
    "a2018010917270834895",
    "a202404192105238602001635",
    "a202010082358318990477763",
    "a2017121816575808682",
    "a2017122201154502554",
    "a202303052030280760001283",
    "a202405241124306572002876",
    "a201810301621302480105169",
    "a201906061800426220167776",
    "a2018030112493216496",
    "a201807231024280280104591",
    "a201806261508593630084027",
    "a2018010921111724454",
    "a201807310651298060035905",
    "a202309161341391112001129",
    "a2018013117585943485",
    "a201809141336200740005671",
    "a202010181841365740501047",
    "a2018012313160631913",
    "a202208160010571420002358",
    "a201809201925131710080380",
    "a201904091340459270103789",
    "a2017122211111803941",
    "a201803261104473820060189",
    "a202004241505323890308814",
    "a2017111600500601108",
    "a2017122901271001291",
    "a201904262333275900273151",
    "a202212022302595300001050",
    "a202111041317341460003592",
    "a202208311337481812001750",
    "a202204191531060680001858",
    "a202205251054266580001214",
    "a202404011453507702002184",
    "a2018011715130921107",
    "a202011091451361850169891",
    "a2018031411484582569",
    "a201805211032471830093003",
    "a201901181007378610192491",
    "a20191124133520015283",
    "a202110250207269200001206",
    "a202111171412160780001434",
    "a202206241243285730001841",
    "a201807070433331780118304",
    "a202412090831475372002524",
    "a201805282008372260117875",
    "a202011060101061760466111",
    "a202004241438341790100492",
    "a2018031222423305204",
    "a2018010714510836614",
    "a202410091518392572001903",
    "a202205211243151160001703",
    "a202309141041074272001055",
    "a201803251204381530007463",
    "a201909021721021650175306",
    "a202212261106241172001706",
    "a201804041725121110062917",
    "a2017122212202502741"
]

# Set up logging
log_filename = "luxurylog.log"
logging.basicConfig(
    filename=log_filename,
    filemode='w',
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
)

logging.info("Script started")

start_date = input("Enter start date (YYYY-MM-DD): ")
end_date = input("Enter end date (YYYY-MM-DD): ")

cookies = {
    'token': 'Mzk4MDk3Q0E5RTZCN0I1MkYwMTYwNDlCQUNFNkQ5QzVFOEZCOTI1OEEwOTA2MDc0QzUzRTVCNDVDMTg1RTgzRTZBNTY1MTZDQTNFNDFCRkI2ODZGRTgxRjQxRDU3MEZD',
}

# Global optimized session for connection pooling
download_session = None

def create_optimized_session():
    """Create optimized session with connection pooling and retry strategy."""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    
    # Configure HTTP adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=100,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def sanitize_filename(filename):
    """Sanitize filename to remove invalid characters."""
    # Remove or replace invalid characters for file paths
    invalid_chars = '<>:"/\\|?*{}[]()'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove multiple consecutive underscores
    while '__' in filename:
        filename = filename.replace('__', '_')
    
    # Remove leading/trailing underscores and dots
    filename = filename.strip('_.')
    
    # Ensure filename is not empty
    if not filename:
        filename = 'unnamed'
    
    # Limit length to avoid filesystem issues
    if len(filename) > 200:
        filename = filename[:200]
    
    return filename

def compress_image_sync(file_path, quality=85, max_size=(1920, 1080)):
    """Compress image synchronously using PIL."""
    try:
        with Image.open(file_path) as img:
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            # Resize if too large
            if img.size[0] > max_size[0] or img.size[1] > max_size[1]:
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
            
            # Save with compression
            img.save(file_path, 'JPEG', quality=quality, optimize=True)
    except Exception as e:
        logging.warning(f"Failed to compress image {file_path}: {e}")

async def compress_image_async(file_path, quality=85, max_size=(1920, 1080)):
    """Compress image asynchronously using PIL."""
    try:
        with Image.open(file_path) as img:
            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            # Resize if too large
            if img.size[0] > max_size[0] or img.size[1] > max_size[1]:
                img.thumbnail(max_size, Image.Resampling.LANCZOS)
            
            # Save with compression
            img.save(file_path, 'JPEG', quality=quality, optimize=True)
    except Exception as e:
        logging.warning(f"Failed to compress image {file_path}: {e}")

# Progress tracking
progress_tracker = {
    'vendor_progress': defaultdict(lambda: {
        'status': 'Pending',
        'total_products': 0,
        'completed_products': 0,
        'failed_products': 0,
        'start_time': None,
        'end_time': None
    }),
    'overall': {
        'total_vendors': len(vendors),
        'completed_vendors': 0,
        'failed_vendors': 0,
        'start_time': time.time()
    }
}

def progress_monitor():
    """Monitor and display progress for all vendors."""
    while True:
        try:
            # Clear screen
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print("🚀 OPTIMIZED MULTIVENDOR DOWNLOADER - REAL-TIME PROGRESS")
            print("=" * 80)
            
            # Overall progress
            overall = progress_tracker['overall']
            elapsed = time.time() - overall['start_time']
            print(f"📊 OVERALL PROGRESS: {overall['completed_vendors']}/{overall['total_vendors']} vendors completed")
            print(f"⏱️  ELAPSED TIME: {elapsed:.1f}s")
            print(f"📈 SUCCESS RATE: {(overall['completed_vendors']/(overall['completed_vendors']+overall['failed_vendors'])*100):.1f}%" if (overall['completed_vendors']+overall['failed_vendors']) > 0 else "0%")
            print()
            
            # Individual vendor progress
            print("📋 VENDOR STATUS:")
            for vendor, progress in progress_tracker['vendor_progress'].items():
                if progress['status'] != 'Pending':
                    status_icon = "✅" if progress['status'] == 'Completed' else "❌" if progress['status'] == 'Failed' else "🔄"
                    print(f"  {status_icon} {vendor[:20]}... - {progress['status']} - {progress['completed_products']}/{progress['total_products']} products")
            
            print("\n" + "=" * 80)
            print("Press Ctrl+C to stop monitoring (downloads will continue)")
            
            time.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Progress monitor error: {e}")
            time.sleep(5)

def get_headers(vendor: str):
    return {
        'authority': f'{vendor}.wsxcme.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': f'https://{vendor}.wsxcme.com',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

def request_data(vendor: str, start_date, end_date, next_page, retries=3, delay=5):
    """Fetch paginated data from API with optimized retry logic."""
    global download_session
    
    if download_session is None:
        download_session = create_optimized_session()
    
    attempt = 0
    while attempt < retries:
        try:
            params = {
                'albumId': vendor,
                'searchValue': '',
                'searchImg': '',
                'startDate': start_date,
                'endDate': end_date,
                'sourceId': '',
                'slipType': '1',
                'timestamp': next_page,
                'requestDataType': '',
                'transLang': 'en'
            }

            response = download_session.post(
                f'https://{vendor}.wsxcme.com/album/personal/all',
                params=params,
                cookies=cookies,
                headers=get_headers(vendor),
                timeout=15  # Reduced timeout for faster failure detection
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.warning(f"Request failed for vendor {vendor} (Attempt {attempt}/{retries}): {e}")
            
            if attempt < retries:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                time.sleep(wait_time)
            else:
                logging.error(f"Max retries reached for vendor {vendor}. Moving to next step.")
                return None

def check_pagination(pagination_data):
    """Determine if more pages exist based on the presence of a valid pageTimestamp"""
    next_page = pagination_data.get('pageTimestamp')
    if isinstance(next_page, (int, str)) and str(next_page).strip():
        return str(next_page)
    return None

def extract_data(data):
    """Extract relevant data fields from the API response"""
    extracted_data = []
    if 'result' not in data or 'items' not in data['result']:
        return pd.DataFrame()
    for item in data['result']['items']:
        shop_name = item.get('shop_name', '')
        images = item.get('imgsSrc', [])
        first_image = images[0] if images else ''
        title = item.get('title', '')
        tags = item.get('tags', [])
        tag_name = ', '.join([tag['tagName'] for tag in tags if 'tagName' in tag])
        tag_id = ', '.join([str(tag['tagId']) for tag in tags if 'tagId' in tag])
        link = item.get('link', '')
        goods_id = item.get('goods_id', '')
        image_name = ''
        if first_image:
            try:
                path_match = re.sub(r'^https?://[^/]+/', '', first_image)
                path_no_ext = re.sub(r'\.[^.]+$', '', path_match)
                formatted = path_no_ext.replace('/', '_')
                raw_name = f'Product {{ {formatted} }}'
                image_name = re.sub(r'[<>:"/\\|?*]', '_', raw_name)
            except Exception:
                random_number = random.randint(1000000, 9999999)
                image_name = f'Product_{random_number}'
        extracted_data.append({
            'Shop Name': shop_name,
            'Images': ', '.join(images),
            'No of images': len(images),
            'First Image': first_image,
            'Image Name': image_name,
            'Title': title,
            'Tag Name': tag_name,
            'Tag_ID': tag_id,
            'Link': link,
            'Goods_id': goods_id
        })
    return pd.DataFrame(extracted_data)

imgheaders = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

def download_file(url, folder, good_id, sequence, retries=3, delay=2):
    """Download a file with optimized retry logic, connection pooling, and compression."""
    global download_session
    
    if download_session is None:
        download_session = create_optimized_session()
    
    for attempt in range(retries):
        try:
            response = download_session.get(
                url, 
                headers=imgheaders, 
                stream=True, 
                timeout=15,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                # Determine the file extension from Content-Type
                content_type = response.headers.get('Content-Type', '')
                if 'image/jpeg' in content_type:
                    file_extension = 'jpg'
                elif 'image/png' in content_type:
                    file_extension = 'png'
                elif 'image/gif' in content_type:
                    file_extension = 'gif'
                else:
                    file_extension = 'jpg'  # Default fallback

                # Sanitize the file name
                sanitized_good_id = sanitize_filename(good_id)
                file_name = f"{sanitized_good_id}_{sequence:02d}.{file_extension}"
                file_path = os.path.join(folder, file_name)
                
                # Ensure the directory exists
                os.makedirs(folder, exist_ok=True)

                # Optimized file writing with larger chunks
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                        if chunk:  # Filter out keep-alive chunks
                            file.write(chunk)

                # Compress the downloaded image
                compress_image_sync(file_path, quality=85, max_size=(1920, 1080))

                return True, None
            else:
                logging.warning(f"Attempt {attempt + 1}/{retries}: Failed to download {url}. Status code: {response.status_code}")
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{retries}: Error downloading {url}. Error: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff

    return False, (good_id, url)

async def async_download_file(session, url, folder, good_id, sequence, quality=85):
    """Async download with compression using aiohttp."""
    try:
        async with session.get(url, headers=imgheaders, timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status == 200:
                # Determine file extension
                content_type = response.headers.get('Content-Type', '')
                if 'image/jpeg' in content_type:
                    file_extension = 'jpg'
                elif 'image/png' in content_type:
                    file_extension = 'png'
                elif 'image/gif' in content_type:
                    file_extension = 'gif'
                else:
                    file_extension = 'jpg'

                # Sanitize the file name
                sanitized_good_id = sanitize_filename(good_id)
                file_name = f"{sanitized_good_id}_{sequence:02d}.{file_extension}"
                file_path = os.path.join(folder, file_name)
                
                # Ensure the directory exists
                os.makedirs(folder, exist_ok=True)

                # Download and save
                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(65536):
                        await f.write(chunk)

                # Compress the image
                await compress_image_async(file_path, quality)
                
                return True, None
            else:
                return False, (good_id, url)
    except Exception as e:
        logging.error(f"Async download error for {url}: {e}")
        return False, (good_id, url)

failed_urls_set = set()
failed_downloads = []

def process_download(row, base_directory, failed_downloads, vendor):
    global failed_urls_set
    good_id = str(row.get('Image Name', '')).strip()
    first_img_url = str(row.get('First Image', '')).strip()
    tag_name = str(row.get('Tag Name', '')).strip()
    
    if not tag_name:
        tag_name = "Unknown_Tag"
    
    # Sanitize tag name for folder path
    sanitized_tag_name = sanitize_filename(tag_name)
    
    vendor_folder = os.path.join(base_directory, vendor)
    os.makedirs(vendor_folder, exist_ok=True)
    folder_path = os.path.join(vendor_folder, sanitized_tag_name)
    os.makedirs(folder_path, exist_ok=True)
    
    def handle_failed_download(url):
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([vendor, tag_name, good_id, url])
    
    if first_img_url:
        success, error = download_file(first_img_url, folder_path, good_id, 1)
        if not success and error:
            handle_failed_download(first_img_url)

async def process_download_async(session, row, base_directory, failed_downloads, vendor):
    """Async version of process_download."""
    global failed_urls_set
    good_id = str(row.get('Image Name', '')).strip()
    first_img_url = str(row.get('First Image', '')).strip()
    tag_name = str(row.get('Tag Name', '')).strip()
    
    if not tag_name:
        tag_name = "Unknown_Tag"
    
    # Sanitize tag name for folder path
    sanitized_tag_name = sanitize_filename(tag_name)
    
    vendor_folder = os.path.join(base_directory, vendor)
    os.makedirs(vendor_folder, exist_ok=True)
    folder_path = os.path.join(vendor_folder, sanitized_tag_name)
    os.makedirs(folder_path, exist_ok=True)
    
    def handle_failed_download(url):
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([vendor, tag_name, good_id, url])
    
    if first_img_url:
        success, error = await async_download_file(session, first_img_url, folder_path, good_id, 1)
        if not success and error:
            handle_failed_download(first_img_url)

def download_with_multithreading(qualified_df, directory, vendor, use_async=False):
    """Download with optimized multithreading or async processing."""
    total = len(qualified_df)
    progress_tracker['vendor_progress'][vendor]['total_products'] = total
    progress_tracker['vendor_progress'][vendor]['start_time'] = time.time()
    progress_tracker['vendor_progress'][vendor]['status'] = 'Downloading'
    
    print(f"Starting download process for {total} products from vendor {vendor}...")
    
    if use_async:
        # Async processing
        async def async_download_batch():
            async with aiohttp.ClientSession() as session:
                tasks = []
                for index, row in qualified_df.iterrows():
                    task = process_download_async(session, row, directory, failed_downloads, vendor)
                    tasks.append(task)
                
                # Process in batches to avoid overwhelming the server
                batch_size = 50
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    await asyncio.gather(*batch, return_exceptions=True)
                    
                    # Update progress
                    completed = min(i + batch_size, len(tasks))
                    progress_tracker['vendor_progress'][vendor]['completed_products'] = completed
                    
                    if completed >= total:
                        progress_tracker['vendor_progress'][vendor]['status'] = 'Completed Async'
                        progress_tracker['vendor_progress'][vendor]['end_time'] = time.time()
                        break
        
        asyncio.run(async_download_batch())
    else:
        # Sync processing with optimized threading
        max_workers = min(50, total)  # Up to 50 threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_download, row, directory, failed_downloads, vendor): index 
                for index, row in qualified_df.iterrows()
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                completed += 1
                progress_tracker['vendor_progress'][vendor]['completed_products'] = completed
                
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing download for vendor {vendor}: {e}")
                    progress_tracker['vendor_progress'][vendor]['failed_products'] += 1
        
        progress_tracker['vendor_progress'][vendor]['status'] = 'Completed'
        progress_tracker['vendor_progress'][vendor]['end_time'] = time.time()

def process_vendor(vendor, start_date, end_date, use_async=False):
    """Process a single vendor with optimized data fetching and downloading."""
    print(f"\n{'='*60}")
    print(f"Processing vendor: {vendor}")
    print(f"{'='*60}")
    
    progress_tracker['vendor_progress'][vendor]['status'] = 'Processing Data'
    
    next_page = ''
    All_data = pd.DataFrame()
    data = request_data(vendor, start_date, end_date, next_page)
    totall = 0
    
    if data:
        pag_data = data.get('result', {}).get('pagination', {})
        next_page = check_pagination(pag_data)
    
    while next_page:
        if data:
            extracted_data = extract_data(data)
            totall = totall + len(extracted_data)
            print(f"Number of results added: {len(extracted_data)}, TOTAL: {totall}", flush=True)
            All_data = pd.concat([All_data, extracted_data], ignore_index=True)
            pag_data = data.get('result', {}).get('pagination', {})
            next_page = check_pagination(pag_data)
            if next_page is None:
                break
            data = request_data(vendor, start_date, end_date, next_page)
        else:
            break
    
    if len(All_data) == 0:
        print(f"No data to process for vendor {vendor}.", flush=True)
        logging.info(f"No data to process for vendor {vendor}. Skipping.")
        progress_tracker['vendor_progress'][vendor]['status'] = 'No Data'
        progress_tracker['overall']['failed_vendors'] += 1
        return
    
    vendor_csv = f'Luxurybrand_all_data_{vendor}.csv'
    All_data.to_csv(vendor_csv, encoding='utf-8-sig', index=False)
    print(f"TOTAL RESULTS BEFORE FILTER for {vendor}: {len(All_data)}", flush=True)
    
    qualified_df = All_data[
        (All_data['First Image'].notna()) &
        (All_data['First Image'] != '') &
        (All_data['No of images'] == 1)
    ]
    print(f"QUALIFIED RESULTS AFTER FILTER for {vendor}: {len(qualified_df)}", flush=True)
    
    qualified_csv = f'Luxurybrand_qualified_data_{vendor}.csv'
    qualified_df.to_csv(qualified_csv, encoding='utf-8-sig', index=False)
    
    if len(qualified_df) > 0:
        download_with_multithreading(qualified_df, os.getcwd(), vendor, use_async)
        progress_tracker['overall']['completed_vendors'] += 1
    else:
        print(f"No qualified results for vendor {vendor}. Skipping downloads.")
        progress_tracker['vendor_progress'][vendor]['status'] = 'No Qualified Data'
        progress_tracker['overall']['failed_vendors'] += 1

def main():
    """Main function with concurrent vendor processing and performance optimizations."""
    print(f"🚀 OPTIMIZED MULTIVENDOR DOWNLOADER")
    print(f"Processing {len(vendors)} vendors...")
    print(f"Date range: {start_date} to {end_date}")
    print(f"{'='*80}")
    
    # Configuration
    max_concurrent_vendors = 10  # Process up to 10 vendors concurrently
    use_async_downloads = True   # Set to False for sync processing
    
    print(f"⚙️  CONFIGURATION:")
    print(f"   • Concurrent vendors: {max_concurrent_vendors}")
    print(f"   • Download method: {'Async (faster)' if use_async_downloads else 'Sync (stable)'}")
    print(f"   • Connection pooling: Enabled")
    print(f"   • Image compression: Enabled (85% quality, max 1920x1080)")
    print(f"   • Progress tracking: Real-time")
    print(f"{'='*80}")
    
    # Start progress monitoring thread
    progress_thread = threading.Thread(target=progress_monitor, daemon=True)
    progress_thread.start()
    
    # Process vendors concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_vendors) as executor:
        # Submit all vendor processing tasks
        future_to_vendor = {}
        
        for vendor in vendors:
            future = executor.submit(process_vendor, vendor, start_date, end_date, use_async_downloads)
            future_to_vendor[future] = vendor
        
        # Wait for all vendors to complete
        for future in concurrent.futures.as_completed(future_to_vendor):
            vendor = future_to_vendor[future]
            try:
                future.result()
                print(f"✅ Successfully processed vendor {vendor}")
            except Exception as e:
                print(f"❌ Error processing vendor {vendor}: {e}")
                logging.error(f"Error processing vendor {vendor}: {e}")
                progress_tracker['overall']['failed_vendors'] += 1
    
    # Final summary
    overall = progress_tracker['overall']
    total_time = time.time() - overall['start_time']
    
    print(f"\n{'='*80}")
    print(f"🎉 PROCESSING COMPLETE!")
    print(f"{'='*80}")
    print(f"📊 FINAL SUMMARY:")
    print(f"   • Total vendors: {len(vendors)}")
    print(f"   • Successfully processed: {overall['completed_vendors']}")
    print(f"   • Failed: {overall['failed_vendors']}")
    print(f"   • Failed downloads: {len(failed_downloads)}")
    print(f"   • Total time: {total_time:.1f} seconds")
    print(f"   • Average time per vendor: {total_time/len(vendors):.1f} seconds")
    print(f"   • Success rate: {(overall['completed_vendors']/len(vendors)*100):.1f}%")
    
    if failed_downloads:
        failed_csv = "failed_downloads.csv"
        pd.DataFrame(failed_downloads, columns=["Vendor", "Tag", "Image Name", "URL"]).to_csv(failed_csv, index=False)
        print(f"\n⚠️  {len(failed_downloads)} failed downloads saved to '{failed_csv}'")
    else:
        print(f"\n✅ No failed downloads!")
    
    print(f"{'='*80}")
    logging.info(f"Script completed successfully. Processed {overall['completed_vendors']}/{len(vendors)} vendors in {total_time:.1f}s")

if __name__ == "__main__":
    main()