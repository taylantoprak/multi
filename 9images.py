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
import shutil
import asyncio
import aiohttp
import aiofiles
from tqdm import tqdm
import threading

# Array of vendors to process
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


# Set up logging with fallback to console if disk space issues
def setup_logging():
    log_filename = "luxurylog.log"
    try:
        logging.basicConfig(
            filename=log_filename, 
            filemode='w',  # Overwrites the log file each time the script runs
            level=logging.INFO, 
            format="%(asctime)s:%(levelname)s:%(message)s"
        )
    except OSError as e:
        if e.errno == 28:  # No space left on device
            print(f"Warning: Cannot create log file due to disk space. Logging to console only.")
            logging.basicConfig(
                level=logging.INFO, 
                format="%(asctime)s:%(levelname)s:%(message)s"
            )
        else:
            raise

setup_logging()

def check_disk_space(path, required_bytes=100*1024*1024):  # 100MB minimum
    """Check if there's enough disk space available."""
    try:
        statvfs = os.statvfs(path)
        free_bytes = statvfs.f_frsize * statvfs.f_bavail
        return free_bytes >= required_bytes, free_bytes
    except (OSError, AttributeError):
        # Fallback for Windows or if statvfs fails
        try:
            free_bytes = shutil.disk_usage(path).free
            return free_bytes >= required_bytes, free_bytes
        except OSError:
            return False, 0

def format_bytes(bytes_value):
    """Format bytes into human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

class ProgressTracker:
    """Thread-safe progress tracking for downloads."""
    def __init__(self, total_vendors, total_files=0):
        self.total_vendors = total_vendors
        self.total_files = total_files
        self.completed_vendors = 0
        self.completed_files = 0
        self.failed_files = 0
        self.lock = threading.Lock()
        self.vendor_progress = {}
        self.start_time = time.time()
        
    def update_vendor_progress(self, vendor, completed, total, failed=0):
        with self.lock:
            self.vendor_progress[vendor] = {
                'completed': completed,
                'total': total,
                'failed': failed,
                'percentage': (completed / total * 100) if total > 0 else 0
            }
    
    def update_file_progress(self, completed=1, failed=0):
        with self.lock:
            self.completed_files += completed
            self.failed_files += failed
    
    def complete_vendor(self, vendor):
        with self.lock:
            self.completed_vendors += 1
    
    def get_overall_progress(self):
        with self.lock:
            vendor_pct = (self.completed_vendors / self.total_vendors * 100) if self.total_vendors > 0 else 0
            file_pct = (self.completed_files / self.total_files * 100) if self.total_files > 0 else 0
            elapsed = time.time() - self.start_time
            return vendor_pct, file_pct, elapsed, self.completed_files, self.failed_files

# Global progress tracker
progress_tracker = None

def safe_log(message, level=logging.INFO):
    """Safely log a message, handling disk space errors."""
    try:
        if level == logging.INFO:
            logging.info(message)
        elif level == logging.WARNING:
            logging.warning(message)
        elif level == logging.ERROR:
            logging.error(message)
    except OSError as e:
        if e.errno == 28:  # No space left on device
            print(f"LOG ERROR (disk full): {message}")
        else:
            raise

logging.info("Script started")

start_date = input("Enter start date (YYYY-MM-DD): ")
end_date = input("Enter end date (YYYY-MM-DD): ")


# Initialize pagination
next_page = ''  

cookies = {
    'token': 'Mzk4MDk3Q0E5RTZCN0I1MkYwMTYwNDlCQUNFNkQ5QzVFOEZCOTI1OEEwOTA2MDc0QzUzRTVCNDVDMTg1RTgzRTZBNTY1MTZDQTNFNDFCRkI2ODZGRTgxRjQxRDU3MEZD',
}

# Base headers template - will be customized per vendor
base_headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

def request_data(vendor, start_date, end_date, next_page, retries=3, delay=15):
    """Fetch paginated data from API with retry logic."""
    attempt = 0

    while attempt < retries:
        try:
            # Create vendor-specific headers
            vendor_headers = base_headers.copy()
            vendor_headers['authority'] = f'{vendor}.wsxcme.com'
            vendor_headers['origin'] = f'https://{vendor}.wsxcme.com'
            
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

            response = requests.post(
                f'https://{vendor}.wsxcme.com/album/personal/all',
                params=params,
                cookies=cookies,
                headers=vendor_headers,
                timeout=30  # optional: to prevent hanging forever
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.warning(f"Request failed for vendor {vendor} (Attempt {attempt}/{retries}): {e}")
            print(f"Request failed for vendor {vendor} (Attempt {attempt}/{retries}): {e}", flush=True)

            if attempt < retries:
                print(f"Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)
            else:
                logging.error(f"Max retries reached for vendor {vendor}. Moving to next step.")
                return None


def check_pagination(pagination_data):
    """Determine if more pages exist based on the presence of a valid pageTimestamp"""
    next_page = pagination_data.get('pageTimestamp')  # Get next page token

    # Ensure next_page is valid (not None, not empty, and a valid number)
    if isinstance(next_page, (int, str)) and str(next_page).strip():
        print(f'Next page found: {str(next_page)}', flush=True)
        return str(next_page)  # Return valid next page token

    print('No more pages found', flush=True)
    return None



def extract_data(data):
    """Extract relevant data fields from the API response"""
    extracted_data = []

    if 'result' not in data or 'items' not in data['result']:
        return pd.DataFrame()

    for item in data['result']['items']:
        shop_name = item.get('shop_name', '')
        images = item.get('imgsSrc', [])
        # Get first 9 images instead of just the first one
        first_nine_images = images[:9] if images else []

        title = item.get('title', '')
        tags = item.get('tags', [])
        tag_name = ', '.join([tag['tagName'] for tag in tags if 'tagName' in tag])
        tag_id = ', '.join([str(tag['tagId']) for tag in tags if 'tagId' in tag])
        link = item.get('link', '')
        goods_id = item.get('goods_id', '')

        # === Create sanitized Image Name from first_image URL ===
        image_name = ''
        if first_nine_images:
            try:
                # Use the first image for naming
                first_image = first_nine_images[0]
                # Remove domain
                path_match = re.sub(r'^https?://[^/]+/', '', first_image)
                # Remove extension
                path_no_ext = re.sub(r'\.[^.]+$', '', path_match)
                # Replace slashes
                formatted = path_no_ext.replace('/', '_')
                # Final clean image name string - use Tag Name as prefix instead of Product
                raw_name = f'{{ {formatted} }}'
                # Sanitize: remove or replace characters invalid in Windows file names
                image_name = re.sub(r'[<>:"/\\|?*]', '_', raw_name)
            except Exception:
                random_number = random.randint(1000000, 9999999)
                image_name = f'Unknown_{random_number}'

        extracted_data.append({
            'Shop Name': shop_name,
            'Images': ', '.join(images),
            'No of images': len(images),
            'First Nine Images': ', '.join(first_nine_images),  # Store first 9 images
            'Image Name': image_name,  # ✅ Fully ready for file saving
            'Title': title,
            'Tag Name': tag_name,
            'Tag_ID': tag_id,
            'Link': link,
            'Goods_id': goods_id
        })

    return pd.DataFrame(extracted_data)


def process_vendor_data(vendor, start_date, end_date):
    """Process data for a single vendor with pagination."""
    print(f"Processing vendor: {vendor}", flush=True)
    logging.info(f"Processing vendor: {vendor}")
    
    All_data = pd.DataFrame()
    next_page = ''
    data = request_data(vendor, start_date, end_date, next_page)
    totall = 0
    
    if data:
        pag_data = data.get('result', {}).get('pagination', {})
        next_page = check_pagination(pag_data)

    while next_page:
        # Ensure the API request was successful before processing
        if data:
            extracted_data = extract_data(data)
            totall = totall + len(extracted_data)
            print(f"Vendor {vendor} - Results added: {len(extracted_data)}, TOTAL: {totall}", flush=True)
            All_data = pd.concat([All_data, extracted_data], ignore_index=True)

            pag_data = data.get('result', {}).get('pagination', {})  # Safely retrieve pagination data
            next_page = check_pagination(pag_data)  # Get the next page token

            if next_page is None:
                break  # Stop pagination if no next page

            # Fetch the next page
            data = request_data(vendor, start_date, end_date, next_page)
        else:
            break  # Stop loop if API request fails

    # Filter Data - Updated to check for First Nine Images instead of First Image
    if len(All_data) > 0:
        qualified_df = All_data[
            (All_data['First Nine Images'].notna()) &  # Ensure First Nine Images is not empty
            (All_data['First Nine Images'] != '') &  # Ensure First Nine Images is not an empty string
            (All_data['No of images'] > 4)  # Ensure No of images is greater than 4
        ]
        
        print(f"Vendor {vendor} - QUALIFIED RESULTS AFTER FILTER: {len(qualified_df)}", flush=True)
        return qualified_df
    else:
        print(f"Vendor {vendor} - No data to process.", flush=True)
        return pd.DataFrame()

def process_vendor_downloads(vendor, qualified_df, base_directory):
    """Process downloads for a single vendor."""
    if len(qualified_df) == 0:
        return
    
    print(f"Starting downloads for vendor: {vendor} ({len(qualified_df)} items)", flush=True)
    logging.info(f"Starting downloads for vendor: {vendor} ({len(qualified_df)} items)")
    
    total = len(qualified_df)
    completed = 0
    failed_downloads = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_download, row, vendor, base_directory, failed_downloads): index 
                  for index, row in qualified_df.iterrows()}

        for future in concurrent.futures.as_completed(futures):
            completed += 1
            progress = (completed / total) * 100
            sys.stdout.write(f"\rVendor {vendor} - Progress: {completed:>4}/{total:<4} | {progress:>6.2f}% completed")
            sys.stdout.flush()

    print(f"\nVendor {vendor} - Downloads completed ({completed}/{total} items)", flush=True)
    logging.info(f"Vendor {vendor} - Downloads completed ({completed}/{total} items)")
    
    if failed_downloads:
        failed_df = pd.DataFrame(failed_downloads, columns=["Tag", "Image Name", "URL"])
        failed_filename = f"failed_downloads_{vendor}.csv"
        failed_df.to_csv(failed_filename, index=False)
        print(f"Vendor {vendor} - {len(failed_downloads)} failed downloads saved to '{failed_filename}'", flush=True)
        logging.warning(f"Vendor {vendor} - {len(failed_downloads)} failed downloads saved to '{failed_filename}'")

imgheaders = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

async def async_download_file(session, url, folder, file_name, sequence, vendor, pbar=None, retries=4, delay=15):
    """Async download a file with retry logic, disk space checking, and progress tracking."""
    attempt = 0
    
    while attempt < retries:
        try:
            # Check disk space before attempting download
            has_space, free_bytes = check_disk_space(folder, 50*1024*1024)  # 50MB minimum
            if not has_space:
                error_msg = f"Insufficient disk space. Available: {format_bytes(free_bytes)}"
                safe_log(f"Disk space error for {url}: {error_msg}", logging.ERROR)
                if pbar:
                    pbar.set_description(f"❌ {vendor} - Disk space error")
                return False, error_msg
            
            async with session.get(url, headers=imgheaders, timeout=30) as response:
                if response.status == 200:
                    # Get content length for additional space check
                    content_length = response.headers.get('Content-Length')
                    file_size = int(content_length) if content_length else 0
                    
                    if file_size > 0:
                        has_space, free_bytes = check_disk_space(folder, file_size + 10*1024*1024)  # 10MB buffer
                        if not has_space:
                            error_msg = f"Insufficient disk space for file. Required: {format_bytes(file_size)}, Available: {format_bytes(free_bytes)}"
                            safe_log(f"Disk space error for {url}: {error_msg}", logging.ERROR)
                            if pbar:
                                pbar.set_description(f"❌ {vendor} - Disk space error")
                            return False, error_msg
                    
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
                    
                    full_file_name = f"{file_name}.{file_extension}"
                    file_path = os.path.join(folder, full_file_name)
                    
                    # Download with streaming and progress tracking
                    downloaded_size = 0
                    async with aiofiles.open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            try:
                                await f.write(chunk)
                                downloaded_size += len(chunk)
                                
                                # Update progress bar if available
                                if pbar and file_size > 0:
                                    progress_pct = (downloaded_size / file_size) * 100
                                    pbar.set_description(f"📥 {vendor} - Downloading {file_name[:30]}... {progress_pct:.1f}%")
                                
                            except OSError as e:
                                if e.errno == 28:  # No space left on device
                                    # Clean up partial file
                                    try:
                                        os.remove(file_path)
                                    except OSError:
                                        pass
                                    error_msg = f"Disk space error during download: {e}"
                                    safe_log(f"Disk space error for {url}: {error_msg}", logging.ERROR)
                                    if pbar:
                                        pbar.set_description(f"❌ {vendor} - Disk space error")
                                    return False, error_msg
                                else:
                                    raise
                    
                    # Update progress tracker
                    if progress_tracker:
                        progress_tracker.update_file_progress(completed=1)
                    
                    safe_log(f"File downloaded successfully: {url}", logging.INFO)
                    return True, None
                else:
                    safe_log(f"Attempt {attempt + 1}/{retries}: Failed to download {url}. Status code: {response.status}", logging.WARNING)
                    attempt += 1
                    if attempt < retries:
                        if pbar:
                            pbar.set_description(f"🔄 {vendor} - Retrying {file_name[:30]}... (Attempt {attempt})")
                        await asyncio.sleep(delay)
        except asyncio.TimeoutError:
            attempt += 1
            safe_log(f"Attempt {attempt}/{retries}: Timeout downloading {url}", logging.WARNING)
            if attempt < retries:
                if pbar:
                    pbar.set_description(f"⏰ {vendor} - Timeout, retrying {file_name[:30]}...")
                await asyncio.sleep(delay)
        except OSError as e:
            if e.errno == 28:  # No space left on device
                error_msg = f"Disk space error: {e}"
                safe_log(f"Disk space error for {url}: {error_msg}", logging.ERROR)
                if pbar:
                    pbar.set_description(f"❌ {vendor} - Disk space error")
                return False, error_msg
            else:
                attempt += 1
                safe_log(f"Attempt {attempt}/{retries}: Error downloading {url}. Error: {e}", logging.ERROR)
                if attempt < retries:
                    if pbar:
                        pbar.set_description(f"🔄 {vendor} - Error, retrying {file_name[:30]}...")
                    await asyncio.sleep(delay)
        except Exception as e:
            attempt += 1
            safe_log(f"Attempt {attempt}/{retries}: Error downloading {url}. Error: {e}", logging.ERROR)
            if attempt < retries:
                if pbar:
                    pbar.set_description(f"🔄 {vendor} - Error, retrying {file_name[:30]}...")
                await asyncio.sleep(delay)
    
    # After retries failed
    safe_log(f"Max retries reached. Failed to download: {url}", logging.ERROR)
    if pbar:
        pbar.set_description(f"❌ {vendor} - Failed {file_name[:30]} after {retries} attempts")
    return False, f"Max retries reached for {url}"

def download_file(url, folder, file_name, sequence, retries=4, delay=15):
    """Download a file with retry logic."""
    attempt = 0

    while attempt < retries:
        try:
            response = requests.get(url, headers=imgheaders, stream=True, timeout=30)  # add timeout to avoid hanging
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

                # Use the provided file_name with extension
                full_file_name = f"{file_name}.{file_extension}"
                file_path = os.path.join(folder, full_file_name)

                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)

                logging.info(f"File downloaded successfully: {url}")
                return True, None
            else:
                logging.warning(f"Attempt {attempt + 1}/{retries}: Failed to download {url}. Status code: {response.status_code}")
                attempt += 1
                if attempt < retries:
                    time.sleep(delay)
        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt}/{retries}: Error downloading {url}. Error: {e}")
            if attempt < retries:
                time.sleep(delay)

    # After retries failed
    logging.error(f"Max retries reached. Failed to download: {url}")
    return False, (good_id, url)


failed_urls_set = set()

async def _worker(session, url, folder, file_name, sequence, failed_downloads, tag_name, good_id, vendor, pbar=None):
    """Worker function for async downloads."""
    success, error = await async_download_file(session, url, folder, file_name, sequence, vendor, pbar)
    if not success and error:
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([tag_name, good_id, url])
        if progress_tracker:
            progress_tracker.update_file_progress(failed=1)
    return success

async def process_vendor_downloads_async(vendor, qualified_df, base_directory):
    """Process downloads for a single vendor using async operations with visual progress."""
    if len(qualified_df) == 0:
        return
    
    # Count total files to download
    total_files = 0
    for index, row in qualified_df.iterrows():
        first_nine_images_str = str(row.get('First Nine Images', '')).strip()
        if first_nine_images_str:
            image_urls = [url.strip() for url in first_nine_images_str.split(',') if url.strip()]
            total_files += len(image_urls)
    
    if total_files == 0:
        print(f"Vendor {vendor} - No files to download", flush=True)
        return
    
    print(f"\n🚀 Starting downloads for vendor: {vendor}")
    print(f"📊 Items: {len(qualified_df)}, Files: {total_files}")
    safe_log(f"Starting async downloads for vendor: {vendor} ({len(qualified_df)} items, {total_files} files)", logging.INFO)
    
    completed = 0
    failed_downloads = []
    
    # Create session with connection limits
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=60)
    
    # Create progress bar for this vendor
    with tqdm(total=total_files, desc=f"📥 {vendor}", unit="file", 
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} files [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            
            for index, row in qualified_df.iterrows():
                good_id = str(row.get('Image Name', '')).strip()
                first_nine_images_str = str(row.get('First Nine Images', '')).strip()
                tag_name = str(row.get('Tag Name', '')).strip()

                if not tag_name:
                    tag_name = "Unknown_Tag"

                # Create a vendor folder if it doesn't exist
                vendor_folder = os.path.join(base_directory, vendor)
                os.makedirs(vendor_folder, exist_ok=True)

                # Create a subfolder with the tag name inside the vendor folder
                folder_path = os.path.join(vendor_folder, tag_name)
                os.makedirs(folder_path, exist_ok=True)

                # Parse the comma-separated image URLs and create download tasks
                if first_nine_images_str:
                    image_urls = [url.strip() for url in first_nine_images_str.split(',') if url.strip()]
                    
                    for sequence, img_url in enumerate(image_urls, 1):
                        # Use Tag Name as prefix for file naming
                        file_prefix = tag_name.replace(' ', '_').replace(',', '_')
                        file_name = f"{file_prefix}_{good_id}_{sequence:02d}"
                        
                        task = _worker(session, img_url, folder_path, file_name, sequence, 
                                     failed_downloads, tag_name, good_id, vendor, pbar)
                        tasks.append(task)
            
            # Execute all download tasks with progress tracking
            if tasks:
                # Process tasks in batches to show progress
                batch_size = 10
                for i in range(0, len(tasks), batch_size):
                    batch = tasks[i:i + batch_size]
                    results = await asyncio.gather(*batch, return_exceptions=True)
                    
                    # Update progress bar
                    batch_completed = len([r for r in results if r is True])
                    pbar.update(batch_completed)
                    completed += batch_completed
                    
                    # Update vendor progress in global tracker
                    if progress_tracker:
                        progress_tracker.update_vendor_progress(vendor, completed, total_files, len(failed_downloads))
    
    # Mark vendor as completed
    if progress_tracker:
        progress_tracker.complete_vendor(vendor)
    
    # Print completion summary
    success_rate = (completed / total_files * 100) if total_files > 0 else 0
    print(f"\n✅ Vendor {vendor} completed!")
    print(f"📈 Success: {completed}/{total_files} files ({success_rate:.1f}%)")
    if failed_downloads:
        print(f"❌ Failed: {len(failed_downloads)} files")
    
    safe_log(f"Vendor {vendor} - Downloads completed ({completed}/{total_files} files, {success_rate:.1f}% success)", logging.INFO)
    
    if failed_downloads:
        failed_df = pd.DataFrame(failed_downloads, columns=["Tag", "Image Name", "URL"])
        failed_filename = f"failed_downloads_{vendor}.csv"
        try:
            failed_df.to_csv(failed_filename, index=False)
            print(f"💾 Failed downloads saved to '{failed_filename}'")
            safe_log(f"Vendor {vendor} - {len(failed_downloads)} failed downloads saved to '{failed_filename}'", logging.WARNING)
        except OSError as e:
            if e.errno == 28:  # No space left on device
                print(f"⚠️  Cannot save failed downloads list due to disk space")
            else:
                raise

def process_download(row, vendor, base_directory, failed_downloads):
    global failed_urls_set

    good_id = str(row.get('Image Name', '')).strip()
    first_nine_images_str = str(row.get('First Nine Images', '')).strip()
    tag_name = str(row.get('Tag Name', '')).strip()

    if not tag_name:
        tag_name = "Unknown_Tag"

    # Create a vendor folder if it doesn't exist
    vendor_folder = os.path.join(base_directory, vendor)
    os.makedirs(vendor_folder, exist_ok=True)

    # Create a subfolder with the tag name inside the vendor folder
    folder_path = os.path.join(vendor_folder, tag_name)
    os.makedirs(folder_path, exist_ok=True)

    def handle_failed_download(url):
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([tag_name, good_id, url])

    # Parse the comma-separated image URLs and download each one
    if first_nine_images_str:
        image_urls = [url.strip() for url in first_nine_images_str.split(',') if url.strip()]
        
        for sequence, img_url in enumerate(image_urls, 1):
            # Use Tag Name as prefix for file naming
            file_prefix = tag_name.replace(' ', '_').replace(',', '_')
            file_name = f"{file_prefix}_{good_id}_{sequence:02d}"
            success, error = download_file(img_url, folder_path, file_name, sequence)
            if not success and error:
                handle_failed_download(img_url)


    
# Declare failed_downloads as a global variable
failed_downloads = []

def main():
    """Main function to process all vendors concurrently with immediate downloads and visual progress."""
    global progress_tracker
    
    print("=" * 80)
    print("🎯 LUXURY IMAGE DOWNLOADER - ENHANCED VERSION")
    print("=" * 80)
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🏪 Total Vendors: {len(vendors)}")
    print("=" * 80)
    
    safe_log(f"Starting processing for {len(vendors)} vendors", logging.INFO)
    
    # Check initial disk space
    has_space, free_bytes = check_disk_space(os.getcwd(), 500*1024*1024)  # 500MB minimum
    if not has_space:
        print(f"⚠️  Warning: Low disk space detected. Available: {format_bytes(free_bytes)}")
        safe_log(f"Warning: Low disk space detected. Available: {format_bytes(free_bytes)}", logging.WARNING)
    else:
        print(f"💾 Disk Space: {format_bytes(free_bytes)} available")
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(len(vendors))
    
    # Process vendors with a maximum of 3 concurrent operations (reduced to save disk space)
    max_concurrent_vendors = 3
    completed_data_processing = 0
    started_downloads = 0
    
    print(f"\n🚀 Starting data processing and downloads...")
    print(f"⚙️  Concurrent vendors: {max_concurrent_vendors}")
    print("-" * 80)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_vendors) as executor:
        # Submit all vendor processing tasks
        future_to_vendor = {}
        
        for vendor in vendors:
            # First, process data for the vendor
            data_future = executor.submit(process_vendor_data, vendor, start_date, end_date)
            future_to_vendor[data_future] = vendor
        
        # Process completed data futures and start downloads immediately
        for data_future in concurrent.futures.as_completed(future_to_vendor):
            vendor = future_to_vendor[data_future]
            completed_data_processing += 1
            
            try:
                qualified_df = data_future.result()
                if len(qualified_df) > 0:
                    # Count total files for this vendor
                    total_files = 0
                    for index, row in qualified_df.iterrows():
                        first_nine_images_str = str(row.get('First Nine Images', '')).strip()
                        if first_nine_images_str:
                            image_urls = [url.strip() for url in first_nine_images_str.split(',') if url.strip()]
                            total_files += len(image_urls)
                    
                    # Update global progress tracker
                    progress_tracker.total_files += total_files
                    
                    # Start async download process for this vendor immediately
                    download_future = executor.submit(
                        lambda: asyncio.run(process_vendor_downloads_async(vendor, qualified_df, os.getcwd()))
                    )
                    started_downloads += 1
                    
                    print(f"✅ Vendor {vendor} - Data processed ({completed_data_processing}/{len(vendors)}) | Files: {total_files} | Downloads started")
                    safe_log(f"Vendor {vendor} - Data processing completed, async downloads started immediately", logging.INFO)
                else:
                    print(f"⏭️  Vendor {vendor} - No qualified data ({completed_data_processing}/{len(vendors)})")
                    safe_log(f"Vendor {vendor} - No qualified data to download", logging.INFO)
            except Exception as e:
                print(f"❌ Vendor {vendor} - Error: {e} ({completed_data_processing}/{len(vendors)})")
                safe_log(f"Vendor {vendor} - Error processing data: {e}", logging.ERROR)
    
    print("-" * 80)
    print(f"🎉 All vendor processing completed!")
    print(f"📊 Started downloads for {started_downloads} vendors")
    print(f"📁 Total files to download: {progress_tracker.total_files}")
    print("=" * 80)
    
    safe_log(f"All vendor processing completed. Started async downloads for {started_downloads} vendors.", logging.INFO)

def monitor_progress():
    """Monitor and display overall progress in a separate thread."""
    while True:
        if progress_tracker:
            vendor_pct, file_pct, elapsed, completed_files, failed_files = progress_tracker.get_overall_progress()
            
            # Only print if there's progress to show
            if completed_files > 0 or failed_files > 0:
                elapsed_str = f"{int(elapsed//60):02d}:{int(elapsed%60):02d}"
                print(f"\r📊 Overall Progress: Vendors {progress_tracker.completed_vendors}/{progress_tracker.total_vendors} ({vendor_pct:.1f}%) | "
                      f"Files {completed_files}/{progress_tracker.total_files} ({file_pct:.1f}%) | "
                      f"Failed: {failed_files} | Time: {elapsed_str}", end="", flush=True)
            
            # Stop monitoring if all vendors are completed
            if progress_tracker.completed_vendors >= progress_tracker.total_vendors:
                break
        
        time.sleep(2)  # Update every 2 seconds

# Run the main process
if __name__ == "__main__":
    # Start progress monitoring in a separate thread
    monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
    monitor_thread.start()
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user")
        print("🛑 Stopping downloads...")
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        safe_log(f"Unexpected error in main: {e}", logging.ERROR)
    finally:
        print("\n" + "=" * 80)
        print("🏁 Process completed!")
        if progress_tracker:
            vendor_pct, file_pct, elapsed, completed_files, failed_files = progress_tracker.get_overall_progress()
            print(f"📈 Final Stats:")
            print(f"   • Vendors: {progress_tracker.completed_vendors}/{progress_tracker.total_vendors}")
            print(f"   • Files: {completed_files}/{progress_tracker.total_files}")
            print(f"   • Failed: {failed_files}")
            print(f"   • Time: {int(elapsed//60):02d}:{int(elapsed%60):02d}")
        print("=" * 80)