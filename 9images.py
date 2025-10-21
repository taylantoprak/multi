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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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


# Set up logging
log_filename = "luxurylog.log"
logging.basicConfig(
    filename=log_filename, 
    filemode='w',  # Overwrites the log file each time the script runs
    level=logging.INFO, 
    format="%(asctime)s:%(levelname)s:%(message)s"
)

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
    """Process downloads for a single vendor with optimized threading."""
    if len(qualified_df) == 0:
        return
    
    start_time = time.time()
    total_images = len(qualified_df) * 9  # Approximate total images
    
    print(f"Starting downloads for vendor: {vendor} ({len(qualified_df)} items, ~{total_images} images)", flush=True)
    logging.info(f"Starting downloads for vendor: {vendor} ({len(qualified_df)} items, ~{total_images} images)")
    
    total = len(qualified_df)
    completed = 0
    failed_downloads = []

    # Optimize thread count based on system capabilities
    max_workers = min(50, len(qualified_df) * 9)  # Up to 50 threads, or 9 per item (9 images max)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_download, row, vendor, base_directory, failed_downloads): index 
                  for index, row in qualified_df.iterrows()}

        for future in concurrent.futures.as_completed(futures):
            completed += 1
            progress = (completed / total) * 100
            sys.stdout.write(f"\rVendor {vendor} - Progress: {completed:>4}/{total:<4} | {progress:>6.2f}% completed")
            sys.stdout.flush()

    end_time = time.time()
    duration = end_time - start_time
    images_per_second = total_images / duration if duration > 0 else 0
    
    print(f"\nVendor {vendor} - Downloads completed ({completed}/{total} items) in {duration:.2f}s", flush=True)
    print(f"Vendor {vendor} - Performance: {images_per_second:.2f} images/second", flush=True)
    logging.info(f"Vendor {vendor} - Downloads completed ({completed}/{total} items) in {duration:.2f}s")
    logging.info(f"Vendor {vendor} - Performance: {images_per_second:.2f} images/second")
    
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

# Create optimized session with connection pooling
def create_optimized_session():
    """Create a session with optimized connection pooling and retry strategy."""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,  # Number of connection pools
        pool_maxsize=100,     # Maximum number of connections in the pool
        pool_block=False      # Don't block when pool is full
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Global session for reuse
download_session = create_optimized_session()

def download_file(url, folder, file_name, sequence, retries=3, delay=2):
    """Download a file with optimized retry logic and connection pooling."""
    global download_session
    
    for attempt in range(retries):
        try:
            # Use the optimized session with connection pooling
            response = download_session.get(
                url, 
                headers=imgheaders, 
                stream=True, 
                timeout=15,  # Reduced timeout for faster failure detection
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

                # Use the provided file_name with extension
                full_file_name = f"{file_name}.{file_extension}"
                file_path = os.path.join(folder, full_file_name)

                # Optimized file writing with larger chunks
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                        if chunk:  # Filter out keep-alive chunks
                            file.write(chunk)

                return True, None
            else:
                logging.warning(f"Attempt {attempt + 1}/{retries}: Failed to download {url}. Status code: {response.status_code}")
                if attempt < retries - 1:
                    time.sleep(delay * (attempt + 1))  # Exponential backoff
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{retries}: Error downloading {url}. Error: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))  # Exponential backoff

    # After retries failed
    logging.error(f"Max retries reached. Failed to download: {url}")
    return False, (file_name, url)


failed_urls_set = set()


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
        
        # Download images concurrently within each product
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(9, len(image_urls))) as img_executor:
            img_futures = []
            
            for sequence, img_url in enumerate(image_urls, 1):
                # Use Tag Name as prefix for file naming
                file_prefix = tag_name.replace(' ', '_').replace(',', '_')
                file_name = f"{file_prefix}_{good_id}_{sequence:02d}"
                
                # Submit download task
                future = img_executor.submit(download_file, img_url, folder_path, file_name, sequence)
                img_futures.append((future, img_url))
            
            # Wait for all images of this product to complete
            for future, img_url in img_futures:
                success, error = future.result()
                if not success and error:
                    handle_failed_download(img_url)


    
# Declare failed_downloads as a global variable
failed_downloads = []

def main():
    """Main function to process all vendors concurrently with immediate downloads.
    
    Download Behavior:
    - Downloads start IMMEDIATELY as each vendor's data processing completes
    - No waiting for all vendors to finish before starting downloads
    - Up to 10 vendors can process data + download images simultaneously
    - This creates an overlapping pipeline for maximum efficiency
    """
    print(f"Starting processing for {len(vendors)} vendors with {max_concurrent_vendors} concurrent operations", flush=True)
    print("Downloads will start IMMEDIATELY as each vendor's data processing completes", flush=True)
    logging.info(f"Starting processing for {len(vendors)} vendors with {max_concurrent_vendors} concurrent operations")
    
    # Process vendors with a maximum of 10 concurrent operations (data processing + downloads)
    max_concurrent_vendors = 10
    completed_data_processing = 0
    started_downloads = 0
    
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
                    # Start download process for this vendor immediately
                    # Downloads will run concurrently with other vendor processing
                    download_future = executor.submit(process_vendor_downloads, vendor, qualified_df, os.getcwd())
                    started_downloads += 1
                    print(f"Vendor {vendor} - Data processing completed ({completed_data_processing}/{len(vendors)}), downloads started immediately", flush=True)
                    logging.info(f"Vendor {vendor} - Data processing completed, downloads started immediately")
                else:
                    print(f"Vendor {vendor} - No qualified data to download ({completed_data_processing}/{len(vendors)})", flush=True)
                    logging.info(f"Vendor {vendor} - No qualified data to download")
            except Exception as e:
                print(f"Vendor {vendor} - Error processing data: {e} ({completed_data_processing}/{len(vendors)})", flush=True)
                logging.error(f"Vendor {vendor} - Error processing data: {e}")
    
    print(f"All vendor processing completed. Started downloads for {started_downloads} vendors.", flush=True)
    print("Note: Downloads run concurrently and may still be in progress.", flush=True)
    logging.info(f"All vendor processing completed. Started downloads for {started_downloads} vendors.")

# Run the main process
if __name__ == "__main__":
    main()