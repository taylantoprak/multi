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
from queue import Queue

# Multiple vendors array - you can add as many as you like
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

cookies = {
    'token': 'Mzk4MDk3Q0E5RTZCN0I1MkYwMTYwNDlCQUNFNkQ5QzVFOEZCOTI1OEEwOTA2MDc0QzUzRTVCNDVDMTg1RTgzRTZBNTY1MTZDQTNFNDFCRkI2ODZGRTgxRjQxRDU3MEZD',
}

def get_headers(vendor_id):
    """Generate headers for a specific vendor."""
    return {
        'authority': f'{vendor_id}.wsxcme.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': f'https://{vendor_id}.wsxcme.com',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

def request_data(start_date, end_date, next_page, vendor_id, retries=3, delay=15):
    """Fetch paginated data from API with retry logic."""
    attempt = 0

    while attempt < retries:
        try:
            params = {
                'albumId': vendor_id,
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
                f'https://{vendor_id}.wsxcme.com/album/personal/all',
                params=params,
                cookies=cookies,
                headers=get_headers(vendor_id),
                timeout=30  # optional: to prevent hanging forever
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            attempt += 1
            logging.warning(f"Request failed (Attempt {attempt}/{retries}): {e}")
            print(f"Request failed (Attempt {attempt}/{retries}): {e}", flush=True)

            if attempt < retries:
                print(f"Retrying in {delay} seconds...", flush=True)
                time.sleep(delay)
            else:
                logging.error("Max retries reached. Moving to next step.")
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


def extract_data(data, vendor_id):
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

        # === Create sanitized Image Name from first_image URL ===
        image_name = ''
        if first_image:
            try:
                # Remove domain
                path_match = re.sub(r'^https?://[^/]+/', '', first_image)
                # Remove extension
                path_no_ext = re.sub(r'\.[^.]+$', '', path_match)
                # Replace slashes
                formatted = path_no_ext.replace('/', '_')
                # Final clean image name string - use only tag name as prefix
                raw_name = f'{formatted}'
                # Sanitize: remove or replace characters invalid in Windows file names
                image_name = re.sub(r'[<>:"/\\|?*]', '_', raw_name)
            except Exception:
                random_number = random.randint(1000000, 9999999)
                image_name = f'image_{random_number}'

        extracted_data.append({
            'Vendor_ID': vendor_id,
            'Shop Name': shop_name,
            'Images': ', '.join(images),
            'No of images': len(images),
            'First Image': first_image,
            'Image Name': image_name,  # ✅ Fully ready for file saving
            'Title': title,
            'Tag Name': tag_name,
            'Tag_ID': tag_id,
            'Link': link,
            'Goods_id': goods_id
        })

    return pd.DataFrame(extracted_data)


def process_vendor_data(vendor_id, start_date, end_date):
    """Process data for a single vendor with pagination."""
    print(f"Processing vendor: {vendor_id}", flush=True)
    logging.info(f"Processing vendor: {vendor_id}")
    
    # Initialize pagination
    next_page = ''
    all_data = pd.DataFrame()
    total = 0
    
    data = request_data(start_date, end_date, next_page, vendor_id)
    if data:
        pag_data = data.get('result', {}).get('pagination', {})
        next_page = check_pagination(pag_data)

    while next_page:
        # Ensure the API request was successful before processing
        if data:
            extracted_data = extract_data(data, vendor_id)
            total += len(extracted_data)
            print(f"Vendor {vendor_id} - Results added: {len(extracted_data)}, TOTAL: {total}", flush=True)
            all_data = pd.concat([all_data, extracted_data], ignore_index=True)

            pag_data = data.get('result', {}).get('pagination', {})  # Safely retrieve pagination data
            next_page = check_pagination(pag_data)  # Get the next page token

            if next_page is None:
                break  # Stop pagination if no next page

            # Fetch the next page
            data = request_data(start_date, end_date, next_page, vendor_id)
        else:
            break  # Stop loop if API request fails
    
    print(f"Vendor {vendor_id} completed. Total results: {len(all_data)}", flush=True)
    logging.info(f"Vendor {vendor_id} completed. Total results: {len(all_data)}")
    return all_data


def download_file(url, folder, good_id, sequence, retries=4, delay=15):
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

                file_name = f"{good_id}.{file_extension}"
                file_path = os.path.join(folder, file_name)

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


imgheaders = {
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

failed_urls_set = set()

def process_download(row, base_directory, failed_downloads):
    global failed_urls_set

    good_id = str(row.get('Image Name', '')).strip()
    first_img_url = str(row.get('First Image', '')).strip()
    tag_name = str(row.get('Tag Name', '')).strip()
    vendor_id = str(row.get('Vendor_ID', '')).strip()

    if not tag_name:
        tag_name = "Unknown_Tag"

    # Create a vendor folder if it doesn't exist
    vendor_folder = os.path.join(base_directory, vendor_id)
    os.makedirs(vendor_folder, exist_ok=True)

    # Create a subfolder with the tag name inside the vendor folder
    folder_path = os.path.join(vendor_folder, tag_name)
    os.makedirs(folder_path, exist_ok=True)

    def handle_failed_download(url):
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([vendor_id, tag_name, good_id, url])

    if first_img_url:
        success, error = download_file(first_img_url, folder_path, good_id, 1)
        if not success and error:
            handle_failed_download(first_img_url)


def download_with_multithreading(qualified_df, directory):
    """Download images with multithreading for a specific vendor's data."""
    failed_downloads = []
    total = len(qualified_df)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_download, row, directory, failed_downloads): index for index, row in qualified_df.iterrows()}

        for future in concurrent.futures.as_completed(futures):
            completed += 1
            progress = (completed / total) * 100
            sys.stdout.write(f"\rProgress: {completed:>4}/{total:<4} | {progress:>6.2f}% completed")
            sys.stdout.flush()

    print()

    if failed_downloads:
        pd.DataFrame(failed_downloads, columns=["Vendor_ID", "Tag", "Image Name", "URL"]).to_csv("failed_downloads.csv", index=False)
        print(f"{len(failed_downloads)} failed downloads saved to 'failed_downloads.csv'", flush=True)
    
    return failed_downloads


def process_vendor_with_downloads(vendor_id, start_date, end_date, base_directory):
    """Process a single vendor: fetch data, filter, and download images."""
    print(f"\n=== Starting processing for vendor: {vendor_id} ===", flush=True)
    
    # Step 1: Fetch all data for this vendor
    all_data = process_vendor_data(vendor_id, start_date, end_date)
    
    if len(all_data) == 0:
        print(f"No data found for vendor {vendor_id}", flush=True)
        return pd.DataFrame(), []
    
    # Step 2: Filter data (only first image, no need for >4 images filter since we only download first image)
    qualified_df = all_data[
        (all_data['First Image'].notna()) &  # Ensure First Image is not empty
        (all_data['First Image'] != '')  # Ensure First Image is not an empty string
    ]
    
    print(f"Vendor {vendor_id} - QUALIFIED RESULTS: {len(qualified_df)}", flush=True)
    
    if len(qualified_df) == 0:
        print(f"No qualified data for vendor {vendor_id}", flush=True)
        return all_data, []
    
    # Step 3: Download images
    print(f"Starting downloads for vendor {vendor_id}...", flush=True)
    failed_downloads = download_with_multithreading(qualified_df, base_directory)
    
    print(f"=== Completed processing for vendor: {vendor_id} ===", flush=True)
    return all_data, failed_downloads


def main():
    """Main function to orchestrate the multi-vendor processing."""
    print(f"Starting processing for {len(vendors)} vendors", flush=True)
    print(f"Vendors: {vendors}", flush=True)
    
    # Process first 5 vendors simultaneously
    max_concurrent = min(5, len(vendors))
    remaining_vendors = vendors[max_concurrent:]
    
    print(f"Starting {max_concurrent} vendors simultaneously...", flush=True)
    
    all_vendor_data = []
    all_failed_downloads = []
    
    # Process first batch concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        future_to_vendor = {
            executor.submit(process_vendor_with_downloads, vendor_id, start_date, end_date, os.getcwd()): vendor_id 
            for vendor_id in vendors[:max_concurrent]
        }
        
        for future in concurrent.futures.as_completed(future_to_vendor):
            vendor_id = future_to_vendor[future]
            try:
                vendor_data, failed_downloads = future.result()
                all_vendor_data.append(vendor_data)
                all_failed_downloads.extend(failed_downloads)
                print(f"Vendor {vendor_id} completed successfully", flush=True)
            except Exception as exc:
                print(f"Vendor {vendor_id} generated an exception: {exc}", flush=True)
                logging.error(f"Vendor {vendor_id} generated an exception: {exc}")
    
    # Process remaining vendors sequentially as downloads finish
    for vendor_id in remaining_vendors:
        print(f"Starting sequential processing for vendor: {vendor_id}", flush=True)
        try:
            vendor_data, failed_downloads = process_vendor_with_downloads(vendor_id, start_date, end_date, os.getcwd())
            all_vendor_data.append(vendor_data)
            all_failed_downloads.extend(failed_downloads)
        except Exception as exc:
            print(f"Vendor {vendor_id} generated an exception: {exc}", flush=True)
            logging.error(f"Vendor {vendor_id} generated an exception: {exc}")
    
    # Combine all data
    if all_vendor_data:
        combined_data = pd.concat(all_vendor_data, ignore_index=True)
        combined_data.to_csv('Luxurybrand_all_data.csv', encoding='utf-8-sig', index=False)
        print(f"TOTAL RESULTS FROM ALL VENDORS: {len(combined_data)}", flush=True)
        
        # Save qualified data (only first image)
        qualified_data = combined_data[
            (combined_data['First Image'].notna()) & 
            (combined_data['First Image'] != '')
        ]
        qualified_data.to_csv('Luxurybrand_qualified_data.csv', encoding='utf-8-sig', index=False)
        print(f"QUALIFIED RESULTS (with first image): {len(qualified_data)}", flush=True)
    
    # Save all failed downloads
    if all_failed_downloads:
        pd.DataFrame(all_failed_downloads, columns=["Vendor_ID", "Tag", "Image Name", "URL"]).to_csv("all_failed_downloads.csv", index=False)
        print(f"Total failed downloads across all vendors: {len(all_failed_downloads)}", flush=True)
    
    print("All processing completed!", flush=True)
    logging.info("Script completed successfully")


if __name__ == "__main__":
    main()