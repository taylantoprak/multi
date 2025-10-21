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

def request_data(vendor: str, start_date, end_date, next_page, retries=3, delay=15):
    """Fetch paginated data from API with retry logic."""
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

            response = requests.post(
                f'https://{vendor}.wsxcme.com/album/personal/all',
                params=params,
                cookies=cookies,
                headers=get_headers(vendor),
                timeout=30
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
    next_page = pagination_data.get('pageTimestamp')
    if isinstance(next_page, (int, str)) and str(next_page).strip():
        print(f'Next page found: {str(next_page)}', flush=True)
        return str(next_page)
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

def download_file(url, folder, good_id, sequence, retries=4, delay=15):
    """Download a file with retry logic."""
    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(url, headers=imgheaders, stream=True, timeout=30)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'image/jpeg' in content_type:
                    file_extension = 'jpg'
                elif 'image/png' in content_type:
                    file_extension = 'png'
                elif 'image/gif' in content_type:
                    file_extension = 'gif'
                else:
                    file_extension = 'jpg'
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
    logging.error(f"Max retries reached. Failed to download: {url}")
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
    vendor_folder = os.path.join(base_directory, vendor)
    os.makedirs(vendor_folder, exist_ok=True)
    folder_path = os.path.join(vendor_folder, tag_name)
    os.makedirs(folder_path, exist_ok=True)
    def handle_failed_download(url):
        if url and url not in failed_urls_set:
            failed_urls_set.add(url)
            failed_downloads.append([vendor, tag_name, good_id, url])
    if first_img_url:
        success, error = download_file(first_img_url, folder_path, good_id, 1)
        if not success and error:
            handle_failed_download(first_img_url)

def download_with_multithreading(qualified_df, directory, vendor):
    total = len(qualified_df)
    completed = 0
    print(f"Starting download process for {total} products from vendor {vendor}...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_download, row, directory, failed_downloads, vendor): index for index, row in qualified_df.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            progress = (completed / total) * 100
            sys.stdout.write(f"\rProgress: {completed:>4}/{total:<4} | {progress:>6.2f}% completed")
            sys.stdout.flush()
    print()

def process_vendor(vendor, start_date, end_date):
    print(f"\n{'='*60}")
    print(f"Processing vendor: {vendor}")
    print(f"{'='*60}")
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
        download_with_multithreading(qualified_df, os.getcwd(), vendor)
    else:
        print(f"No qualified results for vendor {vendor}. Skipping downloads.")

def main():
    print(f"Processing {len(vendors)} vendors...")
    print(f"Date range: {start_date} to {end_date}")
    print(f"{'='*80}")
    successful_vendors = 0
    for i, vendor in enumerate(vendors, 1):
        print(f"\nProcessing vendor {i}/{len(vendors)}: {vendor}")
        print(f"Progress: {i}/{len(vendors)} vendors")
        try:
            process_vendor(vendor, start_date, end_date)
            successful_vendors += 1
            print(f"✅ Successfully processed vendor {vendor}")
        except Exception as e:
            print(f"❌ Error processing vendor {vendor}: {e}")
            logging.error(f"Error processing vendor {vendor}: {e}")
            continue
    if failed_downloads:
        failed_csv = "failed_downloads.csv"
        pd.DataFrame(failed_downloads, columns=["Vendor", "Tag", "Image Name", "URL"]).to_csv(failed_csv, index=False)
        print(f"\n⚠️  {len(failed_downloads)} failed downloads saved to '{failed_csv}'")
    else:
        print(f"\n✅ No failed downloads!")
    print(f"\n{'='*80}")
    print(f"SUMMARY:")
    print(f"Total vendors: {len(vendors)}")
    print(f"Successfully processed: {successful_vendors}")
    print(f"Failed: {len(vendors) - successful_vendors}")
    print(f"Failed downloads: {len(failed_downloads)}")
    print(f"{'='*80}")
    logging.info(f"Script completed successfully. Processed {successful_vendors}/{len(vendors)} vendors")

if __name__ == "__main__":
    main()
