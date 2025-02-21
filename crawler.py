import os
import time
import datetime
import urllib.parse
import threading
from concurrent.futures import ThreadPoolExecutor, wait

import requests
import urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from process_pdf import extract_text_from_pdf
from database import get_data

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize global variables
LOG_FILENAME = None
STATS = None

# Helper Function: Write log
def write_log(message):
    """Write log with timestamp"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {message}\n")

# Helper Function: Initialize Selenium WebDriver
def init_driver():
    """Initialize Selenium WebDriver"""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    options.add_argument('--disable-blink-features=AutomationControlled') 
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
    return webdriver.Chrome(options=options)

# Helper Function: Get search results using selenium
def get_search_results(driver, company_name, search_url, search_query, max_trials=3):
    for trial in range(max_trials): # Try up to 3 times
        try:
            # Visit search page
            driver.get(search_url)
            wait = WebDriverWait(driver, 30)
            
            # Wait for search results to load
            search_results = wait.until(
                EC.presence_of_all_elements_located(search_query)
            )
            
            # Check if search results are retrieved successfully
            if search_results:
                return search_results
            # If not found, wait 2 seconds and retry
            time.sleep(2)

        # If there is an error, and not reached max trials, wait 2 seconds and retry
        except Exception as e:
            if trial < max_trials - 1:
                time.sleep(2)
                continue
            # If reached max trials, write log and return None
            write_log(f"{company_name}: Failed to get search results after {max_trials} attempts: {str(e)}")
            return None
        
    # If all attempts failed, return None
    return None

# Helper Function: Download PDF file (including verify whether content contains scope 1 or scope 2)
def download_pdf(company_name, url, max_trials=3):
    
    # Check if URL is PDF
    if 'pdf' not in url:
        write_log(f"{company_name}: Is not a PDF URL | URL: {url}")
        return None
    
    # Set download request headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/pdf'
    }

    # Create PDF file path
    pdf_path = f"./reports/{company_name}.pdf"

    for trial in range(max_trials): # Try up to 3 times
        try:
            # Send request
            response = requests.get(url, headers=headers, verify=False, timeout=30)
            
            # If request successful, process PDF file
            if response.status_code == 200:
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
                
                # Check if PDF content contains scope 1 or scope 2
                text = extract_text_from_pdf(pdf_path) # Only extract the page that contains scope 1 or scope 2
                if text == None or text == "": 
                    write_log(f"{company_name}: PDF content does not contain 'scope 1' or 'scope 2' | URL: {url}")
                    if os.path.exists(pdf_path):
                        os.remove(pdf_path)
                    return None
                else:
                    write_log(f"{company_name}: Valid PDF downloaded | URL: {url}")
                    return pdf_path
        
        # If there is an error, and not reached max trials, wait 2 seconds and retry
        except Exception as e:
            if trial < max_trials - 1:
                time.sleep(2)
                continue

            # If reached max trials, delete PDF file
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
            write_log(f"{company_name}: PDF Processing Error | Error: {e} | URL: {url}")
            return None
    
    # If all attempts failed, delete file path
    write_log(f"{company_name}: Failed to download PDF after {max_trials} attempts | URL: {url}")
    if os.path.exists(pdf_path):
        os.remove(pdf_path)
    return None

# Step 1: Try to search PDF directly in Bing
def search_pdf_in_bing(driver, company_name):
    
    # Search query
    search_query = f"{company_name} sustainability report 2024 pdf -responsibilityreports"
    search_url = f"https://www.bing.com/search?q={urllib.parse.quote(search_query)}&first=1&form=QBRE"
    write_log(f"{company_name}: Searching PDF in Bing | URL: {search_url}")

    # Call helper function to get search results
    search_query = (By.CSS_SELECTOR, '.b_algo h2 a')
    search_results = get_search_results(driver, company_name, search_url, search_query)
    
    # If no search results found, return None
    if not search_results:
        write_log(f"{company_name}: No Search Results Found | URL: {search_url}")
        return None

    # Extract PDF links from search results
    pdf_links = []
    for result in search_results:
        url = result.get_attribute('href').lower()
        if url and '.pdf' in url:
            pdf_links.append(url)   

    # If no PDF links found, return None
    if not pdf_links:
        write_log(f"{company_name}: No PDF Links Found in Search Results | URL: {search_url}")
        return None

    # Try to download PDF (only pdf content contains scope 1 or scope 2 will be downloaded)
    for pdf in pdf_links:
        pdf_path = download_pdf(company_name, pdf)
        if pdf_path:
            return pdf_path
        
    write_log(f"{company_name}: No Valid PDF Found in Search Results")
    return None

# Step 2: If PDF not found directly in Bing, search company's sustainability website
def search_webpage_in_bing(driver, company_name):
    
    # Search query
    search_query = f"{company_name} sustainability report -responsibilityreports"
    search_url = f"https://www.bing.com/search?q={urllib.parse.quote(search_query)}&first=1&form=QBRE"
    write_log(f"{company_name}: Searching Webpage in Bing | URL: {search_url}")
        
    # Call helper function to get search results
    search_query = (By.CSS_SELECTOR, '.b_algo h2 a')
    search_results = get_search_results(driver, company_name, search_url, search_query)

    # If no search results found, return None
    if not search_results:
        write_log(f"{company_name}: No Search Results Found | URL: {search_url}")
        return None
    
    # Extract first 3 non-PDF webpage links from search results
    url_list = []
    count = 0
    for result in search_results:
        if count >= 3:
            break
        try:
            url = result.get_attribute('href')
            if url and '.pdf' not in url.lower(): # Only non-PDF links will be added
                url_list.append(url)
                count += 1
        except Exception as e:
            write_log(f"{company_name}: Error getting URL from search result: {str(e)}")
            continue

    # If no valid URL found, return None
    if not url_list:
        write_log(f"{company_name}: No Valid URL Found in Search Results")
        return None
            
    return url_list

# Step 3: Find PDF links in company's sustainability website
def find_pdf_in_webpage(driver, company_name, url):

    write_log(f"{company_name}: Searching PDF in Webpage | URL: {url}")

    # Call helper function to get search results
    search_query = (By.TAG_NAME, "a")
    search_results = get_search_results(driver, company_name, url, search_query)

    # If no search results found, return None
    if not search_results:
        write_log(f"{company_name}: No Search Results Found | URL: {url}")
        return None
    
    # Extract PDF links from search results
    pdf_links = []
    for result in search_results:
        try:
            # Get href attribute of link
            href = result.get_attribute('href')
            if not href:  # Skip if href is None or empty string
                continue

            # Check if link is PDF
            is_pdf = ('.pdf' in href.lower())

            # Check if link text contains keywords
            text = result.text.lower()
            keywords = ['report', 'esg', 'sustainability', 'impact', 'environment', 'green', 'carbon', 'emissions']
            has_keywords = any(keyword in text for keyword in keywords)
            
            # Check if it's PDF and contains keywords
            if is_pdf and has_keywords and (href not in pdf_links):
                pdf_links.append(href)
                
        except Exception as e:
            # If element is stale, continue to next one
            continue
    write_log(f"{company_name}: Found {len(pdf_links)} PDF on webpage.")

    if not pdf_links:
        return None

    # Download and check first 10 PDFs
    for pdf in pdf_links[:10]:
        pdf_path = download_pdf(company_name, pdf)
        if pdf_path:
            return pdf_path
    
    write_log(f"{company_name}: No Valid PDF Found in Webpage")
    return None
        
# Process single company
def process_company(company_name):

    print(f"Processing {company_name}...")
    driver = init_driver()
    
    # 1. Search PDF directly
    pdf_url = search_pdf_in_bing(driver, company_name)
    if pdf_url:
        with threading.Lock():  # Use lock to protect shared resource access
            STATS['direct_pdf_success'] += 1
        driver.quit()
        return pdf_url
    
    # 2. If PDF not found, search webpage, and find PDF in webpage
    webpage_url_list = search_webpage_in_bing(driver, company_name)
    if webpage_url_list:
        for url in webpage_url_list:
            pdf_url = find_pdf_in_webpage(driver, company_name, url)
            if pdf_url: 
                with threading.Lock():  # Use lock to protect shared resource access
                    STATS['webpage_pdf_success'] += 1
                driver.quit()
                return pdf_url
    
    # If all methods failed
    with threading.Lock():  # Use lock to protect shared resource access
        STATS['failed_companies'].append(company_name)
    driver.quit()

# Process a batch of companies
def process_batch(table_name, total_batches, batch_num):

    print(f"\nStarting Batch {batch_num}...")
    
    # Initialize global statistics
    global STATS
    STATS = {
        'total_companies': 0,
        'direct_pdf_success': 0,
        'webpage_pdf_success': 0,
        'failed_companies': []
    }
    
    # Create logs directory (if not exists)
    os.makedirs('./logs', exist_ok=True)
    
    # Set global log filename
    global LOG_FILENAME
    LOG_FILENAME = f'./logs/crawler_batch{batch_num}_log.txt'
    summary_filename = f'./logs/crawler_batch{batch_num}_summary.txt'
    
    # Get company list from database
    query = f"SELECT company_name FROM {table_name}"
    companies = get_data(query)
    
    # Get company list for corresponding batch
    batch_companies = [
        company['company_name'] 
        for i, company in enumerate(companies) 
        if i % total_batches == (batch_num - 1)
    ]
    
    # Get list of existing PDF files
    existing_pdfs = {
        os.path.splitext(f)[0] 
        for f in os.listdir('./reports') 
        if f.endswith('.pdf')
    }

    # Filter out companies that already have PDFs
    companies_to_process = [
        company_name 
        for company_name in batch_companies 
        if company_name not in existing_pdfs
    ]

    # Add start delimiter to log
    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        start_time = datetime.datetime.now()
        f.write("="*50 + "\n")
        f.write(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n")

    STATS['total_companies'] = len(companies_to_process)
    
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_company, company_name)
            for company_name in companies_to_process
        ]
        wait(futures)

    '''
    # Without using threads
    for company_name in companies_to_process:
        process_company(company_name)
    '''
    
    # Generate summary report
    with open(summary_filename, 'a', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write(f"Crawler Summary Report - Batch {batch_num}\n")
        f.write(f"Total Companies: {STATS['total_companies']}\n")
        f.write(f"Direct PDF Search Success: {STATS['direct_pdf_success']}\n")
        f.write(f"Webpage PDF Search Success: {STATS['webpage_pdf_success']}\n")
        f.write(f"Failed Companies: {len(STATS['failed_companies'])}\n")
        f.write("\nList of Failed Companies:\n")
        for company in STATS['failed_companies']:
            f.write(f"- {company}\n")
        f.write("\n" + "="*50 + "\n")
    
    # Add end delimiter to log
    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        end_time = datetime.datetime.now()
        f.write("="*50 + "\n")
        f.write(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n")
    
    print(f"Batch {batch_num} completed")

# Process a batch of companies
def process_missing_reports(table_name):

    print(f"\nStarting...")
    
    global STATS
    STATS = {
        'total_companies': 0,
        'direct_pdf_success': 0,
        'webpage_pdf_success': 0,
        'failed_companies': []
    }
    
    global LOG_FILENAME
    LOG_FILENAME = f'./logs/crawler_missing_reports_log.txt'
    summary_filename = f'./logs/crawler_missing_reports_summary.txt'
    
    query = f"SELECT company_name FROM {table_name}"
    companies = get_data(query)

    existing_pdfs = {
        os.path.splitext(f)[0] 
        for f in os.listdir('./reports') 
        if f.endswith('.pdf')
    }

    companies_to_process = [
        company['company_name']
        for company in companies 
        if company['company_name'] not in existing_pdfs
    ]

    with open('./reports/_missing_reports.txt', 'a') as f:
        f.write(f"="*50 + "\n")
        f.write(f"Total missing reports: {len(companies_to_process)}\n")
        for company in companies_to_process:
            f.write(company + '\n')

    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        start_time = datetime.datetime.now()
        f.write("="*50 + "\n")
        f.write(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n")

    STATS['total_companies'] = len(companies_to_process)
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_company, company_name)
            for company_name in companies_to_process
        ]
        wait(futures)
    
    with open(summary_filename, 'a', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write(f"Crawler Summary Report - Missing Reports\n")
        f.write(f"Total Companies: {STATS['total_companies']}\n")
        f.write(f"Direct PDF Search Success: {STATS['direct_pdf_success']}\n")
        f.write(f"Webpage PDF Search Success: {STATS['webpage_pdf_success']}\n")
        f.write(f"Failed Companies: {len(STATS['failed_companies'])}\n")
        f.write("\nList of Failed Companies:\n")
        for company in STATS['failed_companies']:
            f.write(f"- {company}\n")
        f.write("\n" + "="*50 + "\n")
    
    with open(LOG_FILENAME, 'a', encoding='utf-8') as f:
        end_time = datetime.datetime.now()
        f.write("="*50 + "\n")
        f.write(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n")
    
    print(f"Completed")

if __name__ == "__main__":
    # Test single company
    #company_name = "APPLE"
    #process_company(company_name)

    # Batch processing
    table_name = "emissions_data"
    total_batches = 10  # Divide the whole list into 10 batches
    attempt_times = 8  # Each batch try 8 times

    for i in range(attempt_times):
        for j in range(total_batches):
            batch_num = j + 1
            process_batch(table_name, total_batches, batch_num)

    # Process missing reports
    # process_missing_reports(table_name)
