import requests
from bs4 import BeautifulSoup
import logging
import time
import re
from urllib.parse import urljoin, urlparse, parse_qs, unquote
import base64
import json
import hashlib
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import sys
import traceback

# Configure logging for verbose output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.StreamHandler(), # Output to console
    logging.FileHandler('fetcher.log') # Save to file for debugging
])
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Constants
UNLICENSED_MESSAGE = "License required to access this information"

# Get environment variables
logger.debug("Loading environment variables")
WP_SITE_URL = os.getenv('WP_SITE_URL', 'https://mauritius.mimusjobs.com')
WP_USERNAME = os.getenv('WP_USERNAME', '')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD', '')
COUNTRY = os.getenv('COUNTRY', 'Mauritius')
KEYWORD = os.getenv('KEYWORD', '')
FETCHER_TOKEN = os.getenv('FETCHER_TOKEN', '')
VALID_LICENSE_KEY = os.getenv('VALID_LICENSE_KEY', '')  # Set this in your environment

logger.debug(f"Environment variables loaded: WP_SITE_URL={WP_SITE_URL}, COUNTRY={COUNTRY}, KEYWORD={KEYWORD}")

# WordPress endpoints (will be updated in main() if custom site_url provided)
WP_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job-listings"
WP_COMPANY_URL = f"{WP_SITE_URL}/wp-json/wp/v2/company"
WP_MEDIA_URL = f"{WP_SITE_URL}/wp-json/wp/v2/media"
WP_JOB_TYPE_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_type"
WP_JOB_REGION_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_region"
WP_SAVE_COMPANY_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-company"
WP_SAVE_JOB_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-job"
WP_FETCHER_STATUS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-status"
WP_CREDENTIALS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-credentials"

PROCESSED_IDS_FILE = "processed_job_ids.json"
LAST_PAGE_FILE = "last_processed_page.txt"

JOB_TYPE_MAPPING = {
    "Full-time": "full-time",
    "Part-time": "part-time",
    "Contract": "contract",
    "Temporary": "temporary",
    "Freelance": "freelance",
    "Internship": "internship",
    "Volunteer": "volunteer"
}

FRENCH_TO_ENGLISH_JOB_TYPE = {
    "Temps plein": "Full-time",
    "Temps partiel": "Part-time",
    "Contrat": "Contract",
    "Temporaire": "Temporary",
    "Indépendant": "Freelance",
    "Stage": "Internship",
    "Bénévolat": "Volunteer"
}

def fetch_credentials():
    """Fetch WordPress credentials from the REST API if not provided in environment."""
    global WP_USERNAME, WP_APP_PASSWORD
    logger.debug("Attempting to fetch WordPress credentials")
    if WP_USERNAME and WP_APP_PASSWORD:
        logger.info("Credentials provided via environment variables")
        return True
    logger.info(f"Fetching credentials from {WP_CREDENTIALS_URL}")
    try:
        response = requests.get(WP_CREDENTIALS_URL, timeout=5, verify=False)
        logger.debug(f"Credentials request status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.error(f"Failed to fetch credentials: {data.get('message', 'Unknown error')}")
            return False
        WP_USERNAME = data.get('wp_username')
        WP_APP_PASSWORD = data.get('wp_app_password')
        if not WP_USERNAME or not WP_APP_PASSWORD:
            logger.error("Credentials fetched but empty or invalid")
            return False
        logger.info("Successfully fetched credentials from WordPress")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch credentials from {WP_CREDENTIALS_URL}: {str(e)}")
        return False

def check_fetcher_status(auth_headers):
    """Check the fetcher status from WordPress."""
    logger.debug(f"Checking fetcher status at {WP_FETCHER_STATUS_URL}")
    try:
        response = requests.get(WP_FETCHER_STATUS_URL, headers=auth_headers, timeout=5, verify=False)
        response.raise_for_status()
        status = response.json().get('status', 'stopped')
        logger.info(f"Fetcher status: {status}")
        return status
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check fetcher status: {str(e)}")
        return 'stopped'

def sanitize_text(text, is_url=False):
    if not text:
        return ''
    if is_url:
        text = text.strip()
        if not text.startswith(('http://', 'https://')):
            text = 'https://' + text
        return text
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
    text = re.sub(r'(\w)(\w)', r'\1 \2', text) if re.match(r'^\w+$', text) else text
    return ' '.join(text.split())

def normalize_for_deduplication(text):
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text.lower()

def generate_id(combined):
    return hashlib.md5(combined.encode()).hexdigest()[:16]

def split_paragraphs(text, max_length=200):
    paragraphs = text.split('\n\n')
    result = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        while len(para) > max_length:
            split_point = para.rfind(' ', 0, max_length)
            if split_point == -1:
                split_point = para.rfind('.', 0, max_length)
            if split_point == -1:
                split_point = max_length
            result.append(para[:split_point].strip())
            para = para[split_point:].strip()
        if para:
            result.append(para)
    return '\n\n'.join(result)

def save_company_to_wordpress(index, company_data, wp_headers, licensed):
    company_name = company_data.get("company_name", "")
    company_details = company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else "")
    company_logo = company_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    company_website = company_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else "")
    company_industry = company_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = company_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    company_type = company_data.get("company_type", UNLICENSED_MESSAGE if not licensed else "")
    company_address = company_data.get("company_address", UNLICENSED_MESSAGE if not licensed else "")

    company_id = generate_id(company_name)
    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": company_details,
        "company_logo": sanitize_text(company_logo, is_url=True),
        "company_website": sanitize_text(company_website, is_url=True),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_type": sanitize_text(company_type),
        "company_address": sanitize_text(company_address),
        "company_tagline": sanitize_text(company_details),
        "company_twitter": "",
        "company_video": ""
    }
    
    try:
        response = requests.post(WP_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15)
        response.raise_for_status()
        post = response.json()
        if post.get('success'):
            logger.info(f"Successfully saved company {company_name}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"Company {company_name} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers, licensed):
    job_title = job_data.get("job_title", "")
    job_description = job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else "")
    job_type = job_data.get("job_type", "")
    location = job_data.get("location", "Mauritius")
    job_url = job_data.get("job_url", "")
    company_name = job_data.get("company_name", "")
    company_logo = job_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    environment = job_data.get("environment", UNLICENSED_MESSAGE if not licensed else "").lower()
    job_salary = job_data.get("job_salary", "")
    company_industry = job_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = job_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")

    application = ''
    if '@' in job_data.get("description_application_info", ""):
        application = job_data.get("description_application_info", "")
    elif job_data.get("resolved_application_url", ""):
        application = job_data.get("resolved_application_url", "")
    else:
        application = job_data.get("application_url", "")

    job_id = generate_id(f"{job_title}_{company_name}")
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_description,
        "job_type": sanitize_text(job_type),
        "location": sanitize_text(location),
        "job_url": sanitize_text(job_url, is_url=True),
        "environment": sanitize_text(environment),
        "job_salary": sanitize_text(job_salary),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_website_url": sanitize_text(job_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else ""), is_url=True),
        "company_logo": sanitize_text(company_logo, is_url=True),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_address": job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_twitter": "",
        "company_video": ""
    }
    
    try:
        response = requests.post(WP_JOB_URL, json=post_data, headers=auth_headers, timeout=15)
        response.raise_for_status()
        post = response.json()
        if post.get('success'):
            logger.info(f"Successfully saved job {job_title}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"Job {job_title} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}")
        return None, None

def load_processed_ids():
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(json.load(f))
            logger.info(f"Loaded {len(processed_ids)} processed job IDs")
    except Exception as e:
        logger.error(f"Failed to load processed IDs: {str(e)}")
    return processed_ids

def save_processed_ids(processed_ids):
    try:
        with open(PROCESSED_IDS_FILE, "w") as f:
            json.dump(list(processed_ids), f)
        logger.info(f"Saved {len(processed_ids)} job IDs")
    except Exception as e:
        logger.error(f"Failed to save processed IDs: {str(e)}")

def load_last_page():
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                return int(f.read().strip())
    except Exception as e:
        logger.error(f"Failed to load last page: {str(e)}")
    return 0

def save_last_page(page):
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"Saved last processed page: {page}")
    except Exception as e:
        logger.error(f"Failed to save last page: {str(e)}")

def scrape_job_details(job_url, licensed):
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Basic job info (always available)
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Mauritius'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        
        # Licensed data
        company_logo = UNLICENSED_MESSAGE
        company_url = UNLICENSED_MESSAGE
        environment = UNLICENSED_MESSAGE
        level = UNLICENSED_MESSAGE
        job_functions = UNLICENSED_MESSAGE
        industries = UNLICENSED_MESSAGE
        job_description = UNLICENSED_MESSAGE
        application_url = UNLICENSED_MESSAGE
        description_application_info = UNLICENSED_MESSAGE
        resolved_application_info = UNLICENSED_MESSAGE
        final_application_email = UNLICENSED_MESSAGE
        final_application_url = UNLICENSED_MESSAGE
        resolved_application_url = UNLICENSED_MESSAGE
        company_details = UNLICENSED_MESSAGE
        company_website_url = UNLICENSED_MESSAGE
        company_industry = UNLICENSED_MESSAGE
        company_size = UNLICENSED_MESSAGE
        company_headquarters = UNLICENSED_MESSAGE
        company_type = UNLICENSED_MESSAGE
        company_founded = UNLICENSED_MESSAGE
        company_specialties = UNLICENSED_MESSAGE
        company_address = UNLICENSED_MESSAGE
        
        if licensed:
            # Company logo
            company_logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            company_logo = (company_logo_elem.get('data-delayed-url') or company_logo_elem.get('src') or '') if company_logo_elem else ''
            
            # Company URL
            company_url_elem = soup.select_one(".topcard__org-name-link")
            company_url = company_url_elem['href'] if company_url_elem and company_url_elem.get('href') else ''
            if company_url:
                company_url = re.sub(r'\?.*$', '', company_url)
            
            # Environment
            env_element = soup.select(".topcard__flavor--metadata")
            for elem in env_element:
                text = elem.get_text().strip().lower()
                if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                    environment = elem.get_text().strip()
                    break
            
            # Job criteria (licensed only)
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
            
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            
            # Job description
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                paragraphs = description_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    seen = set()
                    unique_paragraphs = []
                    for p in paragraphs:
                        para = sanitize_text(p.get_text().strip())
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                    job_description = '\n\n'.join(unique_paragraphs)
                else:
                    raw_text = description_container.get_text(separator='\n').strip()
                    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                    seen = set()
                    unique_paragraphs = []
                    for para in paragraphs:
                        para = sanitize_text(para)
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                    job_description = '\n\n'.join(unique_paragraphs)
                
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
            
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
            
            # Extract email from description if available
            if job_description:
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                emails = re.findall(email_pattern, job_description)
                if emails:
                    description_application_info = emails[0]
            
            if company_url and 'linkedin.com' in company_url:
                try:
                    company_response = session.get(company_url, headers=headers, timeout=15)
                    company_response.raise_for_status()
                    company_soup = BeautifulSoup(company_response.text, 'html.parser')
                    
                    company_details_elem = company_soup.select_one("p.about-us__description") or company_soup.select_one("section.core-section-container > div > p")
                    company_details = company_details_elem.get_text().strip() if company_details_elem else ''
                    
                    company_website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a")
                    company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                    
                    def get_company_detail(label):
                        elements = company_soup.select("section.core-section-container.core-section-container--with-border > div > dl > div")
                        for elem in elements:
                            dt = elem.find("dt")
                            if dt and dt.get_text().strip().lower() == label.lower():
                                dd = elem.find("dd")
                                return dd.get_text().strip() if dd else ''
                        return ''
                    
                    company_industry = get_company_detail("Industry")
                    company_size = get_company_detail("Company size")
                    company_headquarters = get_company_detail("Headquarters")
                    company_type = get_company_detail("Type")
                    company_founded = get_company_detail("Founded")
                    company_specialties = get_company_detail("Specialties")
                    company_address = company_soup.select_one("#address-0")
                    company_address = company_address.get_text().strip() if company_address else company_headquarters
                    
                except Exception as e:
                    logger.error(f"Error fetching company page {company_url}: {str(e)}")
        
        row = [
            job_title, company_logo, company_name, company_url, location, environment,
            job_type, level, job_functions, industries, job_description, job_url,
            company_details, company_website_url, company_industry, company_size,
            company_headquarters, company_type, company_founded, company_specialties,
            company_address, application_url, description_application_info,
            resolved_application_info, final_application_email, final_application_url,
            resolved_application_url
        ]
        return row
    except Exception as e:
        logger.error(f"Error in scrape_job_details for {job_url}: {str(e)}")
        return None

def crawl(auth_headers, processed_ids, licensed, country, keyword):
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10
    
    for i in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords={keyword}&location={country}&start={i * 25}'
        logger.info(f"Fetching job search page: {url}")
        time.sleep(random.uniform(5, 10))
        
        try:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            if "login" in response.url or "challenge" in response.url:
                logger.error(f"Login or CAPTCHA detected at {response.url}, stopping crawl")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list > li a")
            urls = [a['href'] for a in job_list if a.get('href')]
            
            logger.info(f"Found {len(urls)} job URLs on page {i}")
            if not urls:
                logger.warning(f"No job URLs found on page {i}")
                continue
            
            for index, job_url in enumerate(urls):
                job_data = scrape_job_details(job_url, licensed)
                if not job_data:
                    logger.error(f"No data scraped for job: {job_url}")
                    failure_count += 1
                    total_jobs += 1
                    continue
                
                job_dict = {
                    "job_title": job_data[0],
                    "company_logo": job_data[1],
                    "company_name": job_data[2],
                    "company_url": job_data[3],
                    "location": job_data[4],
                    "environment": job_data[5],
                    "job_type": job_data[6],
                    "level": job_data[7],
                    "job_functions": job_data[8],
                    "industries": job_data[9],
                    "job_description": job_data[10],
                    "job_url": job_data[11],
                    "company_details": job_data[12],
                    "company_website_url": job_data[13],
                    "company_industry": job_data[14],
                    "company_size": job_data[15],
                    "company_headquarters": job_data[16],
                    "company_type": job_data[17],
                    "company_founded": job_data[18],
                    "company_specialties": job_data[19],
                    "company_address": job_data[20],
                    "application_url": job_data[21],
                    "description_application_info": job_data[22],
                    "resolved_application_info": job_data[23],
                    "final_application_email": job_data[24],
                    "final_application_url": job_data[25],
                    "resolved_application_url": job_data[26],
                    "job_salary": ""
                }
                
                job_title = job_dict.get("job_title", "Unknown Job")
                company_name = job_dict.get("company_name", "")
                job_id = generate_id(f"{job_title}_{company_name}")
                
                if job_id in processed_ids:
                    logger.info(f"Skipping already processed job: {job_id}")
                    total_jobs += 1
                    continue
                
                if not company_name or company_name.lower() == "unknown":
                    logger.info(f"Skipping job with unknown company: {job_title}")
                    failure_count += 1
                    total_jobs += 1
                    continue
                
                total_jobs += 1
                
                company_id, company_message = save_company_to_wordpress(index, job_dict, auth_headers, licensed)
                if company_id is None:
                    failure_count += 1
                    continue
                
                job_post_id, job_message = save_article_to_wordpress(index, job_dict, company_id, auth_headers, licensed)
                
                if job_post_id is not None:
                    processed_ids.add(job_id)
                    success_count += 1
                    print(f"Job '{job_title}' at {company_name} saved successfully")
                else:
                    failure_count += 1
                    print(f"Job '{job_title}' at {company_name} failed to save")
            
            save_last_page(i + 1)
            
        except Exception as e:
            logger.error(f"Error fetching job search page {url}: {str(e)}")
            failure_count += 1
    
    save_processed_ids(processed_ids)
    logger.info(f"Crawl completed. Total: {total_jobs}, Success: {success_count}, Failed: {failure_count}")
    print(f"\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully saved: {success_count}")
    print(f"Failed: {failure_count}")

def main():
    logger.debug("Starting execution")
    
    # Get parameters from command-line arguments or environment variables
    license_key = sys.argv[1] if len(sys.argv) > 1 else os.getenv('LICENSE_KEY', '')
    country = sys.argv[2] if len(sys.argv) > 2 else os.getenv('COUNTRY', 'Mauritius')
    keyword = sys.argv[3] if len(sys.argv) > 3 else os.getenv('KEYWORD', '')
    site_url = sys.argv[4] if len(sys.argv) > 4 else os.getenv('WP_SITE_URL', 'https://mauritius.mimusjobs.com')
    wp_username = sys.argv[5] if len(sys.argv) > 5 else os.getenv('WP_USERNAME', '')
    wp_app_password = sys.argv[6] if len(sys.argv) > 6 else os.getenv('WP_APP_PASSWORD', '')
    
    # Validate license - only true if VALID_LICENSE_KEY is set and matches provided key
    licensed = bool(VALID_LICENSE_KEY) and license_key == VALID_LICENSE_KEY
    
    if not licensed and bool(VALID_LICENSE_KEY):
        logger.warning("No valid license key provided. Scraping limited data.")
        print("Warning: No valid license key provided. Only basic job data will be scraped.")
    elif licensed:
        logger.info("Valid license key provided. Scraping full job data.")
        print("Valid license key provided. Scraping full job data.")
    else:
        logger.info("No license configuration found. Running in unlicensed mode.")
        print("Running in unlicensed mode. Only basic job data will be scraped.")
    
    # Validate required parameters
    if not wp_username or not wp_app_password:
        logger.error("WP_USERNAME and WP_APP_PASSWORD are required")
        print("Error: WP_USERNAME and WP_APP_PASSWORD must be provided")
        sys.exit(1)
    
    if not site_url:
        logger.error("WP_SITE_URL is required")
        print("Error: WP_SITE_URL must be provided")
        sys.exit(1)
    
    # Update global URLs with provided site_url
    global WP_JOB_URL, WP_COMPANY_URL, WP_SAVE_COMPANY_URL, WP_SAVE_JOB_URL
    WP_JOB_URL = f"{site_url.rstrip('/')}/wp-json/fetcher/v1/save-job"
    WP_COMPANY_URL = f"{site_url.rstrip('/')}/wp-json/fetcher/v1/save-company"
    WP_SAVE_COMPANY_URL = WP_COMPANY_URL
    WP_SAVE_JOB_URL = WP_JOB_URL
    
    # Create authentication headers
    auth_string = f"{wp_username}:{wp_app_password}"
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    
    processed_ids = load_processed_ids()
    crawl(auth_headers=wp_headers, processed_ids=processed_ids, licensed=licensed, country=country, keyword=keyword)
    logger.debug("Execution completed")

if __name__ == "__main__":
    main()
