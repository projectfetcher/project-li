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

# Create uploads directory if it doesn't exist
os.makedirs("uploads", exist_ok=True)

# Configure logging with verbose output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join("uploads", 'fetcher.log'))
    ]
)
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}
logger.debug(f"Initialized HTTP headers: {headers}")

# Constants for WordPress (using site_url from command-line arguments)
def get_wp_urls(site_url):
    return {
        "WP_URL": f"{site_url}/wp-json/wp/v2/job-listings",
        "WP_COMPANY_URL": f"{site_url}/wp-json/wp/v2/company",
        "WP_MEDIA_URL": f"{site_url}/wp-json/wp/v2/media",
        "WP_JOB_TYPE_URL": f"{site_url}/wp-json/wp/v2/job_listing_type",
        "WP_JOB_REGION_URL": f"{site_url}/wp-json/wp/v2/job_listing_region",
        "WP_SAVE_COMPANY_URL": f"{site_url}/wp-json/mimus_job_fetcher/v1/save-company",
        "WP_SAVE_JOB_URL": f"{site_url}/wp-json/mimus_job_fetcher/v1/save-job",
        "WP_FETCHER_STATUS_URL": f"{site_url}/wp-json/mimus_job_fetcher/v1/get-status",
        "WP_CREDENTIALS_URL": f"{site_url}/wp-json/mimus_job_fetcher/v1/get-credentials"
    }

PROCESSED_IDS_FILE = os.path.join("uploads", "processed_job_ids.json")
LAST_PAGE_FILE = os.path.join("uploads", "last_processed_page.txt")
logger.debug(f"File paths: PROCESSED_IDS_FILE={PROCESSED_IDS_FILE}, LAST_PAGE_FILE={LAST_PAGE_FILE}")

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
logger.debug(f"Job type mappings: {JOB_TYPE_MAPPING}")
logger.debug(f"French to English job type mappings: {FRENCH_TO_ENGLISH_JOB_TYPE}")

# Valid license key for full data scraping
VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/job-fetcher'
logger.debug(f"Valid license key: {'*' * len(VALID_LICENSE_KEY)}")
logger.debug(f"Unlicensed message: {UNLICENSED_MESSAGE}")

def sanitize_text(text, is_url=False):
    logger.debug(f"sanitize_text called with text='{text[:50]}{'...' if len(text) > 50 else ''}', is_url={is_url}")
    if not text:
        logger.debug("sanitize_text: Empty text, returning empty string")
        return ''
    if is_url:
        text = text.strip()
        logger.debug(f"sanitize_text: Stripped text='{text}'")
        if not text.startswith(('http://', 'https://')):
            text = 'https://' + text
            logger.debug(f"sanitize_text: Added https:// prefix, text='{text}'")
        logger.debug(f"sanitize_text: Returning URL='{text}'")
        return text
    text = re.sub(r'<[^>]+>', '', text)
    logger.debug(f"sanitize_text: Removed HTML tags, text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
    logger.debug(f"sanitize_text: Added space after periods, text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = re.sub(r'(\w)(\w)', r'\1 \2', text) if re.match(r'^\w+$', text) else text
    logger.debug(f"sanitize_text: Separated fused words, text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = ' '.join(text.split())
    logger.debug(f"sanitize_text: Normalized whitespace, returning text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    return text

def normalize_for_deduplication(text):
    logger.debug(f"normalize_for_deduplication called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = re.sub(r'[^\w\s]', '', text)
    logger.debug(f"normalize_for_deduplication: Removed punctuation, text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = re.sub(r'\s+', '', text)
    logger.debug(f"normalize_for_deduplication: Removed whitespace, text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    result = text.lower()
    logger.debug(f"normalize_for_deduplication: Converted to lowercase, returning='{result[:50]}{'...' if len(result) > 50 else ''}'")
    return result

def generate_id(combined):
    logger.debug(f"generate_id called with combined='{combined}'")
    id_hash = hashlib.md5(combined.encode()).hexdigest()[:16]
    logger.debug(f"generate_id: Generated id='{id_hash}'")
    return id_hash

def split_paragraphs(text, max_length=200):
    logger.debug(f"split_paragraphs called with text='{text[:50]}{'...' if len(text) > 50 else ''}', max_length={max_length}")
    paragraphs = text.split('\n\n')
    logger.debug(f"split_paragraphs: Split into {len(paragraphs)} paragraphs")
    result = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            logger.debug("split_paragraphs: Skipping empty paragraph")
            continue
        logger.debug(f"split_paragraphs: Processing paragraph='{para[:50]}{'...' if len(para) > 50 else ''}'")
        while len(para) > max_length:
            split_point = para.rfind(' ', 0, max_length)
            if split_point == -1:
                split_point = para.rfind('.', 0, max_length)
            if split_point == -1:
                split_point = max_length
            logger.debug(f"split_paragraphs: Splitting at position {split_point}, chunk='{para[:split_point][:50]}{'...' if len(para[:split_point]) > 50 else ''}'")
            result.append(para[:split_point].strip())
            para = para[split_point:].strip()
        if para:
            logger.debug(f"split_paragraphs: Adding final chunk='{para[:50]}{'...' if len(para) > 50 else ''}'")
            result.append(para)
    final_text = '\n\n'.join(result)
    logger.debug(f"split_paragraphs: Returning text with {len(result)} paragraphs, length={len(final_text)}")
    return final_text

def save_company_to_wordpress(index, company_data, wp_headers, licensed, wp_urls):
    logger.debug(f"save_company_to_wordpress called with index={index}, company_data={json.dumps(company_data, indent=2)[:200]}..., licensed={licensed}")
    company_name = company_data.get("company_name", "")
    company_details = company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else "")
    company_logo = company_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    company_website = company_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else "")
    company_industry = company_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = company_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    company_type = company_data.get("company_type", UNLICENSED_MESSAGE if not licensed else "")
    company_address = company_data.get("company_address", UNLICENSED_MESSAGE if not licensed else "")
    logger.debug(f"save_company_to_wordpress: Extracted company fields: name='{company_name}', details='{company_details[:50]}...', logo='{company_logo}', website='{company_website}', industry='{company_industry}', founded='{company_founded}', type='{company_type}', address='{company_address}'")
    
    # Check for existing company with the same logo
    if company_logo and company_logo != UNLICENSED_MESSAGE:
        try:
            # Query WordPress for companies with the same logo
            response = requests.get(
                f"{wp_urls['WP_COMPANY_URL']}?meta_key=company_logo&meta_value={company_logo}",
                headers=wp_headers,
                timeout=15
            )
            logger.debug(f"save_company_to_wordpress: GET response for logo check status={response.status_code}, headers={response.headers}")
            response.raise_for_status()
            companies = response.json()
            if companies:
                # Found an existing company with the same logo
                company_id = companies[0].get('id')
                logger.info(f"save_company_to_wordpress: Found existing company with logo {company_logo}: ID {company_id}")
                return company_id, f"Company with logo {company_logo} already exists"
        except Exception as e:
            logger.error(f"save_company_to_wordpress: Failed to check for existing company with logo {company_logo}: {str(e)}")
            # Continue to save the company if the check fails
    
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
    logger.debug(f"save_company_to_wordpress: Prepared post_data={json.dumps(post_data, indent=2)[:200]}...")
    response = None
    try:
        response = requests.post(wp_urls["WP_SAVE_COMPANY_URL"], json=post_data, headers=wp_headers, timeout=15)
        logger.debug(f"save_company_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        if post['success']:
            logger.info(f"save_company_to_wordpress: Successfully saved company {company_name}: ID {post.get('id')}, Message {post.get('message')}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"save_company_to_wordpress: Company {company_name} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_company_to_wordpress: Failed to save company {company_name}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers, licensed, wp_urls):
    logger.debug(f"save_article_to_wordpress called with index={index}, job_data={json.dumps(job_data, indent=2)[:200]}..., company_id={company_id}, licensed={licensed}")
    job_title = job_data.get("job_title", "")
    job_description = job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else "")
    job_type = job_data.get("job_type", "")
    location = job_data.get("location", "Unknown")
    job_url = job_data.get("job_url", "")
    company_name = job_data.get("company_name", "")
    company_logo = job_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    environment = job_data.get("environment", UNLICENSED_MESSAGE if not licensed else "").lower()
    job_salary = job_data.get("job_salary", "")
    company_industry = job_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = job_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    logger.debug(f"save_article_to_wordpress: Extracted job fields: title='{job_title}', description='{job_description[:50]}...', type='{job_type}', location='{location}', url='{job_url}', company='{company_name}', logo='{company_logo}', environment='{environment}', salary='{job_salary}', industry='{company_industry}', founded='{company_founded}'")
    application = ''
    if '@' in job_data.get("description_application_info", ""):
        application = job_data.get("description_application_info", "")
    elif job_data.get("resolved_application_url", ""):
        application = job_data.get("resolved_application_url", "")
    else:
        application = job_data.get("application_url", "")
        if not application:
            logger.warning(f"save_article_to_wordpress: No valid application email or URL found for job {job_title}")
    logger.debug(f"save_article_to_wordpress: Selected application='{application}'")
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
    logger.info(f"save_article_to_wordpress: Final job post payload for {job_title}: {json.dumps(post_data, indent=2)[:200]}...")
    try:
        response = requests.post(wp_urls["WP_SAVE_JOB_URL"], json=post_data, headers=auth_headers, timeout=15)
        logger.debug(f"save_article_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        if post['success']:
            logger.info(f"save_article_to_wordpress: Successfully saved job {job_title}: ID {post.get('id')}, Message {post.get('message')}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"save_article_to_wordpress: Job {job_title} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_article_to_wordpress: Failed to save job {job_title}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def load_processed_ids():
    logger.debug(f"load_processed_ids called for file={PROCESSED_IDS_FILE}")
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(json.load(f))
            logger.info(f"load_processed_ids: Loaded {len(processed_ids)} processed job IDs from {PROCESSED_IDS_FILE}")
        else:
            logger.debug(f"load_processed_ids: File {PROCESSED_IDS_FILE} does not exist")
    except Exception as e:
        logger.error(f"load_processed_ids: Failed to load processed IDs from {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)
    logger.debug(f"load_processed_ids: Returning {len(processed_ids)} IDs")
    return processed_ids

def save_processed_ids(processed_ids):
    logger.debug(f"save_processed_ids called with {len(processed_ids)} IDs")
    try:
        with open(PROCESSED_IDS_FILE, "w") as f:
            json.dump(list(processed_ids), f)
        logger.info(f"save_processed_ids: Saved {len(processed_ids)} job IDs to {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"save_processed_ids: Failed to save processed IDs to {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)

def load_last_page():
    logger.debug(f"load_last_page called for file={LAST_PAGE_FILE}")
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                page = int(f.read().strip())
                logger.info(f"load_last_page: Loaded last processed page: {page}")
                return page
        logger.debug(f"load_last_page: File {LAST_PAGE_FILE} does not exist")
    except Exception as e:
        logger.error(f"load_last_page: Failed to load last page from {LAST_PAGE_FILE}: {str(e)}", exc_info=True)
    logger.debug("load_last_page: Returning default page 0")
    return 0

def save_last_page(page):
    logger.debug(f"save_last_page called with page={page}")
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"save_last_page: Saved last processed page: {page} to {LAST_PAGE_FILE}")
    except Exception as e:
        logger.error(f"save_last_page: Failed to save last page to {LAST_PAGE_FILE}: {str(e)}", exc_info=True)

def crawl(auth_headers, processed_ids, licensed, country, keyword, wp_urls):
    logger.debug(f"crawl called with processed_ids_count={len(processed_ids)}, licensed={licensed}, country={country}, keyword={keyword}")
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10
    logger.debug(f"crawl: Starting from page {start_page}, scraping {pages_to_scrape} pages")

    for i in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords={keyword}&location={country}&start={i * 25}'
        logger.info(f"crawl: Fetching job search page: {url}")
        time.sleep(random.uniform(5, 10))
        try:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            logger.debug(f"crawl: Sending GET request to {url} with headers={headers}")
            response = session.get(url, headers=headers, timeout=15)
            logger.debug(f"crawl: GET response status={response.status_code}, headers={response.headers}")
            response.raise_for_status()
            if "login" in response.url or "challenge" in response.url:
                logger.error(f"crawl: Login or CAPTCHA detected at {response.url}, stopping crawl")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list > li a")
            urls = [a['href'] for a in job_list if a.get('href')]
            logger.info(f"crawl: Found {len(urls)} job URLs on page {i}: {urls}")
            if not urls:
                logger.warning(f"crawl: No job URLs found on page {i}. Possible selector issue or no jobs available.")

            for index, job_url in enumerate(urls):
                logger.debug(f"crawl: Processing job {index + 1}/{len(urls)}: {job_url}")
                job_data = scrape_job_details(job_url, licensed)
                if not job_data:
                    logger.error(f"crawl: No data scraped for job: {job_url}")
                    print(f"Job (URL: {job_url}) failed to scrape: No data returned")
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
                    "job_description": job_data[6],
                    "job_url": job_url,
                    "application_url": job_data[7],
                    "resolved_application_url": job_data[8],
                    "description_application_info": job_data[9],
                    "company_website_url": job_data[10],
                    "company_details": job_data[11],
                    "company_industry": job_data[12],
                    "company_founded": job_data[13],
                    "company_type": job_data[14],
                    "company_address": job_data[15],
                    "job_type": job_data[16],
                    "job_salary": job_data[17]
                }
                normalized_title = normalize_for_deduplication(job_dict["job_title"])
                normalized_company = normalize_for_deduplication(job_dict["company_name"])
                job_id = generate_id(normalized_title + normalized_company)
                if job_id in processed_ids:
                    logger.info(f"crawl: Job {job_id} already processed, skipping")
                    continue
                company_data = {
                    "company_name": job_dict["company_name"],
                    "company_details": job_dict["company_details"],
                    "company_logo": job_dict["company_logo"],
                    "company_website_url": job_dict["company_website_url"],
                    "company_industry": job_dict["company_industry"],
                    "company_founded": job_dict["company_founded"],
                    "company_type": job_dict["company_type"],
                    "company_address": job_dict["company_address"]
                }
                company_id, company_message = save_company_to_wordpress(index, company_data, auth_headers, licensed, wp_urls)
                if company_id:
                    job_id_saved, job_message = save_article_to_wordpress(index, job_dict, company_id, auth_headers, licensed, wp_urls)
                    if job_id_saved:
                        processed_ids.add(job_id)
                        success_count += 1
                    else:
                        failure_count += 1
                else:
                    failure_count += 1
                total_jobs += 1
            save_last_page(i + 1)
        except Exception as e:
            logger.error(f"crawl: Error processing page {i}: {str(e)}", exc_info=True)
            break
    save_processed_ids(processed_ids)
    logger.info(f"crawl: Crawling complete. Total jobs: {total_jobs}, Success: {success_count}, Failures: {failure_count}")

def scrape_job_details(job_url, licensed):
    logger.debug(f"scrape_job_details called with job_url={job_url}, licensed={licensed}")
    try:
        time.sleep(random.uniform(5, 10))
        response = requests.get(job_url, headers=headers, timeout=15)
        logger.debug(f"scrape_job_details: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        job_title = soup.select_one(".top-card-layout__title").get_text(strip=True) if soup.select_one(".top-card-layout__title") else ""
        company_logo = soup.select_one(".topcard__org-name-link img")['src'] if soup.select_one(".topcard__org-name-link img") else ""
        company_name = soup.select_one(".topcard__org-name-link").get_text(strip=True) if soup.select_one(".topcard__org-name-link") else ""
        company_url = soup.select_one(".topcard__org-name-link")['href'] if soup.select_one(".topcard__org-name-link") else ""
        location = soup.select_one(".topcard__flavor-row .topcard__flavor--bullet").get_text(strip=True) if soup.select_one(".topcard__flavor-row .topcard__flavor--bullet") else ""
        environment = soup.select_one(".topcard__flavor-row .topcard__flavor--metadata").get_text(strip=True) if soup.select_one(".topcard__flavor-row .topcard__flavor--metadata") else ""
        job_description = soup.select_one(".description__text").get_text(strip=True) if soup.select_one(".description__text") else ""
        job_description = split_paragraphs(job_description)
        application_url = soup.select_one(".apply-button")['href'] if soup.select_one(".apply-button") else ""
        resolved_application_url = ""
        if application_url:
            try:
                app_response = requests.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                resolved_application_url = app_response.url
            except Exception as e:
                logger.error(f"scrape_job_details: Failed to resolve application URL {application_url}: {str(e)}")
        description_application_info = re.search(r'apply\s+to\s+([^\s@]+@[^\s@]+\.[^\s@]+)', job_description, re.IGNORECASE).group(1) if re.search(r'apply\s+to\s+([^\s@]+@[^\s@]+\.[^\s@]+)', job_description, re.IGNORECASE) else ""
        company_website_url = ""
        company_details = ""
        company_industry = ""
        company_founded = ""
        company_type = ""
        company_address = ""
        if licensed and company_url:
            try:
                company_response = requests.get(company_url, headers=headers, timeout=15)
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                company_website_url = company_soup.select_one(".org-top-card-summary__website a")['href'] if company_soup.select_one(".org-top-card-summary__website a") else ""
                company_details = company_soup.select_one(".about-us__description").get_text(strip=True) if company_soup.select_one(".about-us__description") else ""
                company_industry = company_soup.select_one(".org-top-card-summary__industry").get_text(strip=True) if company_soup.select_one(".org-top-card-summary__industry") else ""
                company_founded = company_soup.select_one(".org-top-card-summary__founded").get_text(strip=True) if company_soup.select_one(".org-top-card-summary__founded") else ""
                company_type = company_soup.select_one(".org-top-card-summary__company-type").get_text(strip=True) if company_soup.select_one(".org-top-card-summary__company-type") else ""
                company_address = company_soup.select_one(".org-top-card-summary__headquarters").get_text(strip=True) if company_soup.select_one(".org-top-card-summary__headquarters") else ""
            except Exception as e:
                logger.error(f"scrape_job_details: Failed to scrape company details from {company_url}: {str(e)}")
        job_type = soup.select_one(".description__job-criteria-item:nth-child(1) .description__job-criteria-text").get_text(strip=True) if soup.select_one(".description__job-criteria-item:nth-child(1) .description__job-criteria-text") else ""
        job_salary = soup.select_one(".description__job-criteria-item:nth-child(2) .description__job-criteria-text").get_text(strip=True) if soup.select_one(".description__job-criteria-item:nth-child(2) .description__job-criteria-text") else ""
        return [
            job_title, company_logo, company_name, company_url, location, environment, job_description,
            application_url, resolved_application_url, description_application_info, company_website_url,
            company_details, company_industry, company_founded, company_type, company_address, job_type, job_salary
        ]
    except Exception as e:
        logger.error(f"scrape_job_details: Failed to scrape job details from {job_url}: {str(e)}", exc_info=True)
        return None

def main():
    logger.debug("main: Starting execution")
    # Check license key, country, keyword, site_url, wp_username, wp_app_password from command-line arguments
    license_key = sys.argv[1] if len(sys.argv) > 1 else ""
    country = sys.argv[2] if len(sys.argv) > 2 else "Unknown"
    keyword = sys.argv[3] if len(sys.argv) > 3 else ""
    site_url = sys.argv[4] if len(sys.argv) > 4 else ""
    wp_username = sys.argv[5] if len(sys.argv) > 5 else ""
    wp_app_password = sys.argv[6] if len(sys.argv) > 6 else ""
    logger.debug(f"main: Parameters - License: {'*' * len(license_key) if license_key else 'None'}, Country: {country}, Keyword: {keyword}, Site URL: {site_url}, WP Username: {wp_username}, WP App Password: {'*' * len(wp_app_password) if wp_app_password else 'None'}")
    
    # Validate site_url
    if not site_url or not site_url.startswith(('http://', 'https://')):
        logger.error("main: Invalid or missing site_url. Please provide a valid WordPress site URL (e.g., https://example.com)")
        print("Error: Invalid or missing site_url. Please provide a valid WordPress site URL (e.g., https://example.com)")
        return
    
    licensed = license_key == VALID_LICENSE_KEY
    if not licensed:
        logger.warning("main: No valid license key provided. Scraping limited data.")
        print("Warning: No valid license key provided. Only basic job data (title, company name, location, job type, job URL) will be scraped.")
    else:
        logger.info("main: Valid license key provided. Scraping full job data.")
        print("Valid license key provided. Scraping full job data.")
    
    # Get WordPress API URLs using site_url
    wp_urls = get_wp_urls(site_url)
    logger.debug(f"main: WordPress endpoints: {wp_urls}")

    # Prepare authentication headers for credentials request
    if wp_username and wp_app_password:
        auth_string = f"{wp_username}:{wp_app_password}"
        logger.debug(f"main: Constructing auth string with WP_USERNAME={wp_username}, WP_APP_PASSWORD={'*' * len(wp_app_password)}")
        auth = base64.b64encode(auth_string.encode()).decode()
        auth_headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json"
        }
    else:
        auth_headers = headers
        logger.warning("main: No WP username or app password provided; attempting unauthenticated request to credentials endpoint")

    # Fetch WordPress credentials from plugin endpoint
    try:
        response = requests.get(wp_urls["WP_CREDENTIALS_URL"], headers=auth_headers, timeout=15)
        logger.debug(f"main: GET response for credentials status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        credentials = response.json()
        if not credentials.get('success', False):
            logger.warning(f"main: Credentials endpoint returned failure: {credentials.get('message', 'Unknown error')}")
            print(f"Warning: Credentials endpoint returned failure: {credentials.get('message', 'Unknown error')}. Using command-line credentials.")
        else:
            wp_username = credentials.get('wp_username', wp_username)
            wp_app_password = credentials.get('wp_app_password', wp_app_password)
            logger.debug(f"main: Fetched WP Username={wp_username}, WP App Password={'*' * len(wp_app_password)}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"main: Endpoint not found. Check plugin activation and namespace.")
            print("Error: Credentials endpoint not found (404). Ensure the plugin is activated and the namespace is 'mimus_job_fetcher/v1'.")
        else:
            logger.error(f"main: Failed to fetch credentials: {str(e)}", exc_info=True)
            print(f"Error: Failed to fetch credentials: {str(e)}")
        return  # Or continue with command-line creds
    except Exception as e:
        logger.error(f"main: Failed to fetch credentials from {wp_urls['WP_CREDENTIALS_URL']}: {str(e)}", exc_info=True)
        print(f"Error: Failed to fetch WordPress credentials from {wp_urls['WP_CREDENTIALS_URL']}. Check site URL, credentials, and server configuration.")
        return

    # Prepare headers for subsequent requests
    auth_string = f"{wp_username}:{wp_app_password}"
    logger.debug(f"main: Constructing auth string with WP_USERNAME={wp_username}, WP_APP_PASSWORD={'*' * len(wp_app_password)}")
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    logger.debug(f"main: Prepared WordPress headers: {wp_headers}")

    processed_ids = load_processed_ids()
    logger.debug(f"main: Loaded {len(processed_ids)} processed job IDs")
    crawl(auth_headers=wp_headers, processed_ids=processed_ids, licensed=licensed, country=country, keyword=keyword, wp_urls=wp_urls)
    logger.debug("main: Completed execution")

if __name__ == "__main__":
    main()
