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

# Constants for WordPress (site_url will be fetched dynamically)
def get_wp_urls(site_url):
    return {
        "WP_URL": f"{site_url}/wp-json/wp/v2/job-listings",
        "WP_COMPANY_URL": f"{site_url}/wp-json/wp/v2/company",
        "WP_MEDIA_URL": f"{site_url}/wp-json/wp/v2/media",
        "WP_JOB_TYPE_URL": f"{site_url}/wp-json/wp/v2/job_listing_type",
        "WP_JOB_REGION_URL": f"{site_url}/wp-json/wp/v2/job_listing_region",
        "WP_SAVE_COMPANY_URL": f"{site_url}/wp-json/fetcher/v1/save-company",
        "WP_SAVE_JOB_URL": f"{site_url}/wp-json/fetcher/v1/save-job",
        "WP_FETCHER_STATUS_URL": f"{site_url}/wp-json/fetcher/v1/get-status",
        "WP_CREDENTIALS_URL": f"{site_url}/wp-json/fetcher/v1/get-credentials"
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
                logger.debug(f"crawl: Constructed job_dict={json.dumps(job_dict, indent=2)[:200]}...")

                job_title = job_dict.get("job_title", "Unknown Job")
                company_name = job_dict.get("company_name", "")

                job_id = generate_id(f"{job_title}_{company_name}")
                logger.debug(f"crawl: Generated job_id={job_id} for job_title='{job_title}', company_name='{company_name}'")

                if job_id in processed_ids:
                    logger.info(f"crawl: Skipping already processed job: {job_id} ({job_title} at {company_name})")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed.")
                    total_jobs += 1
                    continue

                if not company_name or company_name.lower() == "unknown":
                    logger.info(f"crawl: Skipping job with unknown company: {job_title} (ID: {job_id})")
                    print(f"Job '{job_title}' (ID: {job_id}) skipped - unknown company")
                    failure_count += 1
                    total_jobs += 1
                    continue

                total_jobs += 1

                company_id, company_message = save_company_to_wordpress(index, job_dict, auth_headers, licensed, wp_urls)
                logger.debug(f"crawl: Company save result: company_id={company_id}, company_message={company_message}")
                if company_id is None:
                    failure_count += 1
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed - company save error.")
                    continue
                job_post_id, job_message = save_article_to_wordpress(index, job_dict, company_id, auth_headers, licensed, wp_urls)
                logger.debug(f"crawl: Job save result: job_post_id={job_post_id}, job_message={job_message}")

                if job_post_id is not None:
                    processed_ids.add(job_id)
                    logger.info(f"crawl: Processed and saved job: {job_id} - {job_title} at {company_name}")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) saved successfully. Message: {job_message}")
                    success_count += 1
                else:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed to save. Check logs for details.")
                    failure_count += 1

            save_last_page(i + 1)

        except Exception as e:
            logger.error(f"crawl: Error fetching job search page: {url} - {str(e)}", exc_info=True)
            failure_count += 1
    save_processed_ids(processed_ids)
    logger.info(f"crawl: Completed. Total jobs processed: {total_jobs}, Successfully saved: {success_count}, Failed: {failure_count}")
    print("\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully saved: {success_count}")
    print(f"Failed to save or scrape: {failure_count}")

def scrape_job_details(job_url, licensed):
    logger.debug(f"scrape_job_details called with job_url={job_url}, licensed={licensed}")
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        logger.debug(f"scrape_job_details: Sending GET request to {job_url} with headers={headers}")
        response = session.get(job_url, headers=headers, timeout=15)
        logger.debug(f"scrape_job_details: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        logger.info(f"scrape_job_details: Scraped Job Title: {job_title}")
        company_logo = ''
        if licensed:
            company_logo_elem = soup.select_one("img.artdeco-entity-image.artdeco-entity-image--square-5")
            company_logo = company_logo_elem.get('src') if company_logo_elem and company_logo_elem.get('src') else ''
            if company_logo and 'media.licdn.com' in company_logo:
                # Remove query parameters
                company_logo = re.sub(r'\?.*$', '', company_logo)
                # Ensure the URL ends with .jpg
                if not company_logo.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                    company_logo = f"{company_logo}.jpg"
                # Validate the logo URL
                try:
                    logo_response = session.head(company_logo, headers=headers, timeout=5)
                    content_type = logo_response.headers.get('content-type', '')
                    if 'image' not in content_type.lower():
                        logger.warning(f"scrape_job_details: Logo URL {company_logo} is not an image (Content-Type: {content_type})")
                        company_logo = ''
                    else:
                        logger.info(f"scrape_job_details: Validated Company Logo URL: {company_logo}")
                except Exception as e:
                    logger.error(f"scrape_job_details: Failed to validate logo URL {company_logo}: {str(e)}")
                    company_logo = ''
            else:
                logger.warning(f"scrape_job_details: Invalid or missing logo URL: {company_logo}")
                company_logo = ''
            logger.info(f"scrape_job_details: Scraped Company Logo URL: {company_logo}")
        else:
            company_logo = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set company_logo={UNLICENSED_MESSAGE}")
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        logger.info(f"scrape_job_details: Scraped Company Name: {company_name}")
        company_url = ''
        if licensed:
            company_url_elem = soup.select_one(".topcard__org-name-link")
            company_url = company_url_elem['href'] if company_url_elem and company_url_elem.get('href') else ''
            if company_url:
                company_url = re.sub(r'\?.*$', '', company_url)
                logger.info(f"scrape_job_details: Scraped Company URL: {company_url}")
            else:
                logger.info(f"scrape_job_details: No Company URL found")
        else:
            company_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set company_url={UNLICENSED_MESSAGE}")
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Unknown'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f"scrape_job_details: Deduplicated location for {job_title}: {location}")
        environment = ''
        if licensed:
            env_element = soup.select(".topcard__flavor--metadata")
            for elem in env_element:
                text = elem.get_text().strip().lower()
                if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                    environment = elem.get_text().strip()
                    break
            logger.info(f"scrape_job_details: Scraped Environment: {environment}")
        else:
            environment = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set environment={UNLICENSED_MESSAGE}")
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        logger.info(f"scrape_job_details: Scraped Type: {job_type}")
        level = ''
        if licensed:
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            logger.info(f"scrape_job_details: Scraped Level: {level}")
        else:
            level = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set level={UNLICENSED_MESSAGE}")
        job_functions = ''
        if licensed:
            functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = functions_elem.get_text().strip() if functions_elem else ''
            logger.info(f"scrape_job_details: Scraped Job Functions: {job_functions}")
        else:
            job_functions = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_functions={UNLICENSED_MESSAGE}")
        industries = ''
        if licensed:
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            logger.info(f"scrape_job_details: Scraped Industries: {industries}")
        else:
            industries = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set industries={UNLICENSED_MESSAGE}")
        job_description = ''
        if licensed:
            description_elem = soup.select_one(".description__text")
            job_description = description_elem.get_text().strip() if description_elem else ''
            job_description = re.sub(r'\n+', '\n\n', job_description)
            job_description = split_paragraphs(job_description)
            logger.info(f"scrape_job_details: Scraped Job Description (length: {len(job_description)}): {job_description[:200]}...")
        else:
            job_description = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_description={UNLICENSED_MESSAGE}")
        application_url = ''
        resolved_application_url = ''
        description_application_info = ''
        resolved_application_info = ''
        final_application_email = ''
        final_application_url = ''
        if licensed:
            application_elem = soup.select_one("a.topcard__link")
            application_url = application_elem['href'] if application_elem and application_elem.get('href') else ''
            if application_url:
                application_url = re.sub(r'\?.*$', '', application_url)
                logger.info(f"scrape_job_details: Scraped Application URL: {application_url}")
            else:
                logger.info(f"scrape_job_details: No Application URL found")
            # Try to resolve the application URL
            if application_url:
                try:
                    app_response = session.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                    resolved_application_url = app_response.url
                    logger.info(f"scrape_job_details: Resolved Application URL: {resolved_application_url}")
                except Exception as e:
                    logger.error(f"scrape_job_details: Failed to resolve application URL {application_url}: {str(e)}")
                    resolved_application_url = application_url
            # Extract application info from description
            email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
            url_pattern = r'https?://[^\s]+'
            description_application_info = re.findall(email_pattern, job_description) or re.findall(url_pattern, job_description) or ''
            if description_application_info:
                description_application_info = description_application_info[0] if isinstance(description_application_info, list) else description_application_info
                logger.info(f"scrape_job_details: Extracted application info from description: {description_application_info}")
            # Resolve application info if it's a URL
            if description_application_info and re.match(url_pattern, description_application_info):
                try:
                    info_response = session.get(description_application_info, headers=headers, timeout=15, allow_redirects=True)
                    resolved_application_info = info_response.url
                    logger.info(f"scrape_job_details: Resolved application info URL: {resolved_application_info}")
                except Exception as e:
                    logger.error(f"scrape_job_details: Failed to resolve application info URL {description_application_info}: {str(e)}")
                    resolved_application_info = description_application_info
            # Determine final application email or URL
            if description_application_info and '@' in description_application_info:
                final_application_email = description_application_info
                logger.info(f"scrape_job_details: Final application email: {final_application_email}")
            elif resolved_application_info:
                final_application_url = resolved_application_info
                logger.info(f"scrape_job_details: Final application URL: {final_application_url}")
            elif resolved_application_url:
                final_application_url = resolved_application_url
                logger.info(f"scrape_job_details: Final application URL: {final_application_url}")
            elif application_url:
                final_application_url = application_url
                logger.info(f"scrape_job_details: Final application URL: {final_application_url}")
            else:
                logger.warning(f"scrape_job_details: No valid application info found")
        else:
            application_url = UNLICENSED_MESSAGE
            resolved_application_url = UNLICENSED_MESSAGE
            description_application_info = UNLICENSED_MESSAGE
            resolved_application_info = UNLICENSED_MESSAGE
            final_application_email = UNLICENSED_MESSAGE
            final_application_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set application fields to {UNLICENSED_MESSAGE}")
        company_details = ''
        company_website_url = ''
        company_industry = ''
        company_size = ''
        company_headquarters = ''
        company_type = ''
        company_founded = ''
        company_specialties = ''
        company_address = ''
        if licensed:
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                details_elem = company_soup.select_one("p.about-us__description")
                company_details = details_elem.get_text().strip() if details_elem else ''
                company_details = re.sub(r'\n+', '\n\n', company_details)
                company_details = split_paragraphs(company_details)
                logger.info(f"scrape_job_details: Scraped Company Details (length: {len(company_details)}): {company_details[:200]}...")
                website_elem = company_soup.select_one("a.org-top-card-summary__website")
                company_website_url = website_elem['href'] if website_elem and website_elem.get('href') else ''
                if company_website_url:
                    company_website_url = re.sub(r'\?.*$', '', company_website_url)
                    logger.info(f"scrape_job_details: Scraped Company Website: {company_website_url}")
                else:
                    logger.info(f"scrape_job_details: No Company Website found")
                industry_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(1)")
                company_industry = industry_elem.get_text().strip() if industry_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Industry: {company_industry}")
                size_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(2)")
                company_size = size_elem.get_text().strip() if size_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Size: {company_size}")
                headquarters_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(3)")
                company_headquarters = headquarters_elem.get_text().strip() if headquarters_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Headquarters: {company_headquarters}")
                type_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(4)")
                company_type = type_elem.get_text().strip() if type_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Type: {company_type}")
                founded_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(5)")
                company_founded = founded_elem.get_text().strip() if founded_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Founded: {company_founded}")
                specialties_elem = company_soup.select_one("dd.org-page-details-module__content:nth-of-type(6)")
                company_specialties = specialties_elem.get_text().strip() if specialties_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Specialties: {company_specialties}")
                address_div = company_soup.select_one("div.org-locations-module__list")
                if address_div:
                    address_items = address_div.select("div.org-locations-module__item")
                    primary_address = next((item.get_text().strip() for item in address_items if "Primary" in item.get_text()), None)
                    if primary_address:
                        company_address = primary_address.replace("Primary", "").strip()
                        logger.info(f"scrape_job_details: Scraped Primary Company Address: {company_address}")
                    else:
                        company_address = address_items[0].get_text().strip() if address_items else company_headquarters
                        logger.warning(f"scrape_job_details: No primary address found, using first address or headquarters: {company_address}")
                else:
                    company_address = company_headquarters
                    logger.warning(f"scrape_job_details: No address div found, using headquarters: {company_address}")
            except Exception as e:
                logger.error(f"scrape_job_details: Error fetching company page: {company_url} - {str(e)}", exc_info=True)
                company_details = ''
                company_website_url = ''
                company_industry = ''
                company_size = ''
                company_headquarters = ''
                company_type = ''
                company_founded = ''
                company_specialties = ''
                company_address = ''
        else:
            company_details = UNLICENSED_MESSAGE
            company_website_url = UNLICENSED_MESSAGE
            company_industry = UNLICENSED_MESSAGE
            company_size = UNLICENSED_MESSAGE
            company_headquarters = UNLICENSED_MESSAGE
            company_type = UNLICENSED_MESSAGE
            company_founded = UNLICENSED_MESSAGE
            company_specialties = UNLICENSED_MESSAGE
            company_address = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set company fields to {UNLICENSED_MESSAGE}")
        row = [
            job_title,
            company_logo,
            company_name,
            company_url,
            location,
            environment,
            job_type,
            level,
            job_functions,
            industries,
            job_description,
            job_url,
            company_details,
            company_website_url,
            company_industry,
            company_size,
            company_headquarters,
            company_type,
            company_founded,
            company_specialties,
            company_address,
            application_url,
            description_application_info,
            resolved_application_info,
            final_application_email,
            final_application_url,
            resolved_application_url
        ]
        logger.info(f"scrape_job_details: Full scraped row for job: {str(row)[:200]}...")
        return row
    except Exception as e:
        logger.error(f"scrape_job_details: Error in scrape_job_details for {job_url}: {str(e)}", exc_info=True)
        return None

def main():
    logger.debug("main: Starting execution")
    # Check license key, country, keyword, wp_username, wp_app_password from environment variables
    license_key = os.environ.get('LICENSE_KEY', '')
    country = os.environ.get('COUNTRY', 'Unknown')
    keyword = os.environ.get('KEYWORD', '')
    wp_username = os.environ.get('WP_USERNAME', '')
    wp_app_password = os.environ.get('WP_APP_PASSWORD', '')
    logger.debug(f"main: Parameters - License: {'*' * len(license_key) if license_key else 'None'}, Country: {country}, Keyword: {keyword}, WP Username: {wp_username}, WP App Password: {'*' * len(wp_app_password) if wp_app_password else 'None'}")

    licensed = license_key == VALID_LICENSE_KEY
    if not licensed:
        logger.warning("main: No valid license key provided. Scraping limited data.")
        print("Warning: No valid license key provided. Only basic job data (title, company name, location, job type, job URL) will be scraped.")
    else:
        logger.info("main: Valid license key provided. Scraping full job data.")
        print("Valid license key provided. Scraping full job data.")

    # Temporary site_url for fetching credentials (use a placeholder or default, will be updated)
    temp_site_url = 'https://mauritius.mimusjobs.com'  # Placeholder, will be replaced
    wp_urls = get_wp_urls(temp_site_url)
    
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

    # Fetch WordPress credentials and site_url from plugin endpoint
    try:
        response = requests.get(wp_urls["WP_CREDENTIALS_URL"], headers=auth_headers, timeout=15)
        logger.debug(f"main: GET response for credentials status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        credentials = response.json()
        site_url = credentials.get('site_url', '')
        wp_username = credentials.get('wp_username', wp_username)
        wp_app_password = credentials.get('wp_app_password', wp_app_password)
        logger.debug(f"main: Fetched site_url={site_url}, WP Username={wp_username}, WP App Password={'*' * len(wp_app_password)}")
    except Exception as e:
        logger.error(f"main: Failed to fetch credentials from {wp_urls['WP_CREDENTIALS_URL']}: {str(e)}", exc_info=True)
        print(f"Error: Failed to fetch WordPress credentials and site_url from {wp_urls['WP_CREDENTIALS_URL']}. Check credentials and server configuration.")
        return

    # Validate site_url
    if not site_url or not site_url.startswith(('http://', 'https://')):
        logger.error("main: Invalid or missing site_url from plugin input. Please provide a valid WordPress site URL in the plugin settings.")
        print("Error: Invalid or missing site_url from plugin input. Please provide a valid WordPress site URL in the plugin settings.")
        return

    # Update wp_urls with the fetched site_url
    wp_urls = get_wp_urls(site_url)
    logger.debug(f"main: Updated WordPress endpoints: {wp_urls}")

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
