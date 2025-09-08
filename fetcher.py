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

# Configure logging with verbose output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fetcher.log')
    ]
)
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}
logger.debug(f"Initialized HTTP headers: {headers}")

# Constants for WordPress
WP_URL = "https://mauritius.mimusjobs.com/wp-json/wp/v2/job-listings"
WP_COMPANY_URL = "https://mauritius.mimusjobs.com/wp-json/wp/v2/company"
WP_MEDIA_URL = "https://mauritius.mimusjobs.com/wp-json/wp/v2/media"
WP_JOB_TYPE_URL = "https://mauritius.mimusjobs.com/wp-json/wp/v2/job_listing_type"
WP_JOB_REGION_URL = "https://mauritius.mimusjobs.com/wp-json/wp/v2/job_listing_region"
WP_USERNAME = "mary"
WP_APP_PASSWORD = "Piab Mwog pfiq pdfK BOGH hDEy"
PROCESSED_IDS_FILE = "mauritius_processed_job_ids.csv"
LAST_PAGE_FILE = "last_processed_page.txt"
logger.debug(f"WordPress endpoints: WP_URL={WP_URL}, WP_COMPANY_URL={WP_COMPANY_URL}, WP_MEDIA_URL={WP_MEDIA_URL}, WP_JOB_TYPE_URL={WP_JOB_TYPE_URL}, WP_JOB_REGION_URL={WP_JOB_REGION_URL}")
logger.debug(f"Credentials: WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'*' * len(WP_APP_PASSWORD)}")
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

def generate_job_id(job_title, company_name):
    logger.debug(f"generate_job_id called with job_title='{job_title}', company_name='{company_name}'")
    combined = f"{job_title}_{company_name}"
    logger.debug(f"generate_job_id: Combined string='{combined}'")
    job_id = hashlib.md5(combined.encode()).hexdigest()[:16]
    logger.debug(f"generate_job_id: Generated job_id='{job_id}'")
    return job_id

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

def get_or_create_term(term_name, taxonomy, wp_url, auth_headers):
    logger.debug(f"get_or_create_term called with term_name='{term_name}', taxonomy='{taxonomy}', wp_url='{wp_url}'")
    term_name = sanitize_text(term_name)
    logger.debug(f"get_or_create_term: Sanitized term_name='{term_name}'")
    if not term_name:
        logger.debug("get_or_create_term: Empty term_name, returning None")
        return None
    check_url = f"{wp_url}?search={term_name}"
    logger.debug(f"get_or_create_term: Checking term at URL='{check_url}'")
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=10)
        logger.debug(f"get_or_create_term: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        terms = response.json()
        logger.debug(f"get_or_create_term: Found {len(terms)} terms")
        for term in terms:
            if term['name'].lower() == term_name.lower():
                logger.debug(f"get_or_create_term: Found existing term ID={term['id']}, name={term['name']}")
                return term['id']
        post_data = {"name": term_name, "slug": term_name.lower().replace(' ', '-')}
        logger.debug(f"get_or_create_term: Creating new term with data={post_data}")
        response = requests.post(wp_url, json=post_data, headers=auth_headers, timeout=10)
        logger.debug(f"get_or_create_term: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        term = response.json()
        logger.info(f"get_or_create_term: Created new {taxonomy} term: {term_name}, ID: {term['id']}")
        return term['id']
    except requests.exceptions.RequestException as e:
        logger.error(f"get_or_create_term: Failed to get or create {taxonomy} term {term_name}: {str(e)}", exc_info=True)
        return None

def check_existing_job(job_title, company_name, auth_headers):
    logger.debug(f"check_existing_job called with job_title='{job_title}', company_name='{company_name}'")
    check_url = f"{WP_URL}?search={job_title}&meta_key=_company_name&meta_value={company_name}"
    logger.debug(f"check_existing_job: Checking job at URL='{check_url}'")
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=10)
        logger.debug(f"check_existing_job: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        posts = response.json()
        logger.debug(f"check_existing_job: Found {len(posts)} existing posts")
        if posts:
            post = posts[0]
            logger.info(f"check_existing_job: Found existing job: {job_title} at {company_name}, Post ID: {post.get('id')}, URL: {post.get('link')}")
            return post.get('id'), post.get('link')
        logger.debug("check_existing_job: No existing job found")
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"check_existing_job: Failed to check existing job {job_title} at {company_name}: {str(e)}", exc_info=True)
        return None, None

def save_company_to_wordpress(index, company_data, wp_headers, licensed):
    logger.debug(f"save_company_to_wordpress called with index={index}, company_data={json.dumps(company_data, indent=2)[:200]}..., licensed={licensed}")
    company_name = company_data.get("company_name", "Unknown Company")  # Default if empty
    company_details = company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else "")
    company_logo = company_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    company_website = company_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else "")
    company_industry = company_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = company_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    company_type = company_data.get("company_type", UNLICENSED_MESSAGE if not licensed else "")
    company_address = company_data.get("company_address", UNLICENSED_MESSAGE if not licensed else "")
    logger.debug(f"save_company_to_wordpress: Extracted company fields: name='{company_name}', details='{company_details[:50]}...', logo='{company_logo}', website='{company_website}', industry='{company_industry}', founded='{company_founded}', type='{company_type}', address='{company_address}'")

    # Check if company already exists
    check_url = f"{WP_COMPANY_URL}?search={company_name}"
    logger.debug(f"save_company_to_wordpress: Checking existing company at URL='{check_url}'")
    try:
        response = requests.get(check_url, headers=wp_headers, timeout=10)
        logger.debug(f"save_company_to_wordpress: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        posts = response.json()
        logger.debug(f"save_company_to_wordpress: Found {len(posts)} existing companies")
        if posts:
            post = posts[0]
            logger.info(f"save_company_to_wordpress: Found existing company {company_name}: Post ID {post.get('id')}, URL {post.get('link')}")
            return post.get("id"), post.get("link")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_company_to_wordpress: Failed to check existing company {company_name}: {str(e)}", exc_info=True)

    attachment_id = 0
    if licensed and company_logo and company_logo != UNLICENSED_MESSAGE:
        logger.debug(f"save_company_to_wordpress: Attempting to upload logo URL='{company_logo}'")
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logger.debug(f"save_company_to_wordpress: Logo GET response status={logo_response.status_code}, headers={logo_response.headers}")
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": wp_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            logger.debug(f"save_company_to_wordpress: Uploading logo to {WP_MEDIA_URL} with headers={logo_headers}")
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content)
            logger.debug(f"save_company_to_wordpress: Media POST response status={media_response.status_code}, headers={media_response.headers}, body={media_response.text[:200]}")
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"save_company_to_wordpress: Uploaded logo for {company_name}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"save_company_to_wordpress: Failed to upload logo for {company_name}: {str(e)}", exc_info=True)

    post_data = {
        "title": company_name,
        "content": company_details,
        "status": "publish",
        "featured_media": attachment_id,
        "meta": {
            "_company_name": sanitize_text(company_name),
            "_company_logo": str(attachment_id) if attachment_id else "",
            "_company_website": sanitize_text(company_website, is_url=True),
            "_company_industry": sanitize_text(company_industry),
            "_company_founded": sanitize_text(company_founded),
            "_company_type": sanitize_text(company_type),
            "_company_address": sanitize_text(company_address),
            "_company_tagline": sanitize_text(company_details),
            "_company_twitter": "",
            "_company_video": ""
        }
    }
    logger.debug(f"save_company_to_wordpress: Prepared post_data={json.dumps(post_data, indent=2)[:200]}...")
    try:
        response = requests.post(WP_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15)
        logger.debug(f"save_company_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        logger.info(f"save_company_to_wordpress: Successfully posted company {company_name}: Post ID {post.get('id')}, URL {post.get('link')}")
        return post.get("id"), post.get("link")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_company_to_wordpress: Failed to post company {company_name}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers, licensed):
    logger.debug(f"save_article_to_wordpress called with index={index}, job_data={json.dumps(job_data, indent=2)[:200]}..., company_id={company_id}, licensed={licensed}")
    job_title = job_data.get("job_title", "Unknown Job")  # Default if empty
    job_description = job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else "")
    job_type = job_data.get("job_type", "")
    location = job_data.get("location", "Mauritius")
    job_url = job_data.get("job_url", "")
    company_name = job_data.get("company_name", "Unknown Company")  # Default if empty
    company_logo = job_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    environment = job_data.get("environment", UNLICENSED_MESSAGE if not licensed else "").lower()
    job_salary = job_data.get("job_salary", "")
    company_industry = job_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = job_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    logger.debug(f"save_article_to_wordpress: Extracted job fields: title='{job_title}', description='{job_description[:50]}...', type='{job_type}', location='{location}', url='{job_url}', company='{company_name}', logo='{company_logo}', environment='{environment}', salary='{job_salary}', industry='{company_industry}', founded='{company_founded}'")

    # Check if job already exists on WordPress
    existing_post_id, existing_post_url = check_existing_job(job_title, company_name, auth_headers)
    if existing_post_id:
        logger.info(f"save_article_to_wordpress: Skipping duplicate job: {job_title} at {company_name}, Post ID: {existing_post_id}, URL: {existing_post_url}")
        print(f"Job '{job_title}' at {company_name} skipped - already posted on WordPress. Post ID: {existing_post_id}, URL: {existing_post_url}")
        return existing_post_id, existing_post_url

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

    attachment_id = 0
    if licensed and company_logo and company_logo != UNLICENSED_MESSAGE:
        logger.debug(f"save_article_to_wordpress: Attempting to upload logo URL='{company_logo}'")
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logger.debug(f"save_article_to_wordpress: Logo GET response status={logo_response.status_code}, headers={logo_response.headers}")
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": auth_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo_job_{index}.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            logger.debug(f"save_article_to_wordpress: Uploading logo to {WP_MEDIA_URL} with headers={logo_headers}")
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content)
            logger.debug(f"save_article_to_wordpress: Media POST response status={media_response.status_code}, headers={media_response.headers}, body={media_response.text[:200]}")
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"save_article_to_wordpress: Uploaded logo for job {job_title}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"save_article_to_wordpress: Failed to upload logo for job {job_title}: {str(e)}", exc_info=True)

    post_data = {
        "title": sanitize_text(job_title),
        "content": job_description,
        "status": "publish",
        "featured_media": attachment_id,
        "meta": {
            "_job_title": sanitize_text(job_title),
            "_job_location": sanitize_text(location),
            "_job_type": sanitize_text(job_type),
            "_job_description": job_description,
            "_job_salary": sanitize_text(job_salary),
            "_application": sanitize_text(application, is_url=('@' not in application)),
            "_company_id": str(company_id) if company_id else "",
            "_company_name": sanitize_text(company_name),
            "_company_website": sanitize_text(job_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else ""), is_url=True),
            "_company_logo": str(attachment_id) if attachment_id else "",
            "_company_tagline": sanitize_text(job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else "")),
            "_company_address": sanitize_text(job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else "")),
            "_company_industry": sanitize_text(company_industry),
            "_company_founded": sanitize_text(company_founded),
            "_company_twitter": "",
            "_company_video": ""
        },
        "job_listing_type": [job_type_id] if (job_type_id := get_or_create_term(job_type, "job_type", WP_JOB_TYPE_URL, auth_headers)) else [],
        "job_listing_region": [job_region_id] if (job_region_id := get_or_create_term(location, "job_region", WP_JOB_REGION_URL, auth_headers)) else []
    }
    
    logger.info(f"save_article_to_wordpress: Final job post payload for {job_title}: {json.dumps(post_data, indent=2)[:200]}...")
    
    try:
        response = requests.post(WP_URL, json=post_data, headers=auth_headers, timeout=15)
        logger.debug(f"save_article_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        logger.info(f"save_article_to_wordpress: Successfully posted job {job_title}: Post ID {post.get('id')}, URL {post.get('link')}")
        return post.get("id"), post.get("link")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_article_to_wordpress: Failed to post job {job_title}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def load_processed_ids():
    logger.debug(f"load_processed_ids called for file={PROCESSED_IDS_FILE}")
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(line.strip() for line in f if line.strip())
            logger.info(f"load_processed_ids: Loaded {len(processed_ids)} processed job IDs from {PROCESSED_IDS_FILE}")
        else:
            logger.debug(f"load_processed_ids: File {PROCESSED_IDS_FILE} does not exist")
    except Exception as e:
        logger.error(f"load_processed_ids: Failed to load processed IDs from {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)
    logger.debug(f"load_processed_ids: Returning {len(processed_ids)} IDs")
    return processed_ids

def save_processed_id(job_id):
    logger.debug(f"save_processed_id called with job_id={job_id}")
    try:
        with open(PROCESSED_IDS_FILE, "a") as f:
            f.write(f"{job_id}\n")
        logger.info(f"save_processed_id: Saved job ID {job_id} to {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"save_processed_id: Failed to save job ID {job_id} to {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)

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

def crawl(auth_headers, processed_ids, licensed):
    logger.debug(f"crawl called with processed_ids_count={len(processed_ids)}, licensed={licensed}")
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10
    logger.debug(f"crawl: Starting from page {start_page}, scraping {pages_to_scrape} pages")
    
    for i in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords=&location=Mauritius&start={i * 25}'
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
            urls = [a['href'] for a in job_list if a.get('href') and '/jobs/view/' in a['href']]  # Filter only job view URLs
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
                company_name = job_dict.get("company_name", "Unknown Company")
                
                job_id = generate_job_id(job_title, company_name)
                logger.debug(f"crawl: Generated job_id={job_id} for job_title='{job_title}', company_name='{company_name}'")
                
                if job_id in processed_ids:
                    logger.info(f"crawl: Skipping already processed job: {job_id} ({job_title} at {company_name})")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed.")
                    total_jobs += 1
                    continue
                
                # Removed skip for unknown company
                
                total_jobs += 1
                
                company_id, company_url = save_company_to_wordpress(index, job_dict, auth_headers, licensed)
                logger.debug(f"crawl: Company save result: company_id={company_id}, company_url={company_url}")
                job_post_id, job_post_url = save_article_to_wordpress(index, job_dict, company_id, auth_headers, licensed)
                logger.debug(f"crawl: Job save result: job_post_id={job_post_id}, job_post_url={job_post_url}")
                
                if job_post_id:
                    processed_ids.add(job_id)
                    save_processed_id(job_id)
                    logger.info(f"crawl: Processed and saved job: {job_id} - {job_title} at {company_name}")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) successfully posted to WordPress. Post ID: {job_post_id}, URL {job_post_url}")
                    success_count += 1
                else:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed to post to WordPress. Check logs for details.")
                    failure_count += 1
            
            save_last_page(i + 1)
            
        except Exception as e:
            logger.error(f"crawl: Error fetching job search page: {url} - {str(e)}", exc_info=True)
            failure_count += 1
    
    logger.info(f"crawl: Completed. Total jobs processed: {total_jobs}, Successfully posted: {success_count}, Failed: {failure_count}")
    print("\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully posted: {success_count}")
    print(f"Failed to post or scrape: {failure_count}")

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
        job_title = job_title.get_text().strip() if job_title else 'Unknown Job'
        logger.info(f"scrape_job_details: Scraped Job Title: {job_title}")

        company_logo = ''
        if licensed:
            company_logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            company_logo = (company_logo_elem.get('data-delayed-url') or company_logo_elem.get('src') or '') if company_logo_elem else ''
            logger.info(f"scrape_job_details: Scraped Company Logo URL: {company_logo}")
        else:
            company_logo = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set company_logo={UNLICENSED_MESSAGE}")

        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else 'Unknown Company'
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
        location = location.get_text().strip() if location else 'Mauritius'
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
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
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
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                paragraphs = description_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"scrape_job_details: Raw paragraphs for {job_title}: {[sanitize_text(p.get_text().strip())[:50] for p in paragraphs if p.get_text().strip()]}")
                    for p in paragraphs:
                        para = sanitize_text(p.get_text().strip())
                        if not para:
                            logger.debug(f"scrape_job_details: Skipping empty paragraph for {job_title}")
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                            logger.debug(f"scrape_job_details: Added unique paragraph: {para[:50]}...")
                        elif norm_para:
                            logger.info(f"scrape_job_details: Removed duplicate paragraph for {job_title}: {para[:50]}...")
                    job_description = '\n\n'.join(unique_paragraphs)
                else:
                    raw_text = description_container.get_text(separator='\n').strip()
                    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"scrape_job_details: Raw text paragraphs for {job_title}: {[sanitize_text(para)[:50] for para in paragraphs]}")
                    for para in paragraphs:
                        para = sanitize_text(para)
                        if not para:
                            logger.debug(f"scrape_job_details: Skipping empty paragraph for {job_title}")
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                            logger.debug(f"scrape_job_details: Added unique paragraph: {para[:50]}...")
                        elif norm_para:
                            logger.info(f"scrape_job_details: Removed duplicate paragraph for {job_title}: {para[:50]}...")
                    job_description = '\n\n'.join(unique_paragraphs)
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
                logger.info(f'Scraped Job Description (length): {len(job_description)}, Paragraphs: {len(job_description.split(delimiter))}')
            else:
                logger.warning(f"scrape_job_details: No job description container found for {job_title}")
        else:
            job_description = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_description={UNLICENSED_MESSAGE}")

        description_application_info = ''
        description_application_url = ''
        if licensed and job_description and job_description != UNLICENSED_MESSAGE:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
                logger.info(f"scrape_job_details: Found email in job description: {description_application_info}")
            else:
                links = description_container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                        description_application_url = href
                        description_application_info = href
                        logger.info(f"scrape_job_details: Found application link in job description: {description_application_info}")
                        break
        else:
            description_application_info = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set description_application_info={UNLICENSED_MESSAGE}")

        application_url = ''
        if licensed:
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
            logger.info(f"scrape_job_details: Scraped Application URL: {application_url}")
        else:
            application_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set application_url={UNLICENSED_MESSAGE}")

        resolved_application_info = ''
        resolved_application_url = ''
        if licensed and application_url and application_url != UNLICENSED_MESSAGE:
            logger.debug(f"scrape_job_details: Following application URL: {application_url}")
            try:
                time.sleep(5)
                resp_app = session.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                logger.debug(f"scrape_job_details: Application URL GET response status={resp_app.status_code}, headers={resp_app.headers}, final_url={resp_app.url}")
                resolved_application_url = resp_app.url
                logger.info(f"scrape_job_details: Resolved Application URL: {resolved_application_url}")
                
                app_soup = BeautifulSoup(resp_app.text, 'html.parser')
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                emails = re.findall(email_pattern, resp_app.text)
                if emails:
                    resolved_application_info = emails[0]
                    logger.info(f"scrape_job_details: Found email in application page: {resolved_application_info}")
                else:
                    links = app_soup.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                            resolved_application_info = href
                            logger.info(f"scrape_job_details: Found application link in application page: {resolved_application_info}")
                            break
            except Exception as e:
                logger.error(f"scrape_job_details: Failed to follow application URL redirect: {str(e)}", exc_info=True)
                error_str = str(e)
                external_url_match = re.search(r'host=\'([^\']+)\'', error_str)
                if external_url_match:
                    external_url = external_url_match.group(1)
                    resolved_application_url = f"https://{external_url}"
                    logger.info(f"scrape_job_details: Extracted external URL from error for application: {resolved_application_url}")
                else:
                    resolved_application_url = description_application_url if description_application_url else application_url
                    logger.warning(f"scrape_job_details: No external URL found in error, using fallback: {resolved_application_url}")
        else:
            resolved_application_info = UNLICENSED_MESSAGE
            resolved_application_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set resolved_application_info={UNLICENSED_MESSAGE}, resolved_application_url={UNLICENSED_MESSAGE}")

        final_application_email = description_application_info if description_application_info and '@' in description_application_info else ''
        final_application_url = description_application_url if description_application_url else ''
        if licensed:
            if final_application_email and resolved_application_info and '@' in resolved_application_info:
                final_application_email = final_application_email if final_application_email == resolved_application_info else final_application_email
            elif resolved_application_info and '@' in resolved_application_info:
                final_application_email = final_application_email or resolved_application_info
                logger.debug(f"scrape_job_details: Set final_application_email={final_application_email}")

            if description_application_url and resolved_application_url:
                final_application_url = description_application_url if description_application_url == resolved_application_url else resolved_application_url
            elif resolved_application_url:
                final_application_url = resolved_application_url
            logger.debug(f"scrape_job_details: Set final_application_url={final_application_url}")
        else:
            final_application_email = UNLICENSED_MESSAGE
            final_application_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set final_application_email={UNLICENSED_MESSAGE}, final_application_url={UNLICENSED_MESSAGE}")

        company_details = ''
        company_website_url = ''
        company_industry = ''
        company_size = ''
        company_headquarters = ''
        company_type = ''
        company_founded = ''
        company_specialties = ''
        company_address = ''

        if licensed and company_url and company_url != UNLICENSED_MESSAGE:
            logger.info(f"scrape_job_details: Fetching company page: {company_url}")
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                logger.debug(f"scrape_job_details: Company page GET response status={company_response.status_code}, headers={company_response.headers}")
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')

                company_details_elem = company_soup.select_one("p.about-us__description") or company_soup.select_one("section.core-section-container > div > p")
                company_details = company_details_elem.get_text().strip() if company_details_elem else ''
                logger.info(f"scrape_job_details: Scraped Company Details: {company_details[:100] + '...' if company_details else ''}")

                company_website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a")
                company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                logger.info(f"scrape_job_details: Scraped Company Website URL: {company_website_url}")

                if 'linkedin.com/redir/redirect' in company_website_url:
                    parsed_url = urlparse(company_website_url)
                    query_params = parse_qs(parsed_url.query)
                    if 'url' in query_params:
                        company_website_url = unquote(query_params['url'][0])
                        logger.info(f"scrape_job_details: Extracted external company website from redirect: {company_website_url}")
                    else:
                        logger.warning(f"scrape_job_details: No 'url' param in LinkedIn redirect for {company_name}")

                if company_website_url and 'linkedin.com' not in company_website_url:
                    logger.debug(f"scrape_job_details: Following company website URL: {company_website_url}")
                    try:
                        time.sleep(5)
                        resp_company_web = session.get(company_website_url, headers=headers, timeout=15, allow_redirects=True)
                        logger.debug(f"scrape_job_details: Company website GET response status={resp_company_web.status_code}, headers={resp_company_web.headers}, final_url={resp_company_web.url}")
                        company_website_url = resp_company_web.url
                        logger.info(f"scrape_job_details: Resolved Company Website URL: {company_website_url}")
                    except Exception as e:
                        logger.error(f"scrape_job_details: Failed to resolve company website URL: {str(e)}", exc_info=True)
                        error_str = str(e)
                        external_url_match = re.search(r'host=\'([^\']+)\'', error_str)
                        if external_url_match:
                            external_url = external_url_match.group(1)
                            company_website_url = f"https://{external_url}"
                            logger.info(f"scrape_job_details: Extracted external URL from error for company website: {company_website_url}")
                        else:
                            logger.warning(f"scrape_job_details: No external URL found in error for {company_name}")
                            company_website_url = ''
                else:
                    description_elem = company_soup.select_one("p.about-us__description")
                    if description_elem:
                        description_text = description_elem.get_text()
                        url_pattern = r'https?://(?!www\.linkedin\.com)[^\s]+'
                        urls = re.findall(url_pattern, description_text)
                        if urls:
                            company_website_url = urls[0]
                            logger.info(f"scrape_job_details: Found company website in description: {company_website_url}")
                            try:
                                time.sleep(5)
                                resp_company_web = session.get(company_website_url, headers=headers, timeout=15, allow_redirects=True)
                                logger.debug(f"scrape_job_details: Company website description GET response status={resp_company_web.status_code}, headers={resp_company_web.headers}, final_url={resp_company_web.url}")
                                company_website_url = resp_company_web.url
                                logger.info(f"scrape_job_details: Resolved Company Website URL from description: {company_website_url}")
                            except Exception as e:
                                logger.error(f"scrape_job_details: Failed to resolve company website URL from description: {str(e)}", exc_info=True)
                                company_website_url = ''
                        else:
                            logger.warning(f"scrape_job_details: No valid company website URL found in description for {company_name}")
                            company_website_url = ''
                    else:
                        logger.warning(f"scrape_job_details: No company description found for {company_name}")
                        company_website_url = ''

                if company_website_url and 'linkedin.com' in company_website_url:
                    logger.warning(f"scrape_job_details: Skipping LinkedIn URL for company website: {company_website_url}")
                    company_website_url = ''

                def get_company_detail(label):
                    logger.debug(f"scrape_job_details: get_company_detail called with label={label}")
                    elements = company_soup.select("section.core-section-container.core-section-container--with-border > div > dl > div")
                    for elem in elements:
                        dt = elem.find("dt")
                        if dt and dt.get_text().strip().lower() == label.lower():
                            dd = elem.find("dd")
                            value = dd.get_text().strip() if dd else ''
                            logger.debug(f"scrape_job_details: Found {label}='{value}'")
                            return value
                    logger.debug(f"scrape_job_details: No {label} found")
                    return ''

                company_industry = get_company_detail("Industry")
                logger.info(f"scrape_job_details: Scraped Company Industry: {company_industry}")

                company_size = get_company_detail("Company size")
                logger.info(f"scrape_job_details: Scraped Company Size: {company_size}")

                company_headquarters = get_company_detail("Headquarters")
                logger.info(f"scrape_job_details: Scraped Company Headquarters: {company_headquarters}")

                company_type = get_company_detail("Type")
                logger.info(f"scrape_job_details: Scraped Company Type: {company_type}")

                company_founded = get_company_detail("Founded")
                logger.info(f"scrape_job_details: Scraped Company Founded: {company_founded}")

                company_specialties = get_company_detail("Specialties")
                logger.info(f"scrape_job_details: Scraped Company Specialties: {company_specialties}")

                company_address = company_soup.select_one("#address-0")
                company_address = company_address.get_text().strip() if company_address else company_headquarters
                logger.info(f"scrape_job_details: Scraped Company Address: {company_address}")
            except Exception as e:
                logger.error(f"scrape_job_details: Error fetching company page: {company_url} - {str(e)}", exc_info=True)
                company_details = UNLICENSED_MESSAGE
                company_website_url = UNLICENSED_MESSAGE
                company_industry = UNLICENSED_MESSAGE
                company_size = UNLICENSED_MESSAGE
                company_headquarters = UNLICENSED_MESSAGE
                company_type = UNLICENSED_MESSAGE
                company_founded = UNLICENSED_MESSAGE
                company_specialties = UNLICENSED_MESSAGE
                company_address = UNLICENSED_MESSAGE
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
    # Check license key from command-line argument
    license_key = sys.argv[1] if len(sys.argv) > 1 else ""
    logger.debug(f"main: License key provided: {'*' * len(license_key) if license_key else 'None'}")
    licensed = license_key == VALID_LICENSE_KEY
    
    if not licensed:
        logger.warning("main: No valid license key provided. Scraping limited data.")
        print("Warning: No valid license key provided. Only basic job data (title, company name, location, job type, job URL) will be scraped.")
    else:
        logger.info("main: Valid license key provided. Scraping full job data.")
        print("Valid license key provided. Scraping full job data.")

    auth_string = f"{WP_USERNAME}:{WP_APP_PASSWORD}"
    logger.debug(f"main: Constructing auth string with WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'*' * len(WP_APP_PASSWORD)}")
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    logger.debug(f"main: Prepared WordPress headers: {wp_headers}")
    
    processed_ids = load_processed_ids()
    logger.debug(f"main: Loaded {len(processed_ids)} processed job IDs")
    crawl(auth_headers=wp_headers, processed_ids=processed_ids, licensed=licensed)
    logger.debug("main: Completed execution")

if __name__ == "__main__":
    main()
