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

# Configure logging for verbose output
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.StreamHandler(),
    logging.FileHandler('fetcher.log')
])
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Get environment variables
logger.debug("Loading environment variables")
WP_SITE_URL = os.getenv('WP_SITE_URL')
WP_USERNAME = os.getenv('WP_USERNAME')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
COUNTRY = os.getenv('COUNTRY')
KEYWORD = os.getenv('KEYWORD', '')
FETCHER_TOKEN = os.getenv('FETCHER_TOKEN', '')
logger.debug(f"Environment variables: WP_SITE_URL={WP_SITE_URL}, WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'***' if WP_APP_PASSWORD else None}, COUNTRY={COUNTRY}, KEYWORD={KEYWORD}, FETCHER_TOKEN={'***' if FETCHER_TOKEN else None}")

# Constants for WordPress
WP_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job-listings"
WP_COMPANY_URL = f"{WP_SITE_URL}/wp-json/wp/v2/company"
WP_MEDIA_URL = f"{WP_SITE_URL}/wp-json/wp/v2/media"
WP_JOB_TYPE_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_type"
WP_JOB_REGION_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_region"
WP_SAVE_COMPANY_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-company"
WP_SAVE_JOB_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-job"
WP_FETCHER_STATUS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-status"
WP_CREDENTIALS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-credentials"
PROCESSED_IDS_FILE = "processed_job_ids.csv"
LAST_PAGE_FILE = "last_processed_page.txt"
EXPECTED_LICENSE_KEY = 'A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5'

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
logger.debug(f"WordPress endpoints: WP_URL={WP_URL}, WP_COMPANY_URL={WP_COMPANY_URL}, WP_MEDIA_URL={WP_MEDIA_URL}, WP_SAVE_COMPANY_URL={WP_SAVE_COMPANY_URL}, WP_SAVE_JOB_URL={WP_SAVE_JOB_URL}")

def fetch_credentials():
    """Fetch WordPress credentials and license key from the REST API if not provided in environment."""
    global WP_USERNAME, WP_APP_PASSWORD, LICENSE_KEY
    logger.debug("Attempting to fetch WordPress credentials and license key")
    if WP_USERNAME and WP_APP_PASSWORD:
        logger.info("Credentials provided via environment variables")
        LICENSE_KEY = os.getenv('LICENSE_KEY', '')
        return True
    logger.info(f"Fetching credentials from {WP_CREDENTIALS_URL}")
    try:
        response = requests.get(WP_CREDENTIALS_URL, timeout=5, verify=False)
        logger.debug(f"Credentials request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Credentials response: {json.dumps(data, indent=2)[:200]}...")
        if not data.get('success'):
            logger.error(f"Failed to fetch credentials: {data.get('message', 'Unknown error')}")
            return False
        WP_USERNAME = data.get('wp_username')
        WP_APP_PASSWORD = data.get('wp_app_password')
        LICENSE_KEY = data.get('license_key', '')
        logger.debug(f"Fetched WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'***' if WP_APP_PASSWORD else None}, LICENSE_KEY={'***' if LICENSE_KEY else None}")
        if not WP_USERNAME or not WP_APP_PASSWORD:
            logger.error("Credentials fetched but empty or invalid")
            return False
        logger.info("Successfully fetched credentials from WordPress")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch credentials from {WP_CREDENTIALS_URL}: {str(e)}")
        return False

def is_license_valid():
    """Check if the license key is valid."""
    logger.debug(f"Validating license key: {'***' if LICENSE_KEY else None}")
    return LICENSE_KEY == EXPECTED_LICENSE_KEY

def check_fetcher_status(auth_headers):
    """Check the fetcher status from WordPress."""
    logger.debug(f"Checking fetcher status at {WP_FETCHER_STATUS_URL}")
    try:
        response = requests.get(WP_FETCHER_STATUS_URL, headers=auth_headers, timeout=5, verify=False)
        logger.debug(f"Fetcher status request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        status = response.json().get('status', 'stopped')
        logger.info(f"Fetcher status: {status}")
        return status
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check fetcher status: {str(e)}")
        return 'stopped'

def sanitize_text(text, is_url=False):
    logger.debug(f"Sanitizing text: {text[:50]}... (is_url={is_url})")
    if not text:
        logger.debug("Text is empty, returning empty string")
        return ''
    if is_url:
        text = text.strip()
        if not text.startswith(('http://', 'https://')):
            text = 'https://' + text
        logger.debug(f"Sanitized URL: {text}")
        return text
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
    text = re.sub(r'(\w)(\w)', r'\1 \2', text) if re.match(r'^\w+$', text) else text
    sanitized = ' '.join(text.split())
    logger.debug(f"Sanitized text: {sanitized[:50]}...")
    return sanitized

def normalize_for_deduplication(text):
    """Normalize text for deduplication by removing spaces, punctuation, and converting to lowercase."""
    logger.debug(f"Normalizing text for deduplication: {text[:50]}...")
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    normalized = text.lower()
    logger.debug(f"Normalized text: {normalized[:50]}...")
    return normalized

def generate_job_id(job_title, company_name):
    """Generate a unique job ID based on job title and company name."""
    logger.debug(f"Generating job ID for title={job_title[:30]}..., company={company_name}")
    combined = f"{job_title}_{company_name}"
    job_id = hashlib.md5(combined.encode()).hexdigest()[:16]
    logger.debug(f"Generated job ID: {job_id}")
    return job_id

def split_paragraphs(text, max_length=200):
    """Split large paragraphs into smaller ones, each up to max_length characters."""
    logger.debug(f"Splitting paragraphs for text (length={len(text)}): {text[:50]}...")
    paragraphs = text.split('\n\n')
    result = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            logger.debug("Skipping empty paragraph")
            continue
        while len(para) > max_length:
            split_point = para.rfind(' ', 0, max_length)
            if split_point == -1:
                split_point = para.rfind('.', 0, max_length)
            if split_point == -1:
                split_point = max_length
            result.append(para[:split_point].strip())
            logger.debug(f"Split paragraph: {para[:split_point].strip()[:50]}...")
            para = para[split_point:].strip()
        if para:
            result.append(para)
            logger.debug(f"Added paragraph: {para[:50]}...")
    final_text = '\n\n'.join(result)
    logger.debug(f"Final split text (length={len(final_text)}): {final_text[:50]}...")
    return final_text

def get_or_create_term(term_name, taxonomy, wp_url, auth_headers):
    logger.debug(f"Getting or creating term: {term_name} for taxonomy {taxonomy}")
    term_name = sanitize_text(term_name)
    if not term_name:
        logger.debug("Term name is empty, returning None")
        return None
    check_url = f"{wp_url}?search={term_name}"
    logger.debug(f"Checking term at URL: {check_url}")
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=5, verify=False)
        logger.debug(f"Term check request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        terms = response.json()
        for term in terms:
            if term['name'].lower() == term_name.lower():
                logger.info(f"Found existing {taxonomy} term: {term_name}, ID: {term['id']}")
                return term['id']
        post_data = {"name": term_name, "slug": term_name.lower().replace(' ', '-')}
        logger.debug(f"Creating new term with payload: {post_data}")
        response = requests.post(wp_url, json=post_data, headers=auth_headers, timeout=5, verify=False)
        logger.debug(f"Term creation request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        term = response.json()
        logger.info(f"Created new {taxonomy} term: {term_name}, ID: {term['id']}")
        return term['id']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get or create {taxonomy} term {term_name}: {str(e)}")
        return None

def check_existing_job(job_title, company_name, auth_headers):
    """Check if a job with the same title and company already exists on WordPress."""
    logger.debug(f"Checking for existing job: {job_title[:30]}... at {company_name}")
    check_url = f"{WP_URL}?search={job_title}&meta_key=_company_name&meta_value={company_name}"
    logger.debug(f"Checking job at URL: {check_url}")
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=5, verify=False)
        logger.debug(f"Existing job check status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        posts = response.json()
        if posts:
            logger.info(f"Found existing job: {job_title} at {company_name}, Post ID: {posts[0].get('id')}")
            return posts[0].get('id'), posts[0].get('link')
        logger.debug("No existing job found")
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check existing job {job_title} at {company_name}: {str(e)}")
        return None, None

def save_company_to_wordpress(index, company_data, wp_headers):
    logger.debug(f"Saving company (index={index}): {json.dumps(company_data, indent=2)[:200]}...")
    if check_fetcher_status(wp_headers) != 'running':
        logger.info("Fetcher stopped before saving company")
        return None, None
    has_valid_license = is_license_valid()
    company_name = company_data.get("company_name", "")
    company_details = company_data.get("company_details", "") if has_valid_license else ""
    company_logo = company_data.get("company_logo", "") if has_valid_license else ""
    company_website = company_data.get("company_website_url", "") if has_valid_license else ""
    company_industry = company_data.get("company_industry", "") if has_valid_license else ""
    company_founded = company_data.get("company_founded", "") if has_valid_license else ""
    company_type = company_data.get("company_type", "") if has_valid_license else ""
    company_address = company_data.get("company_address", "") if has_valid_license else ""
   
    company_id = hashlib.md5(company_name.encode()).hexdigest()[:16]
    logger.debug(f"Generated company ID: {company_id} for {company_name}")
   
    attachment_id = 0
    if has_valid_license and company_logo:
        logger.debug(f"Uploading company logo: {company_logo}")
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logger.debug(f"Logo request status: {logo_response.status_code}, Content-Type: {logo_response.headers.get('content-type')}")
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": wp_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            logger.debug(f"Uploading logo to {WP_MEDIA_URL} with headers: {logo_headers}")
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, verify=False)
            logger.debug(f"Media upload status: {media_response.status_code}, Response: {media_response.text[:200]}...")
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for {company_name}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"Failed to upload logo for {company_name}: {str(e)}")
    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": company_details,
        "featured_media": attachment_id,
        "company_website": sanitize_text(company_website, is_url=True),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_type": sanitize_text(company_type),
        "company_address": sanitize_text(company_address),
        "company_tagline": sanitize_text(company_details),
        "company_twitter": "",
        "company_video": ""
    }
    logger.debug(f"Company post payload: {json.dumps(post_data, indent=2)[:200]}...")
   
    response = None
    try:
        logger.debug(f"Sending company data to {WP_SAVE_COMPANY_URL}")
        response = requests.post(WP_SAVE_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15, verify=False)
        logger.debug(f"Company save request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        res = response.json()
        logger.debug(f"Company save response: {json.dumps(res, indent=2)[:200]}...")
        if res.get("success"):
            logger.info(f"Successfully saved company {company_name}: Company ID {company_id}")
            return company_id, f"{WP_SITE_URL}/wp-content/uploads/companies.json"
        elif res.get("message") == "Company already exists":
            logger.info(f"Found existing company {company_name}: Company ID {company_id}")
            return company_id, f"{WP_SITE_URL}/wp-content/uploads/companies.json"
        else:
            logger.error(f"Failed to save company {company_name}: {res}")
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}")
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers):
    logger.debug(f"Saving job (index={index}): {json.dumps(job_data, indent=2)[:200]}...")
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped before saving job")
        return None, None
    has_valid_license = is_license_valid()
    job_title = job_data.get("job_title", "")
    job_description = job_data.get("job_description", "") if has_valid_license else ""
    job_type = job_data.get("job_type", "")
    location = job_data.get("location", COUNTRY)
    job_url = job_data.get("job_url", "") if has_valid_license else ""
    company_name = job_data.get("company_name", "")
    company_logo = job_data.get("company_logo", "") if has_valid_license else ""
    environment = job_data.get("environment", "").lower() if has_valid_license else ""
    job_salary = job_data.get("job_salary", "") if has_valid_license else ""
    company_industry = job_data.get("company_industry", "") if has_valid_license else ""
    company_founded = job_data.get("company_founded", "") if has_valid_license else ""
   
    job_id = generate_job_id(job_title, company_name)
   
    application = ''
    if has_valid_license:
        if '@' in job_data.get("description_application_info", ""):
            application = job_data.get("description_application_info", "")
            logger.debug(f"Using application email from description: {application}")
        elif job_data.get("resolved_application_url", ""):
            application = job_data.get("resolved_application_url", "")
            logger.debug(f"Using resolved application URL: {application}")
        else:
            application = job_data.get("job_url", "")
            logger.debug(f"Using application URL: {application}")
            if not application:
                logger.warning(f"No valid application email or URL found for job {job_title}")
   
    attachment_id = 0
    if has_valid_license and company_logo:
        logger.debug(f"Uploading job logo: {company_logo}")
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logger.debug(f"Job logo request status: {logo_response.status_code}, Content-Type: {logo_response.headers.get('content-type')}")
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": auth_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo_job_{index}.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            logger.debug(f"Uploading job logo to {WP_MEDIA_URL} with headers: {logo_headers}")
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, verify=False)
            logger.debug(f"Job logo upload status: {media_response.status_code}, Response: {media_response.text[:200]}...")
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for job {job_title}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"Failed to upload logo for job {job_title}: {str(e)}")
   
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_description,
        "featured_media": attachment_id,
        "job_location": sanitize_text(location),
        "job_type": sanitize_text(job_type),
        "job_salary": sanitize_text(job_salary),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": str(company_id) if company_id else "",
        "company_name": sanitize_text(company_name),
        "company_website": sanitize_text(job_data.get("company_website_url", ""), is_url=True),
        "company_logo": str(attachment_id) if attachment_id else "",
        "company_tagline": sanitize_text(job_data.get("company_details", "")),
        "company_address": sanitize_text(job_data.get("company_address", "")),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_twitter": "",
        "company_video": ""
    }
   
    logger.info(f"Job post payload for {job_title}: {json.dumps(post_data, indent=2)[:200]}...")
   
    try:
        logger.debug(f"Sending job data to {WP_SAVE_JOB_URL}")
        response = requests.post(WP_SAVE_JOB_URL, json=post_data, headers=auth_headers, timeout=15, verify=False)
        logger.debug(f"Job save request status: {response.status_code}, Response: {response.text[:200]}...")
        response.raise_for_status()
        res = response.json()
        logger.debug(f"Job save response: {json.dumps(res, indent=2)[:200]}...")
        if res.get("success"):
            logger.info(f"Successfully saved job {job_title}: Job ID {job_id}")
            return job_id, f"{WP_SITE_URL}/wp-content/uploads/jobs.json"
        elif res.get("message") == "Job already exists":
            logger.info(f"Found existing job {job_title}: Job ID {job_id}")
            return job_id, f"{WP_SITE_URL}/wp-content/uploads/jobs.json"
        else:
            logger.error(f"Failed to save job {job_title}: {res}")
            return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}")
        return None, None

def load_processed_ids():
    """Load processed job IDs from file."""
    logger.debug(f"Loading processed job IDs from {PROCESSED_IDS_FILE}")
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(processed_ids)} processed job IDs from {PROCESSED_IDS_FILE}")
        else:
            logger.debug(f"No processed IDs file found at {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"Failed to load processed IDs from {PROCESSED_IDS_FILE}: {str(e)}")
    return processed_ids

def save_processed_id(job_id):
    """Append a single job ID to the processed IDs file."""
    logger.debug(f"Saving job ID {job_id} to {PROCESSED_IDS_FILE}")
    try:
        with open(PROCESSED_IDS_FILE, "a") as f:
            f.write(f"{job_id}\n")
        logger.info(f"Saved job ID {job_id} to {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save job ID {job_id} to {PROCESSED_IDS_FILE}: {str(e)}")

def load_last_page():
    """Load the last processed page number."""
    logger.debug(f"Loading last processed page from {LAST_PAGE_FILE}")
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                page = f.read().strip()
                return int(page) if page.isdigit() else 1
        logger.debug(f"No last page file found at {LAST_PAGE_FILE}, returning default page 1")
        return 1
    except Exception as e:
        logger.error(f"Failed to load last processed page from {LAST_PAGE_FILE}: {str(e)}")
        return 1

def save_last_page(page):
    """Save the last processed page number."""
    logger.debug(f"Saving last processed page {page} to {LAST_PAGE_FILE}")
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"Saved page {page} to {LAST_PAGE_FILE}")
    except Exception as e:
        logger.error(f"Failed to save last processed page to {LAST_PAGE_FILE}: {str(e)}")

def main():
    """Main function to run the job fetcher."""
    logger.info("Starting job fetcher")
    if not fetch_credentials():
        logger.error("Cannot proceed without valid WordPress credentials")
        return
    auth_headers = {
        "Authorization": "Basic " + base64.b64encode(f"{WP_USERNAME}:{WP_APP_PASSWORD}".encode()).decode(),
        "Content-Type": "application/json"
    }
    processed_ids = load_processed_ids()
    last_page = load_last_page()
    logger.info(f"Starting from page {last_page}")
    
    # Simulate job scraping (replace with actual scraping logic)
    job_data_list = [
        {
            "job_title": "Software Engineer",
            "company_name": "Tech Corp",
            "job_description": "Develop and maintain web applications.",
            "job_type": "Full-time",
            "location": "United States",
            "job_url": "https://techcorp.com/apply",
            "company_website_url": "https://techcorp.com",
            "company_logo": "https://techcorp.com/logo.png",
            "company_details": "Innovative tech company",
            "company_industry": "Technology",
            "company_founded": "2000",
            "company_address": "123 Tech Lane, CA"
        }
    ]
    
    for index, job_data in enumerate(job_data_list):
        logger.debug(f"Processing job {index + 1}: {job_data['job_title']}")
        job_id = generate_job_id(job_data["job_title"], job_data["company_name"])
        if job_id in processed_ids:
            logger.info(f"Skipping already processed job ID: {job_id}")
            continue
        company_id, _ = save_company_to_wordpress(index, job_data, auth_headers)
        if company_id:
            job_id, job_url = save_article_to_wordpress(index, job_data, company_id, auth_headers)
            if job_id:
                save_processed_id(job_id)
                logger.info(f"Processed job {job_id} successfully")
        else:
            logger.warning(f"Failed to save company for job {job_data['job_title']}")
        time.sleep(random.uniform(1, 3))  # Avoid rate limiting
    save_last_page(last_page + 1)
    logger.info("Job fetching completed")

if __name__ == "__main__":
    main()
