import requests
from requests import Session  # Add this line
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
import urllib.parse

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

# Get environment variables - FIXED: Use LICENSE_KEY instead of FETCHER_TOKEN
logger.debug("Loading environment variables")
WP_SITE_URL = os.getenv('WP_SITE_URL')
WP_USERNAME = os.getenv('WP_USERNAME')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
COUNTRY = os.getenv('COUNTRY')
KEYWORD = os.getenv('KEYWORD', '')  # Optional keyword
LICENSE_KEY = os.getenv('LICENSE_KEY', '')  # FIXED: License key for full data access

# URL encode country and keyword for LinkedIn search
COUNTRY_ENCODED = urllib.parse.quote(COUNTRY or 'Worldwide')
KEYWORD_ENCODED = urllib.parse.quote(KEYWORD) if KEYWORD else ''

logger.debug(f"Environment variables: WP_SITE_URL={WP_SITE_URL}, WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'***' if WP_APP_PASSWORD else None}, COUNTRY={COUNTRY}, KEYWORD={KEYWORD}, LICENSE_KEY={'***' if LICENSE_KEY else None}")
logger.debug(f"Encoded search params: COUNTRY_ENCODED={COUNTRY_ENCODED}, KEYWORD_ENCODED={KEYWORD_ENCODED}")

# Constants for WordPress
WP_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job-listings" if WP_SITE_URL else None
WP_COMPANY_URL = f"{WP_SITE_URL}/wp-json/wp/v2/company" if WP_SITE_URL else None
WP_MEDIA_URL = f"{WP_SITE_URL}/wp-json/wp/v2/media" if WP_SITE_URL else None
WP_JOB_TYPE_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_type" if WP_SITE_URL else None
WP_JOB_REGION_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job_listing_region" if WP_SITE_URL else None
WP_SAVE_COMPANY_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-company" if WP_SITE_URL else None
WP_SAVE_JOB_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-job" if WP_SITE_URL else None
WP_FETCHER_STATUS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-status" if WP_SITE_URL else None
WP_CREDENTIALS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-credentials" if WP_SITE_URL else None

PROCESSED_IDS_FILE = os.path.join("uploads", "processed_job_ids.json")
LAST_PAGE_FILE = os.path.join("uploads", "last_processed_page.txt")

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# FIXED: Valid license key for full data scraping
VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/job-fetcher'

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

logger.debug(f"WordPress URLs configured: SAVE_JOB={WP_SAVE_JOB_URL}, SAVE_COMPANY={WP_SAVE_COMPANY_URL}")
logger.debug(f"Job type mappings: {JOB_TYPE_MAPPING}")
logger.debug(f"French to English job type mappings: {FRENCH_TO_ENGLISH_JOB_TYPE}")

def validate_license_key(license_key):
    """Validate license key - exact match required"""
    if not license_key:
        logger.warning("No LICENSE_KEY provided")
        return False
    
    # Exact match validation
    if license_key.strip() == VALID_LICENSE_KEY:
        logger.info(f"✅ License key validated successfully: {VALID_LICENSE_KEY[:8]}...")
        return True
    
    logger.warning(f"❌ Invalid LICENSE_KEY. Expected: {VALID_LICENSE_KEY[:8]}... Got: {license_key[:8]}...")
    return False

def get_license_status():
    """Check license validity using LICENSE_KEY environment variable"""
    licensed = validate_license_key(LICENSE_KEY)
    
    if licensed:
        logger.info("✅ License validated successfully - Full data access enabled")
        print("✅ License: VALID (Full data access)")
        print(f"   Key: {VALID_LICENSE_KEY[:16]}...")
    else:
        logger.warning("⚠️ No valid license found - Basic data only")
        print("⚠️ License: INVALID (Basic data only)")
        print(f"   Get full license: https://mimusjobs.com/job-fetcher")
        print(f"   Enter in WP Settings: A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5")
    
    return licensed

def validate_environment():
    """Validate required environment variables"""
    missing = []
    if not WP_SITE_URL:
        missing.append("WP_SITE_URL")
    if not WP_USERNAME:
        missing.append("WP_USERNAME")
    if not WP_APP_PASSWORD:
        missing.append("WP_APP_PASSWORD")
    if not COUNTRY:
        missing.append("COUNTRY")
    
    # LICENSE_KEY is optional
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    logger.info("All required environment variables validated successfully")
    logger.info(f"Search configuration: Country='{COUNTRY}', Keyword='{KEYWORD or 'ALL JOBS'}'")
    logger.info(f"License key received: {'Yes' if LICENSE_KEY else 'No'}")
    return True

def build_search_url(page=0):
    """Build LinkedIn search URL with optional keyword"""
    base_url = 'https://www.linkedin.com/jobs/search'
    params = {
        'keywords': KEYWORD_ENCODED,
        'location': COUNTRY_ENCODED,
        'start': str(page * 25)
    }
    
    # Remove empty keyword param if no keyword
    if not KEYWORD:
        params.pop('keywords', None)
    
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    url = f"{base_url}?{query_string}"
    
    logger.debug(f"Built search URL for page {page}: {url}")
    return url

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
    text = ' '.join(text.split())
    return text

def normalize_for_deduplication(text):
    if not text:
        return ''
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text.lower()

def generate_id(combined):
    if not combined:
        return ''
    id_hash = hashlib.md5(combined.encode()).hexdigest()[:16]
    return id_hash

def split_paragraphs(text, max_length=200):
    if not text:
        return ''
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

def create_wp_auth_headers():
    """Create WordPress authentication headers"""
    if not WP_USERNAME or not WP_APP_PASSWORD:
        raise ValueError("WP_USERNAME and WP_APP_PASSWORD are required for WordPress authentication")
    
    auth_string = f"{WP_USERNAME}:{WP_APP_PASSWORD}"
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    logger.debug("Created WordPress auth headers successfully")
    return wp_headers

def save_company_to_wordpress(index, company_data, wp_headers, licensed):
    if not WP_SAVE_COMPANY_URL:
        logger.error("WP_SAVE_COMPANY_URL not configured")
        return None, "WordPress company endpoint not configured"
    
    company_name = company_data.get("company_name", "")
    if not company_name:
        return None, "No company name"
    
    company_id = generate_id(company_name)
    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_logo": sanitize_text(company_data.get("company_logo", ""), is_url=True),
        "company_website": sanitize_text(company_data.get("company_website_url", ""), is_url=True),
        "company_industry": sanitize_text(company_data.get("company_industry", "")),
        "company_founded": sanitize_text(company_data.get("company_founded", "")),
        "company_type": sanitize_text(company_data.get("company_type", "")),
        "company_address": sanitize_text(company_data.get("company_address", "")),
        "company_tagline": sanitize_text(company_data.get("company_details", "")),
        "company_twitter": "",
        "company_video": ""
    }
    
    try:
        response = requests.post(WP_SAVE_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15)
        response.raise_for_status()
        post = response.json()
        if post.get('success'):
            logger.info(f"Successfully saved company {company_name}")
            return post.get("id"), post.get("message", "Company saved successfully")
        else:
            logger.warning(f"Company {company_name} save failed: {post.get('message')}")
            return None, post.get("message", "Company save failed")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, f"Request failed: {str(e)}"

def save_article_to_wordpress(index, job_data, company_id, wp_headers, licensed):
    if not WP_SAVE_JOB_URL:
        logger.error("WP_SAVE_JOB_URL not configured")
        return None, "WordPress job endpoint not configured"
    
    job_title = job_data.get("job_title", "")
    if not job_title:
        return None, "No job title"
    
    company_name = job_data.get("company_name", "")
    job_id = generate_id(f"{job_title}_{company_name}")
    
    # Determine application method
    application = ''
    desc_app_info = job_data.get("description_application_info", "")
    if '@' in desc_app_info:
        application = desc_app_info
    elif job_data.get("resolved_application_url"):
        application = job_data.get("resolved_application_url")
    else:
        application = job_data.get("application_url", "")
    
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else ""),
        "job_type": sanitize_text(job_data.get("job_type", "")),
        "location": sanitize_text(job_data.get("location", COUNTRY or "Worldwide")),
        "job_url": sanitize_text(job_data.get("job_url", ""), is_url=True),
        "environment": sanitize_text(job_data.get("environment", "")),
        "job_salary": sanitize_text(job_data.get("job_salary", "")),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_website_url": sanitize_text(job_data.get("company_website_url", ""), is_url=True),
        "company_logo": sanitize_text(job_data.get("company_logo", ""), is_url=True),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_address": job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": sanitize_text(job_data.get("company_industry", "")),
        "company_founded": sanitize_text(job_data.get("company_founded", "")),
        "company_twitter": "",
        "company_video": ""
    }
    
    try:
        response = requests.post(WP_SAVE_JOB_URL, json=post_data, headers=wp_headers, timeout=15)
        response.raise_for_status()
        post = response.json()
        if post.get('success'):
            logger.info(f"Successfully saved job {job_title}")
            return post.get("id"), post.get("message", "Job saved successfully")
        else:
            logger.warning(f"Job {job_title} save failed: {post.get('message')}")
            return None, post.get("message", "Job save failed")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}")
        return None, f"Request failed: {str(e)}"

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

def scrape_job_details(job_url, licensed, session):
    """Scrape detailed job information from LinkedIn job page"""
    logger.debug(f"scrape_job_details called with job_url={job_url}, licensed={licensed}")
    try:
        logger.debug(f"scrape_job_details: Sending GET request to {job_url} with headers={headers}")
        response = session.get(job_url, headers=headers, timeout=15)
        logger.debug(f"scrape_job_details: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Job title
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        logger.info(f"scrape_job_details: Scraped Job Title: {job_title}")
        
        # Company logo (licensed only)
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
        
        # Company name and URL
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
        
        # Location
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Unknown'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f"scrape_job_details: Deduplicated location for {job_title}: {location}")
        
        # Environment (licensed only)
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
        
        # Job type
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        logger.info(f"scrape_job_details: Scraped Type: {job_type}")
        
        # Level (licensed only)
        level = ''
        if licensed:
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            logger.info(f"scrape_job_details: Scraped Level: {level}")
        else:
            level = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set level={UNLICENSED_MESSAGE}")
        
        # Job functions (licensed only)
        job_functions = ''
        if licensed:
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
            logger.info(f"scrape_job_details: Scraped Job Functions: {job_functions}")
        else:
            job_functions = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_functions={UNLICENSED_MESSAGE}")
        
        # Industries (licensed only)
        industries = ''
        if licensed:
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            logger.info(f"scrape_job_details: Scraped Industries: {industries}")
        else:
            industries = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set industries={UNLICENSED_MESSAGE}")
        
        # Job description (licensed only)
        job_description = ''
        description_container = None
        if licensed:
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                # Extract text using .get_text() with newline separator
                raw_text = description_container.get_text(separator='\n').strip()
                # Split into paragraphs and filter out unwanted phrases
                unwanted_phrases = [
                    "Never Miss a Job Update Again",
                    "Don't Keep! Kindly Share:",
                    "We have started building our professional LinkedIn page"
                ]
                paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                filtered_paragraphs = [
                    para for para in paragraphs
                    if not any(phrase.lower() in para.lower() for phrase in unwanted_phrases)
                ]
                seen = set()
                unique_paragraphs = []
                logger.debug(f"scrape_job_details: Filtered paragraphs for {job_title}: {[sanitize_text(para)[:50] for para in filtered_paragraphs]}")
                for para in filtered_paragraphs:
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
                # Clean up 'Show more/less' text and apply paragraph length limit
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
                delimiter = "\n\n"
                logger.info(f'Scraped Job Description (length): {len(job_description)}, Paragraphs: {job_description.count(delimiter) + 1}')
            else:
                logger.warning(f"scrape_job_details: No job description container found for {job_title}")
        else:
            job_description = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_description={UNLICENSED_MESSAGE}")
        
        # Application info from description
        description_application_info = ''
        description_application_url = ''
        if licensed and job_description and job_description != UNLICENSED_MESSAGE:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
                logger.info(f"scrape_job_details: Found email in job description: {description_application_info}")
            else:
                links = description_container.find_all('a', href=True) if description_container else []
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
        
        # Application URL (licensed only)
        application_url = ''
        if licensed:
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
            logger.info(f"scrape_job_details: Scraped Application URL: {application_url}")
        else:
            application_url = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set application_url={UNLICENSED_MESSAGE}")
        
        # Resolve application URL
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
        
        # Final application details
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
        
        # Company details (licensed only)
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
            if company_url and company_url != UNLICENSED_MESSAGE:
                logger.info(f"scrape_job_details: Fetching company page: {company_url}")
                try:
                    # Attempt to fetch company page with retry
                    for attempt in range(3):  # Try three times
                        try:
                            company_response = session.get(company_url, headers=headers, timeout=15)
                            logger.debug(f"scrape_job_details: Company page GET response status={company_response.status_code}, headers={company_response.headers}")
                            company_response.raise_for_status()
                            break
                        except requests.exceptions.RequestException as e:
                            logger.warning(f"scrape_job_details: Attempt {attempt + 1} failed for company page {company_url}: {str(e)}")
                            if attempt == 2:
                                raise
                            time.sleep(2)
                    company_soup = BeautifulSoup(company_response.text, 'html.parser')
                    
                    # Scrape company details using data-test-id
                    company_details_elem = company_soup.select_one("p[data-test-id='about-us__description']")
                    company_details = company_details_elem.get_text().strip() if company_details_elem else ''
                    logger.info(f"scrape_job_details: Scraped Company Details: {company_details[:100] + '...' if company_details else ''}")
                    
                    # Scrape website using data-test-id
                    website_div = company_soup.select_one("div[data-test-id='about-us__website']")
                    company_website_anchor = website_div.select_one("dd a") if website_div else None
                    company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                    logger.info(f"scrape_job_details: Scraped Company Website URL: {company_website_url}")
                    
                    # Handle LinkedIn redirect URLs
                    if 'linkedin.com/redir/redirect' in company_website_url:
                        parsed_url = urlparse(company_website_url)
                        query_params = parse_qs(parsed_url.query)
                        if 'url' in query_params:
                            company_website_url = unquote(query_params['url'][0])
                            logger.info(f"scrape_job_details: Extracted external company website from redirect: {company_website_url}")
                        else:
                            logger.warning(f"scrape_job_details: No 'url' param in LinkedIn redirect for {company_name}")
                    
                    # Resolve external company website
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
                        # Try to find website in company description
                        if company_details:
                            url_pattern = r'https?://(?!www\.linkedin\.com)[^\s]+'
                            urls = re.findall(url_pattern, company_details)
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
                    
                    # Skip LinkedIn URLs
                    if company_website_url and 'linkedin.com' in company_website_url:
                        logger.warning(f"scrape_job_details: Skipping LinkedIn URL for company website: {company_website_url}")
                        company_website_url = ''
                    
                    # Helper function to get company details
                    def get_company_detail(label):
                        logger.debug(f"scrape_job_details: get_company_detail called with label={label}")
                        div_selector = f"div[data-test-id='about-us__{label.lower()}']"
                        detail_div = company_soup.select_one(div_selector)
                        if detail_div:
                            dd = detail_div.select_one("dd")
                            value = dd.get_text().strip() if dd else ''
                            logger.debug(f"scrape_job_details: Found {label}='{value}'")
                            return value
                        logger.debug(f"scrape_job_details: No {label} found with selector {div_selector}")
                        return ''
                    
                    company_industry = get_company_detail("industry")
                    logger.info(f"scrape_job_details: Scraped Company Industry: {company_industry}")
                    company_size = get_company_detail("size")
                    logger.info(f"scrape_job_details: Scraped Company Size: {company_size}")
                    company_headquarters = get_company_detail("headquarters")
                    logger.info(f"scrape_job_details: Scraped Company Headquarters: {company_headquarters}")
                    company_type = get_company_detail("organizationType")
                    logger.info(f"scrape_job_details: Scraped Company Type: {company_type}")
                    company_founded = get_company_detail("foundedOn")
                    logger.info(f"scrape_job_details: Scraped Company Founded: {company_founded}")
                    company_specialties = get_company_detail("specialties")
                    logger.info(f"scrape_job_details: Scraped Company Specialties: {company_specialties}")
                    
                    # For address, get primary location
                    primary_li = company_soup.select_one("li span.tag-sm.tag-enabled")
                    if primary_li:
                        address_div = primary_li.find_next_sibling("div")
                        if address_div:
                            company_address = address_div.get_text(separator=', ').strip()
                            logger.info(f"scrape_job_details: Scraped Primary Company Address: {company_address}")
                        else:
                            company_address = company_headquarters
                            logger.warning(f"scrape_job_details: No address div found, using headquarters: {company_address}")
                    else:
                        company_address = company_headquarters
                        logger.warning(f"scrape_job_details: No primary location found, using headquarters: {company_address}")
                
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
        
        # Return structured row data
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

# Add this after other environment variables
LINKEDIN_COOKIES = os.getenv('LINKEDIN_COOKIES', '')  # LinkedIn cookies JSON string

# Update the headers section to include cookies if available
def create_linkedin_session():
    """Create session with LinkedIn cookies and ensure proper authentication"""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    if LINKEDIN_COOKIES:
        try:
            cookies = json.loads(LINKEDIN_COOKIES)
            for cookie in cookies:
                # Handle both dict format and list of dicts
                if isinstance(cookie, dict):
                    name = cookie.get('name')
                    value = cookie.get('value')
                    domain = cookie.get('domain', '.linkedin.com')
                else:
                    name = cookie.get('name') if hasattr(cookie, 'get') else str(cookie)
                    value = cookie.get('value') if hasattr(cookie, 'get') else str(cookie)
                    domain = '.linkedin.com'
                
                if name and value:
                    session.cookies.set(name, value, domain=domain)
            
            logger.info("✅ LinkedIn cookies loaded successfully")
            
            # Verify cookies are working by testing authenticated endpoint
            verify_cookies(session)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LinkedIn cookies JSON: {str(e)}")
            print("⚠️ LinkedIn cookies: INVALID JSON")
        except Exception as e:
            logger.error(f"Error loading LinkedIn cookies: {str(e)}")
            print("⚠️ LinkedIn cookies: ERROR")
    else:
        logger.warning("No LinkedIn cookies provided - some application URLs may require login")
        print("⚠️ LinkedIn cookies: NOT PROVIDED (Limited application URL access)")
    
    # Essential LinkedIn headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    })
    
    return session

def verify_cookies(session):
    """Verify LinkedIn cookies are valid by checking authenticated profile endpoint"""
    try:
        # Test with me endpoint or profile to verify authentication
        test_url = "https://www.linkedin.com/voyager/api/identity/profiles/me"
        response = session.get(test_url, timeout=10, allow_redirects=True)
        
        if response.status_code == 200:
            logger.info("✅ LinkedIn authentication verified - cookies working")
            print("✅ LinkedIn Login: AUTHENTICATED")
            return True
        elif "login" in response.url.lower() or response.status_code in [401, 403]:
            logger.warning("⚠️ LinkedIn cookies expired or invalid - redirecting to login")
            print("⚠️ LinkedIn Login: COOKIES EXPIRED")
            return False
        else:
            logger.info("ℹ️ Cookie verification inconclusive, proceeding with session")
            print("ℹ️ LinkedIn Login: SESSION READY")
            return True
            
    except Exception as e:
        logger.warning(f"Cookie verification failed: {str(e)}, proceeding anyway")
        print("ℹ️ LinkedIn Login: UNVERIFIED (proceeding)")
        return True

def create_linkedin_session_with_auth():
    """Create authenticated LinkedIn session with manual cookie handling"""
    session = requests.Session()  # Use requests.Session() instead of Session()
    
    # Configure retries for robustness
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Load LinkedIn cookies (Method 1 - manual JSON)
    loaded_cookies = False
    if LINKEDIN_COOKIES:
        try:
            cookies = json.loads(LINKEDIN_COOKIES)
            loaded_count = 0
            
            for cookie in cookies:
                if isinstance(cookie, dict):
                    name = cookie.get('name')
                    value = cookie.get('value')
                    
                    if name and value:
                        # Simple cookie setting - requests handles domain/path automatically
                        session.cookies.set(name, value)
                        loaded_count += 1
                        logger.debug(f"Loaded cookie: {name}")
                        if name == 'li_at':
                            loaded_cookies = True
            
            logger.info(f"✅ Successfully loaded {loaded_count} LinkedIn cookies")
            print(f"🍪 Loaded {loaded_count} cookies (li_at: {'✅' if loaded_cookies else '❌'})")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in LinkedIn cookies: {str(e)}")
            print("❌ Invalid cookie JSON format")
        except Exception as e:
            logger.error(f"❌ Cookie loading error: {str(e)}")
            print("❌ Cookie loading failed")
    else:
        logger.warning("⚠️ No LinkedIn cookies provided - limited access to public content only")
        print("⚠️ No cookies - public jobs only (no application URLs)")
    
    # Set realistic browser headers
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    })
    
    return session

def crawl(wp_headers, processed_ids, licensed):
    """Main crawling function with robust LinkedIn job extraction"""
    logger.info(f"🚀 Starting crawl: Country={COUNTRY}, Keyword={KEYWORD or 'ALL JOBS'}, Licensed={licensed}")
    logger.info(f"🍪 LinkedIn cookies: {'Yes' if LINKEDIN_COOKIES else 'No'}")
    
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page() or 0
    
    # Create authenticated session
    try:
        session = create_linkedin_session_with_auth()
    except Exception as e:
        logger.error(f"❌ Failed to create LinkedIn session: {str(e)}")
        print("❌ Session creation failed")
        return
    
    # Initial test request
    test_url = build_search_url(start_page)
    logger.info(f"🧪 Testing access: {test_url}")
    
    try:
        test_response = session.get(test_url, timeout=20)
        test_response.raise_for_status()
        
        if "login" in test_response.url.lower() or "challenge" in test_response.url.lower():
            logger.error("❌ LinkedIn login required - check cookies")
            print("❌ LOGIN REQUIRED - Update cookies")
            return
        
        # Save debug HTML for inspection
        debug_file = f"uploads/debug_page_{start_page}.html"
        os.makedirs("uploads", exist_ok=True)
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(test_response.text)
        logger.info(f"💾 Debug HTML saved: {debug_file}")
        
        print("✅ LinkedIn access confirmed")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Network test failed: {str(e)}")
        print("❌ Network error - check connection")
        return
    
    # Main crawling loop
    page_num = start_page
    empty_pages = 0
    max_empty_pages = 5
    
    while empty_pages < max_empty_pages:
        url = build_search_url(page_num)
        logger.info(f"📄 Fetching page {page_num}: {url}")
        
        # Random delay to avoid detection
        time.sleep(random.uniform(5, 12))
        
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            # Check for login/blocks
            if any(x in response.url.lower() for x in ["login", "challenge", "captcha"]):
                logger.error("🔒 Login wall detected - session expired")
                print("🔒 Session expired - get fresh cookies")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Multiple selector strategies for job cards
            job_selectors = [
                # Try most likely current selectors first
                'li[data-occludable-job-id]',
                '[data-occludable-job-id]',
                '.jobs-search-results__list li',
                '.job-search-card',
                '.reusable-search__result-container',
                # Fallback patterns
                '.jobs-search__results-list li',
                'ul.jobs-search-results__list li',
                # Broad search for job links
                'a[href*="/jobs/view/"]',
                'a[href*="/job/"]'
            ]
            
            job_urls = set()  # Use set to avoid duplicates
            
            # Try each selector strategy
            for selector in job_selectors:
                elements = soup.select(selector)
                logger.debug(f"Selector '{selector}': {len(elements)} elements")
                
                if elements:
                    logger.info(f"✓ Active selector: {selector} ({len(elements)} elements)")
                    
                    for elem in elements[:25]:  # Limit processing
                        # Extract job URLs from element
                        links = elem.select('a[href*="/jobs/view/"], a[href*="/job/"], a[data-job-id]')
                        
                        if not links:
                            links = elem.select('a[href*="jobs"], a[href*="/job"]')
                        
                        for link in links:
                            href = link.get('href', '')
                            if href and any(pattern in href for pattern in ['/jobs/view/', '/job/']):
                                # Make absolute URL
                                if href.startswith('/'):
                                    full_url = 'https://www.linkedin.com' + href
                                else:
                                    full_url = href
                                
                                job_urls.add(full_url)
            
            # Fallback: search all links for job patterns
            if not job_urls:
                logger.info("🔍 No job cards found, searching all links...")
                all_links = soup.find_all('a', href=True)
                for link in all_links:
                    href = link.get('href', '')
                    if any(pattern in href for pattern in ['/jobs/view/', '/job/', 'jobId=']):
                        if href.startswith('/'):
                            full_url = 'https://www.linkedin.com' + href
                        else:
                            full_url = href
                        if full_url not in job_urls:
                            job_urls.add(full_url)
                
                logger.info(f"🔍 Fallback found {len(job_urls)} potential job URLs")
            
            job_urls_list = list(job_urls)[:10]  # Reduced limit for testing
            logger.info(f"🎯 Processing {len(job_urls_list)} jobs on page {page_num}")
            
            if not job_urls_list:
                logger.warning(f"📭 No jobs found on page {page_num}")
                # Save current page HTML for debugging
                debug_page_file = f"uploads/debug_page_{page_num}.html"
                with open(debug_page_file, "w", encoding="utf-8") as f:
                    f.write(response.text)
                logger.info(f"💾 Empty page debug saved: {debug_page_file}")
                empty_pages += 1
                save_last_page(page_num + 1)
                page_num += 1
                continue
            
            empty_pages = 0  # Reset when jobs found
            
            # Process each job
            for idx, job_url in enumerate(job_urls_list):
                logger.info(f"🔄 Job {idx + 1}/{len(job_urls_list)}: {job_url}")
                
                try:
                    job_data = scrape_job_details(job_url, licensed, session)
                    if not job_data:
                        failure_count += 1
                        logger.warning(f"Failed to scrape job details: {job_url}")
                        continue
                    
                    # Map job data to dictionary
                    field_names = [
                        "job_title", "company_logo", "company_name", "company_url", "location",
                        "environment", "job_type", "level", "job_functions", "industries",
                        "job_description", "job_url", "company_details", "company_website_url",
                        "company_industry", "company_size", "company_headquarters", "company_type",
                        "company_founded", "company_specialties", "company_address", "application_url",
                        "description_application_info", "resolved_application_info", "final_application_email",
                        "final_application_url", "resolved_application_url"
                    ]
                    
                    # Fix: job_data should have 26 elements, but field_names has 26 - add job_url
                    if len(job_data) == 26:
                        job_data.append(job_url)  # Add job_url if missing
                    elif len(job_data) < len(field_names):
                        # Pad with empty strings
                        job_data.extend([''] * (len(field_names) - len(job_data)))
                    
                    job_dict = dict(zip(field_names, job_data[:len(field_names)]))
                    job_dict["job_salary"] = ""
                    
                    # Validate essential fields
                    job_title = job_dict.get("job_title", "").strip()
                    company_name = job_dict.get("company_name", "").strip()
                    
                    if not job_title or not company_name:
                        logger.warning(f"⏭️ Skipping incomplete job: '{job_title}' - '{company_name}'")
                        failure_count += 1
                        continue
                    
                    # Check for duplicates
                    job_id = generate_id(f"{job_title}_{company_name}")
                    if job_id in processed_ids:
                        logger.info(f"⏭️ Duplicate job skipped: {job_id}")
                        total_jobs += 1
                        continue
                    
                    total_jobs += 1
                    
                    # Save to WordPress
                    company_id, company_msg = save_company_to_wordpress(
                        idx, job_dict, wp_headers, licensed
                    )
                    if not company_id:
                        logger.error(f"💾 Company save failed: {company_msg}")
                        failure_count += 1
                        continue
                    
                    job_post_id, job_msg = save_article_to_wordpress(
                        idx, job_dict, company_id, wp_headers, licensed
                    )
                    
                    if job_post_id:
                        processed_ids.add(job_id)
                        success_count += 1
                        emoji = "🔓" if licensed else "🔒"
                        print(f"{emoji} ✅ {job_title[:60]}... at {company_name}")
                        logger.info(f"Saved job: {job_title} at {company_name}")
                    else:
                        failure_count += 1
                        logger.error(f"💾 Job save failed: {job_msg}")
                        print(f"❌ Failed: {job_title[:50]}... - {job_msg}")
                    
                    # Delay between jobs
                    time.sleep(random.uniform(3, 7))
                
                except Exception as e:
                    logger.error(f"❌ Job processing error: {str(e)}", exc_info=True)
                    failure_count += 1
                    continue
            
            # Save progress
            save_last_page(page_num + 1)
            page_num += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Network error on page {page_num}: {str(e)}")
            empty_pages += 1
            time.sleep(15)
            continue
        except Exception as e:
            logger.error(f"💥 Unexpected error page {page_num}: {str(e)}", exc_info=True)
            empty_pages += 1
            continue
    
    # Final cleanup
    save_processed_ids(processed_ids)
    
    logger.info(f"🏁 Crawl completed: Total={total_jobs}, Success={success_count}, Failed={failure_count}")
    print(f"\n{'='*50}")
    print(f"📊 CRAWL SUMMARY")
    print(f"{'='*50}")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully saved: {success_count}")
    print(f"Failed: {failure_count}")
    print(f"License: {'🔓 FULL ACCESS' if licensed else '🔒 BASIC ACCESS'}")
    print(f"Cookies: {'✅ ENABLED' if LINKEDIN_COOKIES else '⚠️ DISABLED'}")
    print(f"{'='*50}")

# Update main() function to show cookie status
def main():
    try:
        logger.info("Starting LinkedIn Job Fetcher")
        print("🚀 Starting LinkedIn Job Fetcher...")
        print(f"📍 Country: {COUNTRY}")
        print(f"🔍 Keyword: {KEYWORD or 'ALL JOBS'}")
        print(f"🍪 LinkedIn Cookies: {'Yes' if LINKEDIN_COOKIES else 'No'}")
        
        validate_environment()
        licensed = get_license_status()
        wp_headers = create_wp_auth_headers()
        processed_ids = load_processed_ids()
        print(f"📋 Found {len(processed_ids)} previously processed jobs")
        
        crawl(wp_headers, processed_ids, licensed)
        
        print("✅ Job fetcher completed!")
        logger.info("Job fetcher completed successfully")
        
    except ValueError as ve:
        logger.error(f"Configuration error: {str(ve)}")
        print(f"❌ Configuration error: {str(ve)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        print(f"❌ Fatal error: {str(e)}")
        sys.exit(1)



if __name__ == "__main__":
    main()
