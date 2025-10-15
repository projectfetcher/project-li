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
    """Scrape detailed job information - FIXED to preserve content"""
    try:
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Job title - FIXED: Get raw text first
        job_title_elem = soup.select_one("h1.top-card-layout__title")
        job_title = job_title_elem.get_text().strip() if job_title_elem else ''
        logger.info(f'Raw Job Title: "{job_title}"')  # Log raw value
        
        # Company name - FIXED: Get raw text first
        company_elem = soup.select_one(".topcard__org-name-link")
        company_name = company_elem.get_text().strip() if company_elem else ''
        logger.info(f'Raw Company Name: "{company_name}"')
        
        # Company URL
        company_url = ''
        if company_elem and company_elem.get('href'):
            company_url = re.sub(r'\?.*$', '', company_elem['href'])
        
        # Location
        location_elem = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location_raw = location_elem.get_text().strip() if location_elem else COUNTRY or 'Worldwide'
        location = ', '.join(dict.fromkeys([part.strip() for part in location_raw.split(',') if part.strip()]))
        logger.info(f'Raw Location: "{location_raw}" -> Cleaned: "{location}"')
        
        # Job type
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type_raw = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type_raw, job_type_raw)
        logger.info(f'Raw Job Type: "{job_type_raw}" -> Mapped: "{job_type}"')
        
        # FIXED: Job Description - Preserve content
        job_description = UNLICENSED_MESSAGE if not licensed else ''
        if licensed:
            desc_container = soup.select_one(".show-more-less-html__markup") or soup.select_one(".description__text")
            if desc_container:
                # Get raw text first
                raw_desc = desc_container.get_text(separator='\n\n').strip()
                logger.info(f'Raw Description length: {len(raw_desc)} chars')
                
                # Clean but preserve content
                paragraphs = [p.strip() for p in raw_desc.split('\n\n') if p.strip()]
                seen = set()
                unique_paras = []
                
                for para in paragraphs:
                    norm_para = normalize_for_deduplication(para)
                    if norm_para not in seen:
                        unique_paras.append(para)  # Use raw paragraph
                        seen.add(norm_para)
                
                job_description = '\n\n'.join(unique_paras)
                # Remove only LinkedIn UI elements, not content
                job_description = re.sub(r'(?i)(show\s+more|show\s+less|click\s+here).*?(?=\n\n|$)', '', job_description)
                job_description = split_paragraphs(job_description)
                logger.info(f'Final Description: {len(job_description)} chars, {len(unique_paras)} paragraphs')
            else:
                logger.warning('No description container found')
                job_description = 'Job description not available'
        
        # FIXED: Application URL
        application_url = ''
        if licensed:
            app_selectors = [
                "#teriary-cta-container > div > a",
                ".jobs-apply-button--top-card",
                ".jobs-apply-button",
                "[data-test-apply-button]"
            ]
            for selector in app_selectors:
                app_anchor = soup.select_one(selector)
                if app_anchor and app_anchor.get('href'):
                    application_url = app_anchor['href']
                    try:
                        app_resp = session.get(application_url, allow_redirects=True, timeout=10, headers=headers)
                        application_url = app_resp.url
                        logger.info(f'Resolved Application: {application_url}')
                    except Exception as e:
                        logger.warning(f'Could not resolve application URL: {e}')
                    break
        
        # FIXED: Company Details (only if licensed)
        company_details = UNLICENSED_MESSAGE if not licensed else ''
        company_website_url = '' if not licensed else ''
        company_industry = '' if not licensed else ''
        company_founded = '' if not licensed else ''
        company_address = '' if not licensed else ''
        
        if licensed and company_url:
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                # Company details
                details_elem = (company_soup.select_one("p.about-us__description") or 
                               company_soup.select_one(".about-us__description") or
                               company_soup.select_one("section.core-section-container p"))
                company_details = details_elem.get_text().strip() if details_elem else 'Company overview not available'
                logger.info(f'Raw Company Details: {company_details[:100]}...')
                
                # Website with redirect handling
                website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a, .org-about__website a")
                if website_anchor and website_anchor.get('href'):
                    company_website_url = website_anchor['href']
                    # Handle LinkedIn redirects
                    if 'linkedin.com/redir/redirect' in company_website_url:
                        parsed = urlparse(company_website_url)
                        params = parse_qs(parsed.query)
                        if 'url' in params:
                            company_website_url = unquote(params['url'][0])
                            try:
                                web_resp = session.get(company_website_url, allow_redirects=True, timeout=10)
                                company_website_url = web_resp.url
                            except:
                                pass
                    logger.info(f'Company Website: {company_website_url}')
                
                # Parse structured company data
                dl_elements = company_soup.select("dl > div")
                for dl_elem in dl_elements:
                    dt = dl_elem.find("dt")
                    dd = dl_elem.find("dd")
                    if dt and dd:
                        label = dt.get_text().strip().lower()
                        value = dd.get_text().strip()
                        
                        if 'industry' in label:
                            company_industry = value
                        elif 'founded' in label or 'year founded' in label:
                            company_founded = value
                        elif 'headquarters' in label:
                            company_address = value
                
                logger.info(f'Company Info - Industry: {company_industry}, Founded: {company_founded}, Address: {company_address}')
                
            except Exception as e:
                logger.error(f'Company scrape failed: {e}')
                company_details = 'Company details unavailable'
        
        # Return dictionary with raw values
        return {
            'job_title': job_title,  # Raw, unsanitized
            'company_name': company_name,  # Raw, unsanitized
            'location': location,
            'job_type': job_type,
            'job_description': job_description,
            'application': application_url or 'N/A',
            'company_logo': company_logo if licensed else '',
            'company_url': company_url,
            'environment': environment,
            'company_details': company_details,
            'company_website_url': company_website_url,
            'company_industry': company_industry,
            'company_founded': company_founded,
            'company_address': company_address or location,  # Fallback to job location
            'job_url': job_url
        }
        
    except Exception as e:
        logger.error(f'Job scrape failed {job_url}: {e}')
        return None

def crawl(wp_headers, processed_ids, licensed):
    """Main crawling function"""
    logger.info(f"Starting crawl for country={COUNTRY}, keyword={KEYWORD or 'ALL JOBS'}, licensed={licensed}")
    
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 5  # Reduced for testing
    
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    for i in range(start_page, start_page + pages_to_scrape):
        url = build_search_url(i)
        logger.info(f"Fetching page {i}: {url}")
        
        time.sleep(random.uniform(3, 7))  # Reduced delay for testing
        
        try:
            response = session.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            
            if "login" in response.url.lower() or "challenge" in response.url.lower():
                logger.error("Login or CAPTCHA detected, stopping crawl")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list li a")
            urls = [a['href'] for a in job_list if a.get('href') and 'jobs/view' in a['href']]
            
            logger.info(f"Found {len(urls)} job URLs on page {i}")
            
            if not urls:
                logger.warning(f"No jobs found on page {i}, possibly end of results")
                break
            
            for index, job_url in enumerate(urls[:3]):  # Limit to 3 jobs per page for testing
                logger.info(f"Processing job {index + 1}/{len(urls)}: {job_url}")
                
                job_data = scrape_job_details(job_url, licensed, session)
                if not job_data:
                    failure_count += 1
                    continue
                
                job_dict = dict(zip([
                    "job_title", "company_logo", "company_name", "company_url", "location",
                    "environment", "job_type", "level", "job_functions", "industries",
                    "job_description", "job_url", "company_details", "company_website_url",
                    "company_industry", "company_size", "company_headquarters", "company_type",
                    "company_founded", "company_specialties", "company_address", "application_url",
                    "description_application_info", "resolved_application_info", "final_application_email",
                    "final_application_url", "resolved_application_url"
                ], job_data))
                job_dict["job_salary"] = ""
                
                job_title = job_dict.get("job_title", "")
                company_name = job_dict.get("company_name", "")
                
                if not job_title or not company_name:
                    logger.warning(f"Skipping job with missing title or company: {job_title} - {company_name}")
                    failure_count += 1
                    continue
                
                job_id = generate_id(f"{job_title}_{company_name}")
                
                if job_id in processed_ids:
                    logger.info(f"Skipping already processed job: {job_id}")
                    total_jobs += 1
                    continue
                
                total_jobs += 1
                
                # Save company
                company_id, company_msg = save_company_to_wordpress(index, job_dict, wp_headers, licensed)
                if not company_id:
                    logger.error(f"Failed to save company: {company_msg}")
                    failure_count += 1
                    continue
                
                # Save job
                job_post_id, job_msg = save_article_to_wordpress(index, job_dict, company_id, wp_headers, licensed)
                
                if job_post_id:
                    processed_ids.add(job_id)
                    success_count += 1
                    emoji = "🔓" if licensed else "🔒"
                    print(f"{emoji} Saved: {job_title} at {company_name}")
                else:
                    failure_count += 1
                    print(f"✗ Failed: {job_title} at {company_name} - {job_msg}")
                
                time.sleep(random.uniform(2, 5))  # Rate limiting
            
            save_last_page(i + 1)
            
        except Exception as e:
            logger.error(f"Error processing page {i}: {str(e)}")
            failure_count += 1
            continue
    
    save_processed_ids(processed_ids)
    
    logger.info(f"Crawl completed: Total={total_jobs}, Success={success_count}, Failed={failure_count}")
    print(f"\n=== SUMMARY ===")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully saved: {success_count}")
    print(f"Failed: {failure_count}")
    print(f"License status: {'FULL ACCESS' if licensed else 'BASIC ACCESS ONLY'}")

def main():
    """Main execution function"""
    try:
        logger.info("Starting LinkedIn Job Fetcher")
        print("🚀 Starting LinkedIn Job Fetcher...")
        print(f"📍 Country: {COUNTRY}")
        print(f"🔍 Keyword: {KEYWORD or 'ALL JOBS'}")
        
        # Validate environment (LICENSE_KEY is optional)
        validate_environment()
        
        # FIXED: Check license with proper LICENSE_KEY validation
        licensed = get_license_status()
        
        # Create WP headers
        wp_headers = create_wp_auth_headers()
        
        # Load processed IDs
        processed_ids = load_processed_ids()
        print(f"📋 Found {len(processed_ids)} previously processed jobs")
        
        # Start crawling
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
