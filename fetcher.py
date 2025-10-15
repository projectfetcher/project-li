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
    """Scrape detailed job information from LinkedIn with improved selectors"""
    try:
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Job title
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        logger.info(f'Scraped Job Title: {job_title}')
        
        # Company logo (licensed only)
        company_logo = ''
        if licensed:
            logo_selectors = [
                "div.top-card-layout__entity-info-container a img",
                "#main-content > section.core-rail img[data-delayed-url]",
                ".topcard__image img"
            ]
            for selector in logo_selectors:
                logo_elem = soup.select_one(selector)
                if logo_elem:
                    company_logo = logo_elem.get('data-delayed-url') or logo_elem.get('src') or ''
                    break
            logger.info(f'Scraped Company Logo: {company_logo}')
        
        # Company name and URL
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        logger.info(f'Scraped Company Name: {company_name}')
        
        company_url = ''
        if licensed and company_name:
            company_url_elem = soup.select_one(".topcard__org-name-link")
            if company_url_elem and company_url_elem.get('href'):
                company_url = re.sub(r'\?.*$', '', company_url_elem['href'])
                logger.info(f'Scraped Company URL: {company_url}')
        
        # Location
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else COUNTRY or 'Worldwide'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f'Location: {location}')
        
        # Environment (Remote/Hybrid/Onsite)
        environment = ''
        env_elements = soup.select(".topcard__flavor--metadata")
        for elem in env_elements:
            text = elem.get_text().strip().lower()
            if any(word in text for word in ['remote', 'hybrid', 'on-site', 'onsite']):
                environment = elem.get_text().strip()
                break
        logger.info(f'Environment: {environment}')
        
        # Job type with better mapping
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        logger.info(f'Job Type: {job_type}')
        
        # FIXED: Improved Job Description extraction
        job_description = UNLICENSED_MESSAGE if not licensed else ''
        if licensed:
            desc_selectors = [
                ".show-more-less-html__markup",
                "[data-job-description-section] .show-more-less-html__markup",
                ".description__text"
            ]
            description_container = None
            for selector in desc_selectors:
                description_container = soup.select_one(selector)
                if description_container:
                    break
            
            if description_container:
                # Extract paragraphs and clean duplicates
                paragraphs = description_container.find_all(['p', 'li', 'div'], recursive=False)
                seen = set()
                unique_paras = []
                
                for p in paragraphs:
                    para = sanitize_text(p.get_text().strip())
                    if para:
                        norm_para = normalize_for_deduplication(para)
                        if norm_para not in seen:
                            unique_paras.append(para)
                            seen.add(norm_para)
                
                job_description = '\n\n'.join(unique_paras)
                # Remove LinkedIn UI text
                job_description = re.sub(r'(?i)(show more|show less|click here).*', '', job_description)
                job_description = split_paragraphs(job_description)
                logger.info(f'Job Description: {len(job_description)} chars')
            else:
                logger.warning('No job description container found')
        
        # FIXED: Improved Application URL extraction
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
                    # Follow redirect for external application URLs
                    try:
                        app_response = session.get(application_url, allow_redirects=True, timeout=10)
                        application_url = app_response.url
                        logger.info(f'Resolved Application URL: {application_url}')
                    except:
                        pass
                    break
        
        # Company details scraping (licensed only)
        company_details = UNLICENSED_MESSAGE if not licensed else ''
        company_website_url = UNLICENSED_MESSAGE if not licensed else ''
        company_industry = UNLICENSED_MESSAGE if not licensed else ''
        company_founded = UNLICENSED_MESSAGE if not licensed else ''
        company_address = UNLICENSED_MESSAGE if not licensed else ''
        
        if licensed and company_url:
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                # FIXED: Better company details extraction
                details_selectors = [
                    "p.about-us__description",
                    ".about-us__description",
                    "section.core-section-container p",
                    "[data-test-company-overview] p"
                ]
                for selector in details_selectors:
                    details_elem = company_soup.select_one(selector)
                    if details_elem:
                        company_details = sanitize_text(details_elem.get_text().strip())
                        break
                
                # FIXED: Improved website extraction with redirect handling
                website_selectors = [
                    "dl > div:nth-child(1) > dd > a",
                    ".org-about__website a",
                    "[data-test-company-website] a"
                ]
                for selector in website_selectors:
                    website_anchor = company_soup.select_one(selector)
                    if website_anchor and website_anchor.get('href'):
                        company_website_url = website_anchor['href']
                        # Handle LinkedIn redirect URLs
                        if 'linkedin.com/redir/redirect' in company_website_url:
                            parsed = urlparse(company_website_url)
                            params = parse_qs(parsed.query)
                            if 'url' in params:
                                company_website_url = unquote(params['url'][0])
                                # Follow final redirect
                                try:
                                    web_resp = session.get(company_website_url, allow_redirects=True, timeout=10)
                                    company_website_url = web_resp.url
                                except:
                                    pass
                        logger.info(f'Company Website: {company_website_url}')
                        break
                
                # FIXED: Better structured data extraction using DL elements
                dl_elements = company_soup.select("dl > div")
                for dl_elem in dl_elements:
                    dt = dl_elem.find("dt")
                    dd = dl_elem.find("dd")
                    if not dt or not dd:
                        continue
                    
                    label = dt.get_text().strip().lower()
                    value = sanitize_text(dd.get_text().strip())
                    
                    if 'industry' in label:
                        company_industry = value
                    elif 'founded' in label or 'year founded' in label:
                        company_founded = value
                    elif 'headquarters' in label or 'location' in label:
                        company_address = value
                    elif 'type' in label:
                        company_type = value
                    elif 'size' in label:
                        company_size = value
                
                # Fallback: Extract from description if structured data missing
                if not company_industry and company_details:
                    industry_patterns = [
                        r'(?:industry|sector)[:\s]*([^\n\.]{10,})',
                        r'we are a ([^\n\.]{10,}) company'
                    ]
                    for pattern in industry_patterns:
                        match = re.search(pattern, company_details, re.IGNORECASE)
                        if match:
                            company_industry = match.group(1).strip()
                            break
                
                logger.info(f'Company Details: {company_industry}, {company_founded}, {company_address}')
                
            except Exception as e:
                logger.error(f'Company page error: {str(e)}')
                company_details = 'Company details unavailable'
        
        # Job criteria (licensed only)
        level = UNLICENSED_MESSAGE if not licensed else ''
        job_functions = UNLICENSED_MESSAGE if not licensed else ''
        industries = UNLICENSED_MESSAGE if not licensed else ''
        
        if licensed:
            criteria_selectors = [
                ".description__job-criteria-list > li:nth-child(1) > span",  # Level
                ".description__job-criteria-list > li:nth-child(3) > span",  # Functions
                ".description__job-criteria-list > li:nth-child(4) > span"   # Industries
            ]
            
            try:
                level_elem = soup.select_one(criteria_selectors[0])
                level = level_elem.get_text().strip() if level_elem else ''
                
                functions_elem = soup.select_one(criteria_selectors[1])
                job_functions = functions_elem.get_text().strip() if functions_elem else ''
                
                ind_elem = soup.select_one(criteria_selectors[2])
                industries = ind_elem.get_text().strip() if ind_elem else ''
            except:
                pass
        
        # Return comprehensive job data
        return {
            'job_title': job_title,
            'company_logo': company_logo,
            'company_name': company_name,
            'company_url': company_url,
            'location': location,
            'environment': environment,
            'job_type': job_type,
            'level': level,
            'job_functions': job_functions,
            'industries': industries,
            'job_description': job_description,
            'job_url': job_url,
            'application': application_url,
            'company_details': company_details,
            'company_website_url': company_website_url,
            'company_industry': company_industry,
            'company_founded': company_founded,
            'company_address': company_address,
            'company_type': '',
            'company_size': '',
            'company_headquarters': company_address  # Use address as fallback
        }
        
    except Exception as e:
        logger.error(f'Failed to scrape job {job_url}: {str(e)}')
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
