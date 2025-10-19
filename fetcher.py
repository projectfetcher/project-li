import requests
from requests import Session
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
from requests_html import HTMLSession

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
    """Enhanced search URL with better parameters"""
    base_url = 'https://www.linkedin.com/jobs/search'
    
    # Add default keyword if none provided
    default_keyword = KEYWORD_ENCODED or 'software developer'  # Use specific keyword
    
    params = {
        'keywords': default_keyword,
        'location': COUNTRY_ENCODED,
        'start': str(page * 25),
        # Additional filters to get more results
        'f_TPR': 'r86400',  # Past 24 hours for fresh jobs
        'f_E': '1,2,3,4',   # All employment types
        'f_JT': 'F',        # Full-time preferred
        'f_WT': '1'         # 1+ week old jobs
    }
    
    # Build query string
    query_params = []
    for key, value in params.items():
        if value:  # Skip empty values
            query_params.append(f"{key}={urllib.parse.quote(str(value))}")
    
    url = f"{base_url}?{'&'.join(query_params)}"
    logger.debug(f"Built URL: {url}")
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
        # Clean the URL
        parsed = urlparse(job_url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        logger.debug(f"Cleaned URL: {clean_url}")
        
        html_session = HTMLSession()
        response = html_session.get(clean_url, headers=headers)
        response.html.render(timeout=20, sleep=3)  # Render JavaScript
        soup = BeautifulSoup(response.html.html, 'html.parser')
        
        # Save debug HTML
        job_id = re.search(r'/jobs/view/(\d+)', clean_url)
        if job_id:
            debug_file = f"uploads/debug_job_{job_id.group(1)}.html"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response.html.html)
            logger.info(f"💾 Debug job HTML saved: {debug_file}")
        
        # Job title
        job_title = ''
        job_title_selectors = [
            '.jobs-unified-top-card__job-title',
            'h1.t-24.t-bold',
            '.top-card-layout__title',
            'h1[data-test-id="jobs-details-hero-title"]',
            'h1.job-details-jobs-unified-top-card__job-title',
            'main h1'
        ]
        for selector in job_title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                job_title = title_elem.get_text().strip()
                logger.debug(f"Found job title with selector '{selector}': {job_title}")
                break
        
        if not job_title:
            logger.warning("No job title found")
            return None
        
        logger.info(f"✅ Job Title: {job_title}")
        
        # Company logo
        company_logo = ''
        if licensed:
            company_logo_elem = soup.select_one('.jobs-unified-top-card__company-image img')
            company_logo = company_logo_elem.get('src') if company_logo_elem and company_logo_elem.get('src') else ''
            if company_logo and 'media.licdn.com' in company_logo:
                company_logo = re.sub(r'\?.*$', '', company_logo)
                if not company_logo.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                    company_logo = f"{company_logo}.jpg"
                try:
                    logo_response = session.head(company_logo, headers=headers, timeout=5)
                    if 'image' not in logo_response.headers.get('content-type', '').lower():
                        company_logo = ''
                    else:
                        logger.info(f"✅ Validated Company Logo URL: {company_logo}")
                except:
                    company_logo = ''
        
        # Company name and URL
        company_name = soup.select_one('.jobs-unified-top-card__company-name a')
        company_name = company_name.get_text().strip() if company_name else ''
        company_url = company_name['href'] if company_name and company_name.get('href') else ''
        if company_url and licensed:
            company_url = re.sub(r'\?.*$', '', company_url)
        
        # Location
        location = soup.select_one('.jobs-unified-top-card__bullet')
        location = location.get_text().strip() if location else 'Unknown'
        location = ', '.join(dict.fromkeys([part.strip() for part in location.split(',') if part.strip()]))
        
        # Environment
        environment = ''
        if licensed:
            env_elements = soup.select('.jobs-unified-top-card__workplace-type')
            for elem in env_elements:
                text = elem.get_text().strip().lower()
                if text in ['remote', 'hybrid', 'on-site']:
                    environment = text.capitalize()
                    break
        
        # Job type
        job_type_elem = soup.select_one('.jobs-unified-top-card__job-insight li:nth-child(2) span')
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        
        # Level
        level = ''
        if licensed:
            level_elem = soup.select_one('.jobs-unified-top-card__job-insight li:nth-child(1) span')
            level = level_elem.get_text().strip() if level_elem else ''
        
        # Job functions
        job_functions = ''
        if licensed:
            functions_elem = soup.select_one('.jobs-unified-top-card__job-insight li:nth-child(3) span')
            job_functions = functions_elem.get_text().strip() if functions_elem else ''
        
        # Industries
        industries = ''
        if licensed:
            industries_elem = soup.select_one('.jobs-unified-top-card__job-insight li:nth-child(4) span')
            industries = industries_elem.get_text().strip() if industries_elem else ''
        
        # Job description
        job_description = ''
        if licensed:
            description_container = soup.select_one('.jobs-description-content__text')
            if description_container:
                raw_text = description_container.get_text(separator='\n').strip()
                paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                filtered_paragraphs = [
                    para for para in paragraphs
                    if not any(phrase.lower() in para.lower() for phrase in [
                        "Never Miss a Job Update Again",
                        "Don't Keep! Kindly Share:",
                        "We have started building our professional LinkedIn page"
                    ])
                ]
                seen = set()
                unique_paragraphs = []
                for para in filtered_paragraphs:
                    para = sanitize_text(para)
                    norm_para = normalize_for_deduplication(para)
                    if norm_para and norm_para not in seen:
                        unique_paragraphs.append(para)
                        seen.add(norm_para)
                job_description = '\n\n'.join(unique_paragraphs)
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
        
        # Application info
        description_application_info = ''
        description_application_url = ''
        if licensed and job_description:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
            else:
                links = description_container.find_all('a', href=True) if description_container else []
                for link in links:
                    href = link['href']
                    if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                        description_application_url = href
                        description_application_info = href
                        break
        
        # Application URL
        application_url = ''
        if licensed:
            application_anchor = soup.select_one('.jobs-apply-button')
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
        
        # Resolve application URL
        resolved_application_info = ''
        resolved_application_url = ''
        if licensed and application_url:
            try:
                time.sleep(5)
                resp_app = session.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                resolved_application_url = resp_app.url
                app_soup = BeautifulSoup(resp_app.text, 'html.parser')
                emails = re.findall(email_pattern, resp_app.text)
                if emails:
                    resolved_application_info = emails[0]
                else:
                    links = app_soup.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                            resolved_application_info = href
                            break
            except Exception as e:
                resolved_application_url = description_application_url or application_url
        
        # Final application details
        final_application_email = description_application_info if description_application_info and '@' in description_application_info else resolved_application_info
        final_application_url = resolved_application_url or description_application_url or application_url
        
        # Company details
        company_details = ''
        company_website_url = ''
        company_industry = ''
        company_size = ''
        company_headquarters = ''
        company_type = ''
        company_founded = ''
        company_specialties = ''
        company_address = ''
        if licensed and company_url:
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                company_details = company_soup.select_one('.org-about-us__description')
                company_details = company_details.get_text().strip() if company_details else ''
                
                website_anchor = company_soup.select_one('.org-about-us__website a')
                company_website_url = website_anchor['href'] if website_anchor and website_anchor.get('href') else ''
                
                company_industry = company_soup.select_one('.org-about-us__industry')
                company_industry = company_industry.get_text().strip() if company_industry else ''
                
                company_size = company_soup.select_one('.org-about-us__size')
                company_size = company_size.get_text().strip() if company_size else ''
                
                company_headquarters = company_soup.select_one('.org-about-us__headquarters')
                company_headquarters = company_headquarters.get_text().strip() if company_headquarters else ''
                
                company_type = company_soup.select_one('.org-about-us__type')
                company_type = company_type.get_text().strip() if company_type else ''
                
                company_founded = company_soup.select_one('.org-about-us__founded')
                company_founded = company_founded.get_text().strip() if company_founded else ''
                
                company_specialties = company_soup.select_one('.org-about-us__specialties')
                company_specialties = company_specialties.get_text().strip() if company_specialties else ''
                
                company_address = company_headquarters
            except:
                pass
        
        return [
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
            clean_url,
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
        
    except Exception as e:
        logger.error(f"scrape_job_details: Error in scrape_job_details for {job_url}: {str(e)}", exc_info=True)
        return None
    finally:
        html_session.close()

# Add this after other environment variables
LINKEDIN_COOKIES = os.getenv('LINKEDIN_COOKIES', '')  # LinkedIn cookies JSON string

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
    # Create session
    session = requests.Session()
    
    # Configure retries
    try:
        # Try new syntax first (urllib3 2.0+)
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
    except TypeError:
        # Fallback for older urllib3 versions
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
    li_at_present = False
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
                            li_at_present = True
            
            logger.info(f"✅ Successfully loaded {loaded_count} LinkedIn cookies")
            print(f"🍪 Loaded {loaded_count} cookies (li_at: {'✅' if li_at_present else '❌'})")
            
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
    
    if li_at_present:
        verify_cookies(session)
    else:
        logger.warning("No li_at cookie - some job details may be inaccessible")
    
    return session

def crawl(wp_headers, processed_ids, licensed):
    """Main crawling function optimized for requests-only LinkedIn extraction"""
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
        test_response = session.get(test_url, timeout=30)
        test_response.raise_for_status()
        
        if any(redirect in test_response.url.lower() for redirect in ["login", "challenge", "captcha"]):
            logger.error("❌ LinkedIn login required - check cookies")
            print("❌ LOGIN REQUIRED - Update cookies")
            return
        
        # Save debug HTML
        os.makedirs("uploads", exist_ok=True)
        debug_file = f"uploads/debug_page_{start_page}.html"
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
    max_empty_pages = 10  # Increased tolerance
    
    while empty_pages < max_empty_pages:
        url = build_search_url(page_num)
        logger.info(f"📄 Fetching page {page_num}: {url}")
        
        # Random delay
        time.sleep(random.uniform(10, 20))
        
        try:
            response = session.get(url, timeout=40)
            response.raise_for_status()
            
            # Check for blocks
            if any(block in response.url.lower() for block in ["login", "challenge", "captcha"]):
                logger.error("🔒 Session expired - get fresh cookies")
                break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Save page for debugging
            debug_file = f"uploads/debug_page_{page_num}.html"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            
            job_urls = extract_job_urls_from_page(response.text, soup)
            logger.info(f"🎯 Extracted {len(job_urls)} job URLs from page {page_num}")
            
            # Validate URLs
            job_urls_list = []
            for url in job_urls:
                if validate_job_url(url):
                    job_urls_list.append(url)
            
            job_urls_list = list(dict.fromkeys(job_urls_list))[:20]  # Dedupe and limit
            
            if not job_urls_list:
                logger.warning(f"📭 No valid jobs found on page {page_num}")
                empty_pages += 1
                save_last_page(page_num + 1)
                page_num += 1
                continue
            
            empty_pages = 0
            
            # Process jobs
            for idx, job_url in enumerate(job_urls_list):
                logger.info(f"🔄 Job {idx + 1}/{len(job_urls_list)}: {job_url}")
                
                try:
                    time.sleep(random.uniform(5, 10))
                    
                    job_data = scrape_job_details(job_url, licensed, session)
                    if not job_data:
                        failure_count += 1
                        continue
                    
                    # Map data
                    field_names = [
                        "job_title", "company_logo", "company_name", "company_url", "location",
                        "environment", "job_type", "level", "job_functions", "industries",
                        "job_description", "job_url", "company_details", "company_website_url",
                        "company_industry", "company_size", "company_headquarters", "company_type",
                        "company_founded", "company_specialties", "company_address", "application_url",
                        "description_application_info", "resolved_application_info", "final_application_email",
                        "final_application_url", "resolved_application_url"
                    ]
                    
                    while len(job_data) < len(field_names):
                        job_data.append("")
                    
                    job_dict = dict(zip(field_names, job_data[:len(field_names)]))
                    job_dict["job_salary"] = ""
                    job_dict["job_url"] = job_url
                    
                    job_title = job_dict.get("job_title", "").strip()
                    company_name = job_dict.get("company_name", "").strip()
                    
                    if not job_title or not company_name:
                        logger.warning(f"⏭️ Skipping incomplete: {job_title} - {company_name}")
                        failure_count += 1
                        continue
                    
                    job_id = generate_id(f"{normalize_for_deduplication(job_title)}_{normalize_for_deduplication(company_name)}")
                    if job_id in processed_ids:
                        logger.info(f"⏭️ Duplicate skipped: {job_title[:40]}...")
                        total_jobs += 1
                        continue
                    
                    total_jobs += 1
                    
                    # Save to WordPress
                    company_id, company_msg = save_company_to_wordpress(idx, job_dict, wp_headers, licensed)
                    if not company_id:
                        logger.error(f"💾 Company failed: {company_msg}")
                        failure_count += 1
                        continue
                    
                    job_post_id, job_msg = save_article_to_wordpress(idx, job_dict, company_id, wp_headers, licensed)
                    
                    if job_post_id:
                        processed_ids.add(job_id)
                        success_count += 1
                        emoji = "🔓" if licensed else "🔒"
                        print(f"{emoji} ✅ {job_title[:60]}... at {company_name}")
                    else:
                        failure_count += 1
                        print(f"❌ Failed: {job_title[:50]}... - {job_msg}")
                
                except Exception as e:
                    logger.error(f"❌ Job error {job_url}: {str(e)}")
                    failure_count += 1
                    continue
            
            save_last_page(page_num + 1)
            page_num += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Network error page {page_num}: {str(e)}")
            empty_pages += 1
            time.sleep(30)
            continue
        except Exception as e:
            logger.error(f"💥 Error page {page_num}: {str(e)}")
            empty_pages += 1
            continue
    
    save_processed_ids(processed_ids)
    
    print(f"\n{'='*60}")
    print(f"📊 CRAWL SUMMARY")
    print(f"{'='*60}")
    print(f"Total jobs: {total_jobs}, Success: {success_count}, Failed: {failure_count}")
    print(f"License: {'🔓 FULL' if licensed else '🔒 BASIC'}")
    print(f"Cookies: {'✅' if LINKEDIN_COOKIES else '⚠️'}")
    print(f"Debug: uploads/debug_page_*.html")
    print(f"{'='*60}")

def extract_job_urls_from_page(html_content, soup):
    """Advanced job URL extraction without Selenium"""
    job_urls = set()
    
    # Clean URL function
    def clean_url(url):
        # Remove query parameters and fragments
        parsed = urlparse(url)
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return clean if '/jobs/view/' in clean and validate_job_url(clean) else None
    
    # 1. COMPREHENSIVE REGEX PATTERNS
    job_patterns = [
        # Direct job URLs
        r'(https?://www\.linkedin\.com/jobs/view/\d+[^"\s\'<>]*)',
        r'href=[\'"](/jobs/view/\d+[^"\'>]*)[\'"]',
        r'"jobUrl":"(/jobs/view/\d+[^"]*)"'
    ]
    
    for pattern in job_patterns:
        matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0] if match[0] else match[-1]
            
            cleaned_url = clean_url(match if match.startswith('http') else f'https://www.linkedin.com{match}')
            if cleaned_url:
                job_urls.add(cleaned_url)
    
    # 2. JAVASCRIPT VARIABLE EXTRACTION
    js_patterns = [
        r'window\.initialServerState\s*=\s*({.*?});',
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
        r'jobsSearchResults\s*[:=]\s*({.*?})',
        r'jobResults\s*[:=]\s*(\[.*?\])',
    ]
    
    for pattern in js_patterns:
        matches = re.findall(pattern, html_content, re.DOTALL)
        for match in matches:
            try:
                if match.startswith('{'):
                    data = json.loads(match)
                    extract_jobs_from_json(data, job_urls)
                elif match.startswith('['):
                    data = json.loads(match)
                    for item in data:
                        extract_jobs_from_json(item, job_urls)
            except json.JSONDecodeError:
                continue
    
    # 3. DATA ATTRIBUTES SCAN
    data_attrs = soup.find_all(attrs={'data-job-id': True})
    for elem in data_attrs:
        job_id = elem.get('data-job-id')
        if job_id and len(job_id) > 5:
            job_urls.add(f'https://www.linkedin.com/jobs/view/{job_id}')
    
    # 4. LINK SCAN WITH CONTEXT
    all_links = soup.find_all('a', href=True)
    for link in all_links:
        href = link.get('href', '')
        text = link.get_text().strip().lower()
        
        # Look for job links near job-related text
        if any(keyword in href for keyword in ['/jobs/view/', '/job/']) and \
           any(job_context in text for job_context in ['apply', 'job', 'position']):
            full_url = href if href.startswith('http') else 'https://www.linkedin.com' + href
            cleaned_url = clean_url(full_url)
            if cleaned_url:
                job_urls.add(cleaned_url)
    
    # 5. MICRODATA/JSON-LD
    json_ld = soup.find_all('script', type='application/ld+json')
    for script in json_ld:
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                for item in data:
                    if item.get('@type') == 'JobPosting' and item.get('url'):
                        cleaned_url = clean_url(item['url'])
                        if cleaned_url:
                            job_urls.add(cleaned_url)
            elif data.get('@type') == 'JobPosting':
                if data.get('url'):
                    cleaned_url = clean_url(data['url'])
                    if cleaned_url:
                        job_urls.add(cleaned_url)
        except:
            continue
    
    logger.debug(f"Extracted {len(job_urls)} total job URLs")
    return list(job_urls)

def extract_jobs_from_json(data, job_urls):
    """Extract job URLs from nested JSON structure"""
    if isinstance(data, dict):
        # Look for job URL patterns
        for key, value in data.items():
            if isinstance(value, str) and '/jobs/view/' in value:
                job_urls.add(value)
            elif key in ['jobId', 'job_id', 'id'] and isinstance(value, (str, int)):
                if isinstance(value, str) and value.isdigit():
                    job_urls.add(f'https://www.linkedin.com/jobs/view/{value}')
            elif isinstance(value, (dict, list)):
                extract_jobs_from_json(value, job_urls)

def validate_job_url(url):
    """Validate if URL is a proper LinkedIn job URL"""
    if not url or 'linkedin.com' not in url:
        return False
    
    job_id_match = re.search(r'/jobs/view/(\d+)', url)
    if not job_id_match:
        return False
    
    job_id = job_id_match.group(1)
    return len(job_id) > 5 and job_id.isdigit()

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
