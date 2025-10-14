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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Constants
UNLICENSED_MESSAGE = "License required to access this information"

# WordPress endpoints - will be updated dynamically
WP_BASE_URL = None
WP_LICENSE_URL = None
WP_COMPANY_URL = None
WP_JOB_URL = None

PROCESSED_IDS_FILE = "processed_job_ids.json"
LAST_PAGE_FILE = "last_processed_page.txt"

FRENCH_TO_ENGLISH_JOB_TYPE = {
    "Temps plein": "Full-time",
    "Temps partiel": "Part-time",
    "Contrat": "Contract",
    "Temporaire": "Temporary",
    "Indépendant": "Freelance",
    "Stage": "Internship",
    "Bénévolat": "Volunteer"
}

def validate_license(site_url, wp_username, wp_app_password):
    """Fetch and validate license from WordPress plugin API"""
    global WP_BASE_URL, WP_LICENSE_URL, WP_COMPANY_URL, WP_JOB_URL
    
    WP_BASE_URL = site_url.rstrip('/')
    WP_LICENSE_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/validate-license"
    WP_COMPANY_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/save-company"
    WP_JOB_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/save-job"
    
    # Create auth headers
    auth_string = f"{wp_username}:{wp_app_password}"
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    
    try:
        logger.info(f"Validating license with WordPress at {WP_LICENSE_URL}")
        response = requests.post(WP_LICENSE_URL, headers=wp_headers, json={}, timeout=10, verify=False)
        response.raise_for_status()
        
        result = response.json()
        licensed = result.get('licensed', False)
        license_message = result.get('message', '')
        
        if licensed:
            logger.info("✓ License validated successfully by WordPress plugin")
            print("✓ Licensed mode: Full data scraping enabled")
            return True, wp_headers
        else:
            logger.warning(f"⚠ License validation failed: {license_message}")
            print(f"⚠ Unlicensed mode: {license_message}")
            print("Only basic job data will be scraped")
            return False, wp_headers
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to validate license with WordPress: {str(e)}")
        print("❌ License validation failed - running in unlicensed mode")
        return False, wp_headers

def sanitize_text(text, is_url=False):
    if not text:
        return ''
    if is_url:
        text = text.strip()
        if text and not text.startswith(('http://', 'https://')):
            text = 'https://' + text
        return text
    text = re.sub(r'<[^>]+>', '', str(text))
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def generate_id(combined):
    return hashlib.md5(str(combined).encode()).hexdigest()[:16]

def split_paragraphs(text, max_length=200):
    if not text or text == UNLICENSED_MESSAGE:
        return text
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

def save_company_to_wordpress(company_data, wp_headers, licensed):
    """Save company data to WordPress"""
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
        response = requests.post(WP_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15, verify=False)
        response.raise_for_status()
        result = response.json()
        if result.get('success'):
            logger.info(f"✓ Company saved: {company_name}")
            return result.get("id"), result.get("message")
        else:
            logger.warning(f"Company save failed: {result.get('message', 'Unknown error')}")
            return None, result.get('message')
    except Exception as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, str(e)

def save_job_to_wordpress(job_data, company_id, wp_headers, licensed):
    """Save job data to WordPress"""
    job_title = job_data.get("job_title", "")
    if not job_title:
        return None, "No job title"
        
    job_id = generate_id(f"{job_title}_{job_data.get('company_name', '')}")
    
    # Application info
    application = job_data.get("application_url", "")
    if '@' in job_data.get("description_application_info", ""):
        application = job_data.get("description_application_info", "")
    elif job_data.get("resolved_application_url", ""):
        application = job_data.get("resolved_application_url", "")
    
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else ""),
        "job_type": sanitize_text(job_data.get("job_type", "")),
        "location": sanitize_text(job_data.get("location", "")),
        "job_url": sanitize_text(job_data.get("job_url", ""), is_url=True),
        "environment": sanitize_text(job_data.get("environment", "")),
        "job_salary": sanitize_text(job_data.get("job_salary", "")),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": company_id,
        "company_name": sanitize_text(job_data.get("company_name", "")),
        "company_logo": sanitize_text(job_data.get("company_logo", ""), is_url=True),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_website_url": sanitize_text(job_data.get("company_website_url", ""), is_url=True),
        "company_address": job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": sanitize_text(job_data.get("company_industry", "")),
        "company_founded": sanitize_text(job_data.get("company_founded", "")),
        "company_twitter": "",
        "company_video": ""
    }
    
    try:
        response = requests.post(WP_JOB_URL, json=post_data, headers=wp_headers, timeout=15, verify=False)
        response.raise_for_status()
        result = response.json()
        if result.get('success'):
            logger.info(f"✓ Job saved: {job_title}")
            return result.get("id"), result.get("message")
        else:
            logger.warning(f"Job save failed: {result.get('message', 'Unknown error')}")
            return None, result.get('message')
    except Exception as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}")
        return None, str(e)

def load_processed_ids():
    """Load previously processed job IDs"""
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_processed_ids(processed_ids):
    """Save processed job IDs"""
    try:
        with open(PROCESSED_IDS_FILE, "w") as f:
            json.dump(list(processed_ids), f)
    except Exception as e:
        logger.error(f"Failed to save processed IDs: {e}")

def scrape_job_details(job_url, licensed):
    """Scrape job details with strict license enforcement"""
    try:
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1))
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # BASIC DATA - Always available
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        
        company_name_elem = soup.select_one(".topcard__org-name-link")
        company_name = company_name_elem.get_text().strip() if company_name_elem else ''
        
        location_elem = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location_elem.get_text().strip() if location_elem else ''
        
        job_type_elem = soup.select_one(".description__job-criteria-list li:nth-child(2) span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        
        # LICENSED DATA - Strictly blocked without license
        licensed_data = {
            'company_logo': UNLICENSED_MESSAGE,
            'company_url': UNLICENSED_MESSAGE,
            'environment': UNLICENSED_MESSAGE,
            'level': UNLICENSED_MESSAGE,
            'job_functions': UNLICENSED_MESSAGE,
            'industries': UNLICENSED_MESSAGE,
            'job_description': UNLICENSED_MESSAGE,
            'application_url': UNLICENSED_MESSAGE,
            'description_application_info': UNLICENSED_MESSAGE,
            'company_details': UNLICENSED_MESSAGE,
            'company_website_url': UNLICENSED_MESSAGE,
            'company_industry': UNLICENSED_MESSAGE,
            'company_size': UNLICENSED_MESSAGE,
            'company_headquarters': UNLICENSED_MESSAGE,
            'company_type': UNLICENSED_MESSAGE,
            'company_founded': UNLICENSED_MESSAGE,
            'company_specialties': UNLICENSED_MESSAGE,
            'company_address': UNLICENSED_MESSAGE
        }
        
        if licensed:
            # Company logo
            logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            if logo_elem:
                licensed_data['company_logo'] = logo_elem.get('data-delayed-url') or logo_elem.get('src') or ''
            
            # Company URL
            if company_name_elem and company_name_elem.get('href'):
                licensed_data['company_url'] = re.sub(r'\?.*$', '', company_name_elem['href'])
            
            # Environment
            env_elems = soup.select(".topcard__flavor--metadata")
            for elem in env_elems:
                text = elem.get_text().lower()
                if any(word in text for word in ['remote', 'hybrid', 'on-site']):
                    licensed_data['environment'] = elem.get_text().strip()
                    break
            
            # Job criteria
            level_elem = soup.select_one(".description__job-criteria-list li:nth-child(1) span")
            if level_elem:
                licensed_data['level'] = level_elem.get_text().strip()
            
            functions_elem = soup.select_one(".description__job-criteria-list li:nth-child(3) span")
            if functions_elem:
                licensed_data['job_functions'] = functions_elem.get_text().strip()
            
            industries_elem = soup.select_one(".description__job-criteria-list li:nth-child(4) span")
            if industries_elem:
                licensed_data['industries'] = industries_elem.get_text().strip()
            
            # Job description
            desc_container = soup.select_one(".show-more-less-html__markup")
            if desc_container:
                paragraphs = desc_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    text_parts = [sanitize_text(p.get_text()) for p in paragraphs if sanitize_text(p.get_text())]
                    licensed_data['job_description'] = split_paragraphs('\n\n'.join(text_parts))
            
            # Application URL
            app_anchor = soup.select_one("#teriary-cta-container div a")
            if app_anchor and app_anchor.get('href'):
                licensed_data['application_url'] = app_anchor['href']
        
        return {
            'job_title': job_title,
            'company_name': company_name,
            'location': location,
            'job_type': job_type,
            'job_url': job_url,
            **licensed_data,
            'job_salary': ''
        }
        
    except Exception as e:
        logger.error(f"Error scraping {job_url}: {str(e)}")
        return None

def crawl_jobs(wp_headers, licensed, country, keyword, processed_ids):
    """Main crawling function"""
    success_count = 0
    failure_count = 0
    pages_to_scrape = 5  # Limit for testing
    
    print(f"🚀 Starting crawl: {country} - {keyword}")
    print(f"📊 Mode: {'LICENSED (Full Data)' if licensed else 'UNLICENSED (Basic Data Only)'}")
    
    for page in range(pages_to_scrape):
        search_url = f'https://www.linkedin.com/jobs/search?keywords={keyword}&location={country}&start={page * 25}'
        logger.info(f"Scraping page {page + 1}: {search_url}")
        
        try:
            time.sleep(random.uniform(3, 7))  # Rate limiting
            response = requests.get(search_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            if "linkedin.com/login" in response.url or "challenge" in response.url:
                logger.error("Login/CAPTCHA detected - stopping")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            job_links = soup.select("ul.jobs-search__results-list li a[data-occludable-job-id]")
            job_urls = [link['href'] for link in job_links if link.get('href')]
            
            logger.info(f"Found {len(job_urls)} jobs on page {page + 1}")
            
            for job_url in job_urls:
                job_data = scrape_job_details(job_url, licensed)
                if not job_data or not job_data.get('job_title') or not job_data.get('company_name'):
                    failure_count += 1
                    continue
                
                job_id = generate_id(f"{job_data['job_title']}_{job_data['company_name']}")
                if job_id in processed_ids:
                    logger.info(f"Skipping processed job: {job_data['job_title']}")
                    continue
                
                # Save company first
                company_id, company_msg = save_company_to_wordpress(job_data, wp_headers, licensed)
                if not company_id:
                    logger.warning(f"Failed to save company: {company_msg}")
                    failure_count += 1
                    continue
                
                # Save job
                job_post_id, job_msg = save_job_to_wordpress(job_data, company_id, wp_headers, licensed)
                if job_post_id:
                    processed_ids.add(job_id)
                    success_count += 1
                    print(f"✅ {job_data['job_title']} @ {job_data['company_name']}")
                else:
                    failure_count += 1
                    logger.warning(f"Failed to save job: {job_msg}")
            
        except Exception as e:
            logger.error(f"Error on page {page + 1}: {str(e)}")
            failure_count += 1
    
    save_processed_ids(processed_ids)
    print(f"\n📈 SUMMARY:")
    print(f"   Success: {success_count}")
    print(f"   Failed: {failure_count}")
    print(f"   Mode: {'LICENSED' if licensed else 'UNLICENSED'}")

def main():
    """Main execution"""
    if len(sys.argv) < 5:
        print("Usage: python fetcher.py <country> <keyword> <wp_site_url> <wp_username> <wp_app_password>")
        print("Example: python fetcher.py 'botswana' 'software engineer' 'https://yoursite.com' 'user' 'pass'")
        sys.exit(1)
    
    # Get parameters from command line (plugin will pass these)
    country = sys.argv[1]
    keyword = sys.argv[2]
    site_url = sys.argv[3]
    wp_username = sys.argv[4]
    wp_app_password = sys.argv[5] if len(sys.argv) > 5 else ''
    
    if not all([country, keyword, site_url, wp_username, wp_app_password]):
        print("❌ All parameters are required")
        sys.exit(1)
    
    # Validate license with WordPress plugin
    licensed, wp_headers = validate_license(site_url, wp_username, wp_app_password)
    
    # Load processed IDs
    processed_ids = load_processed_ids()
    
    # Start crawling
    try:
        crawl_jobs(wp_headers, licensed, country, keyword, processed_ids)
    except KeyboardInterrupt:
        print("\n⏹️  Crawl interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        print(f"❌ Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
