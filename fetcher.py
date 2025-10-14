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

# Configure logging
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

def get_wp_urls(site_url):
    """Generate WordPress API URLs"""
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

# File paths and constants
PROCESSED_IDS_FILE = os.path.join("uploads", "processed_job_ids.json")
LAST_PAGE_FILE = os.path.join("uploads", "last_processed_page.txt")

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

VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/job-fetcher'

def sanitize_text(text, is_url=False):
    """Clean and sanitize text content"""
    if not text:
        return ''
    
    if is_url:
        text = text.strip()
        if not text.startswith(('http://', 'https://')):
            text = 'https://' + text
        return text
    
    # Remove HTML tags and normalize
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
    text = ' '.join(text.split())
    return text

def normalize_for_deduplication(text):
    """Normalize text for duplicate detection"""
    if not text:
        return ''
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text.lower()

def generate_id(combined):
    """Generate unique ID from text"""
    return hashlib.md5(combined.encode()).hexdigest()[:16]

def split_paragraphs(text, max_length=200):
    """Split long paragraphs into smaller chunks"""
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

def load_processed_ids():
    """Load previously processed job IDs"""
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
    """Save processed job IDs to file"""
    try:
        with open(PROCESSED_IDS_FILE, "w") as f:
            json.dump(list(processed_ids), f)
        logger.info(f"Saved {len(processed_ids)} job IDs")
    except Exception as e:
        logger.error(f"Failed to save processed IDs: {str(e)}")

def load_last_page():
    """Load last processed page number"""
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                return int(f.read().strip())
    except Exception as e:
        logger.error(f"Failed to load last page: {str(e)}")
    return 0

def save_last_page(page):
    """Save current page number"""
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"Saved last processed page: {page}")
    except Exception as e:
        logger.error(f"Failed to save last page: {str(e)}")

def save_company_to_wordpress(company_data, wp_headers, licensed, wp_urls):
    """Save company data to WordPress"""
    company_name = company_data.get("company_name", "")
    if not company_name:
        return None, "No company name"
    
    # Check for existing company with same logo
    company_logo = company_data.get("company_logo", "")
    if company_logo and company_logo != UNLICENSED_MESSAGE:
        try:
            response = requests.get(
                f"{wp_urls['WP_COMPANY_URL']}?meta_key=company_logo&meta_value={company_logo}",
                headers=wp_headers,
                timeout=15
            )
            response.raise_for_status()
            companies = response.json()
            if companies:
                company_id = companies[0].get('id')
                logger.info(f"Found existing company with logo: ID {company_id}")
                return company_id, "Company already exists"
        except Exception as e:
            logger.error(f"Failed to check existing company: {str(e)}")
    
    company_id = generate_id(company_name)
    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_logo": sanitize_text(company_logo, is_url=True),
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
        response = requests.post(wp_urls["WP_SAVE_COMPANY_URL"], json=post_data, headers=wp_headers, timeout=15)
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

def save_job_to_wordpress(job_data, company_id, auth_headers, licensed, wp_urls):
    """Save job data to WordPress"""
    job_title = job_data.get("job_title", "")
    company_name = job_data.get("company_name", "")
    if not job_title or not company_name:
        return None, "Missing job title or company name"
    
    # Determine application method
    application = ""
    description_application_info = job_data.get("description_application_info", "")
    resolved_application_url = job_data.get("resolved_application_url", "")
    application_url = job_data.get("application_url", "")
    
    if '@' in description_application_info:
        application = description_application_info
    elif resolved_application_url:
        application = resolved_application_url
    elif application_url:
        application = application_url
    
    job_id = generate_id(f"{job_title}_{company_name}")
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else ""),
        "job_type": sanitize_text(job_data.get("job_type", "")),
        "location": sanitize_text(job_data.get("location", "Unknown")),
        "job_url": sanitize_text(job_data.get("job_url", ""), is_url=True),
        "environment": sanitize_text(job_data.get("environment", "")),
        "job_salary": sanitize_text(job_data.get("job_salary", "")),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_logo": sanitize_text(job_data.get("company_logo", ""), is_url=True),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_website_url": sanitize_text(job_data.get("company_website_url", ""), is_url=True),
    }
    
    try:
        response = requests.post(wp_urls["WP_SAVE_JOB_URL"], json=post_data, headers=auth_headers, timeout=15)
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

def scrape_job_details(job_url, licensed):
    """Scrape detailed job information from LinkedIn"""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        if "login" in response.url or "challenge" in response.url:
            logger.error(f"Login or CAPTCHA detected at {response.url}")
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Basic job info (always available)
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Unknown'
        
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type_elem.get_text().strip() if job_type_elem else '', 
                                                   job_type_elem.get_text().strip() if job_type_elem else '')
        
        # Licensed features
        company_logo = UNLICENSED_MESSAGE if not licensed else ''
        environment = UNLICENSED_MESSAGE if not licensed else ''
        company_url = UNLICENSED_MESSAGE if not licensed else ''
        level = UNLICENSED_MESSAGE if not licensed else ''
        job_functions = UNLICENSED_MESSAGE if not licensed else ''
        industries = UNLICENSED_MESSAGE if not licensed else ''
        job_description = UNLICENSED_MESSAGE if not licensed else ''
        application_url = UNLICENSED_MESSAGE if not licensed else ''
        
        if licensed:
            # Company logo
            logo_elem = soup.select_one("img.artdeco-entity-image.artdeco-entity-image--square-5")
            company_logo = logo_elem.get('src') if logo_elem and logo_elem.get('src') else ''
            if company_logo:
                company_logo = re.sub(r'\?.*$', '', company_logo)
                if not company_logo.lower().endswith(('.jpg', '.jpeg', '.png', '.gif')):
                    company_logo += '.jpg'
            
            # Company URL
            company_url_elem = soup.select_one(".topcard__org-name-link")
            company_url = company_url_elem['href'] if company_url_elem and company_url_elem.get('href') else ''
            if company_url:
                company_url = re.sub(r'\?.*$', '', company_url)
            
            # Environment
            env_elements = soup.select(".topcard__flavor--metadata")
            for elem in env_elements:
                text = elem.get_text().strip().lower()
                if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                    environment = elem.get_text().strip()
                    break
            
            # Job criteria (licensed)
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
            
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            
            # Job description
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                raw_text = description_container.get_text(separator='\n').strip()
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
                for para in filtered_paragraphs:
                    para = sanitize_text(para)
                    if not para:
                        continue
                    norm_para = normalize_for_deduplication(para)
                    if norm_para not in seen:
                        unique_paragraphs.append(para)
                        seen.add(norm_para)
                
                job_description = '\n\n'.join(unique_paragraphs)
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description).strip()
                job_description = split_paragraphs(job_description)
            
            # Application URL
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
        
        # Company details scraping (licensed only)
        company_details = UNLICENSED_MESSAGE if not licensed else ''
        company_website_url = UNLICENSED_MESSAGE if not licensed else ''
        company_industry = UNLICENSED_MESSAGE if not licensed else ''
        company_size = UNLICENSED_MESSAGE if not licensed else ''
        company_headquarters = UNLICENSED_MESSAGE if not licensed else ''
        company_type = UNLICENSED_MESSAGE if not licensed else ''
        company_founded = UNLICENSED_MESSAGE if not licensed else ''
        company_specialties = UNLICENSED_MESSAGE if not licensed else ''
        company_address = UNLICENSED_MESSAGE if not licensed else ''
        
        if licensed and company_url and 'linkedin.com' in company_url:
            try:
                time.sleep(2)
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                # Company details
                details_elem = company_soup.select_one("p[data-test-id='about-us__description']")
                company_details = details_elem.get_text().strip() if details_elem else ''
                
                # Website
                website_div = company_soup.select_one("div[data-test-id='about-us__website']")
                if website_div:
                    website_anchor = website_div.select_one("dd a")
                    company_website_url = website_anchor['href'] if website_anchor and website_anchor.get('href') else ''
                    if 'linkedin.com/redir/redirect' in company_website_url:
                        parsed = urlparse(company_website_url)
                        params = parse_qs(parsed.query)
                        if 'url' in params:
                            company_website_url = unquote(params['url'][0])
                
                def get_company_detail(label):
                    div_selector = f"div[data-test-id='about-us__{label.lower()}']"
                    detail_div = company_soup.select_one(div_selector)
                    if detail_div:
                        dd = detail_div.select_one("dd")
                        return dd.get_text().strip() if dd else ''
                    return ''
                
                company_industry = get_company_detail("industry")
                company_size = get_company_detail("size")
                company_headquarters = get_company_detail("headquarters")
                company_type = get_company_detail("organizationType")
                company_founded = get_company_detail("foundedOn")
                company_specialties = get_company_detail("specialties")
                company_address = company_headquarters  # Fallback
                
            except Exception as e:
                logger.error(f"Failed to scrape company details: {str(e)}")
        
        job_data = {
            "job_title": job_title,
            "company_logo": company_logo,
            "company_name": company_name,
            "company_url": company_url,
            "location": location,
            "environment": environment,
            "job_type": job_type,
            "level": level,
            "job_functions": job_functions,
            "industries": industries,
            "job_description": job_description,
            "job_url": job_url,
            "company_details": company_details,
            "company_website_url": company_website_url,
            "company_industry": company_industry,
            "company_size": company_size,
            "company_headquarters": company_headquarters,
            "company_type": company_type,
            "company_founded": company_founded,
            "company_specialties": company_specialties,
            "company_address": company_address,
            "application_url": application_url,
            "description_application_info": "",  # Simplified for now
            "resolved_application_url": "",     # Simplified for now
            "job_salary": ""
        }
        
        return job_data
        
    except Exception as e:
        logger.error(f"Error scraping job {job_url}: {str(e)}")
        return None

def crawl(wp_headers, processed_ids, licensed, country, keyword, wp_urls):
    """Main crawling function"""
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10
    
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    for page_num in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords={keyword}&location={country}&start={page_num * 25}'
        logger.info(f"Fetching page {page_num}: {url}")
        
        try:
            time.sleep(random.uniform(5, 10))
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            job_links = soup.select("ul.jobs-search__results-list > li a")
            job_urls = [link['href'] for link in job_links if link.get('href')]
            
            logger.info(f"Found {len(job_urls)} jobs on page {page_num}")
            
            for job_url in job_urls:
                total_jobs += 1
                job_data = scrape_job_details(job_url, licensed)
                
                if not job_data:
                    failure_count += 1
                    continue
                
                job_title = job_data.get("job_title", "Unknown")
                company_name = job_data.get("company_name", "")
                job_id = generate_id(f"{job_title}_{company_name}")
                
                if job_id in processed_ids:
                    logger.info(f"Skipping processed job: {job_id}")
                    continue
                
                if not company_name or company_name.lower() == "unknown":
                    logger.warning(f"Skipping job with unknown company: {job_title}")
                    failure_count += 1
                    continue
                
                # Save company first
                company_id, company_msg = save_company_to_wordpress(job_data, wp_headers, licensed, wp_urls)
                if company_id is None:
                    failure_count += 1
                    continue
                
                # Save job
                job_post_id, job_msg = save_job_to_wordpress(job_data, company_id, wp_headers, licensed, wp_urls)
                
                if job_post_id:
                    processed_ids.add(job_id)
                    success_count += 1
                    logger.info(f"Saved job: {job_title} at {company_name}")
                else:
                    failure_count += 1
            
            save_last_page(page_num + 1)
            
        except Exception as e:
            logger.error(f"Error processing page {page_num}: {str(e)}")
            failure_count += 1
    
    save_processed_ids(processed_ids)
    
    logger.info(f"Crawl completed: {total_jobs} total, {success_count} success, {failure_count} failed")
    print(f"\n--- Summary ---")
    print(f"Total jobs: {total_jobs}")
    print(f"Success: {success_count}")
    print(f"Failed: {failure_count}")

def main():
    """Main execution function"""
    if len(sys.argv) < 5:
        print("Usage: python script.py <license_key> <country> <keyword> <site_url> [wp_username] [wp_app_password]")
        return
    
    license_key = sys.argv[1]
    country = sys.argv[2]
    keyword = sys.argv[3]
    site_url = sys.argv[4]
    wp_username = sys.argv[5] if len(sys.argv) > 5 else ""
    wp_app_password = sys.argv[6] if len(sys.argv) > 6 else ""
    
    if not site_url.startswith(('http://', 'https://')):
        print("Error: Invalid site_url")
        return
    
    licensed = license_key == VALID_LICENSE_KEY
    print(f"Licensed mode: {'Yes' if licensed else 'No (limited data)'}")
    
    wp_urls = get_wp_urls(site_url)
    
    # Setup authentication
    if wp_username and wp_app_password:
        auth_string = f"{wp_username}:{wp_app_password}"
        auth = base64.b64encode(auth_string.encode()).decode()
        wp_headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json"
        }
    else:
        print("Warning: No WordPress credentials provided")
        wp_headers = {"Content-Type": "application/json"}
        return
    
    # Try to fetch credentials from plugin
    try:
        credentials_response = requests.get(wp_urls["WP_CREDENTIALS_URL"], headers=wp_headers, timeout=15)
        if credentials_response.status_code == 200:
            credentials = credentials_response.json()
            wp_username = credentials.get('wp_username', wp_username)
            wp_app_password = credentials.get('wp_app_password', wp_app_password)
            
            # Update headers with fetched credentials
            auth_string = f"{wp_username}:{wp_app_password}"
            auth = base64.b64encode(auth_string.encode()).decode()
            wp_headers["Authorization"] = f"Basic {auth}"
    except Exception as e:
        logger.error(f"Failed to fetch credentials: {str(e)}")
    
    processed_ids = load_processed_ids()
    crawl(wp_headers, processed_ids, licensed, country, keyword, wp_urls)

if __name__ == "__main__":
    main()
