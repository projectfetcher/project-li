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
from typing import Optional, Set, Tuple, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fetcher.log')
    ]
)
logger = logging.getLogger(__name__)

# HTTP headers for scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Environment variables
WP_SITE_URL = os.getenv('WP_SITE_URL')
WP_USERNAME = os.getenv('WP_USERNAME')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
COUNTRY = os.getenv('COUNTRY', 'Unknown')
KEYWORD = os.getenv('KEYWORD', '')
FETCHER_TOKEN = os.getenv('FETCHER_TOKEN', '')
KEY_LICENSE = os.getenv('KEY_LICENSE', '').strip()

# Constants
EXPECTED_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
WP_URL = f"{WP_SITE_URL}/wp-json/wp/v2/job-listings"
WP_COMPANY_URL = f"{WP_SITE_URL}/wp-json/wp/v2/company"
WP_MEDIA_URL = f"{WP_SITE_URL}/wp-json/wp/v2/media"
WP_SAVE_COMPANY_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-company"
WP_SAVE_JOB_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/save-job"
WP_FETCHER_STATUS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-status"
WP_CREDENTIALS_URL = f"{WP_SITE_URL}/wp-json/fetcher/v1/get-credentials"
PROCESSED_IDS_FILE = "processed_job_ids.csv"
LAST_PAGE_FILE = "last_processed_page.txt"
JOB_TYPE_MAPPING = {
    "Full-time": "full-time", "Part-time": "part-time", "Contract": "contract",
    "Temporary": "temporary", "Freelance": "freelance", "Internship": "internship",
    "Volunteer": "volunteer"
}
FRENCH_TO_ENGLISH_JOB_TYPE = {
    "Temps plein": "Full-time", "Temps partiel": "Part-time", "Contrat": "Contract",
    "Temporaire": "Temporary", "Indépendant": "Freelance", "Stage": "Internship",
    "Bénévolat": "Volunteer"
}

def validate_environment() -> bool:
    """Validate required environment variables."""
    required_vars = {'WP_SITE_URL': WP_SITE_URL}
    missing = [key for key, value in required_vars.items() if not value]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        return False
    return True

def has_full_access() -> bool:
    """Check if the user has a valid license."""
    return KEY_LICENSE == EXPECTED_KEY

def locked_field() -> str:
    """Return a placeholder for locked fields when license is missing."""
    return "🔒 <a href='https://mimusjobs.com.jobfetcher' target='_blank'>Get License</a>"

def create_session() -> requests.Session:
    """Create a requests session with retries."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_credentials() -> bool:
    """Fetch WordPress credentials from the REST API if not provided."""
    global WP_USERNAME, WP_APP_PASSWORD
    if WP_USERNAME and WP_APP_PASSWORD:
        logger.info("Credentials provided via environment variables")
        return True
    logger.info(f"Fetching credentials from {WP_CREDENTIALS_URL}")
    try:
        session = create_session()
        response = session.get(WP_CREDENTIALS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.error(f"Failed to fetch credentials: {data.get('message', 'Unknown error')}")
            return False
        WP_USERNAME = data.get('wp_username')
        WP_APP_PASSWORD = data.get('wp_app_password')
        if not WP_USERNAME or not WP_APP_PASSWORD:
            logger.error("Credentials fetched but empty or invalid")
            return False
        logger.info("Successfully fetched credentials from WordPress")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to fetch credentials: {str(e)}")
        return False

def check_fetcher_status(auth_headers: Dict[str, str]) -> str:
    """Check the fetcher status from WordPress."""
    try:
        session = create_session()
        response = session.get(WP_FETCHER_STATUS_URL, headers=auth_headers, timeout=5)
        response.raise_for_status()
        return response.json().get('status', 'stopped')
    except requests.RequestException as e:
        logger.error(f"Failed to check fetcher status: {str(e)}")
        return 'stopped'

def sanitize_text(text: Optional[str], is_url: bool = False) -> str:
    """Sanitize text or URLs."""
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
    return ' '.join(text.split())

def normalize_for_deduplication(text: str) -> str:
    """Normalize text for deduplication."""
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text.lower()

def generate_job_id(job_title: str, company_name: str) -> str:
    """Generate a unique job ID."""
    combined = f"{job_title}_{company_name}"
    return hashlib.md5(combined.encode()).hexdigest()[:16]

def split_paragraphs(text: str, max_length: int = 200) -> str:
    """Split large paragraphs into smaller ones."""
    paragraphs = text.split('\n\n')
    result = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        while len(para) > max_length:
            split_point = para.rfind(' ', 0, max_length) or para.rfind('.', 0, max_length) or max_length
            result.append(para[:split_point].strip())
            para = para[split_point:].strip()
        if para:
            result.append(para)
    return '\n\n'.join(result)

def save_company_to_wordpress(index: int, company_data: Dict[str, Any], auth_headers: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """Save company data to WordPress."""
    if not has_full_access():
        logger.info("Limited access mode - skipping company details")
        return None, None
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped before saving company")
        return None, None

    company_name = company_data.get("company_name", "")
    company_id = hashlib.md5(company_name.encode()).hexdigest()[:16]
    attachment_id = 0
    company_logo = company_data.get("company_logo", "")
    if company_logo:
        try:
            session = create_session()
            logo_response = session.get(company_logo, headers=HEADERS, timeout=10)
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": auth_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            media_response = session.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, timeout=10)
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for {company_name}, Attachment ID: {attachment_id}")
        except requests.RequestException as e:
            logger.error(f"Failed to upload logo for {company_name}: {str(e)}")

    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": sanitize_text(company_data.get("company_details", "")),
        "featured_media": attachment_id,
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
        session = create_session()
        response = session.post(WP_SAVE_COMPANY_URL, json=post_data, headers=auth_headers, timeout=15)
        response.raise_for_status()
        res = response.json()
        if res.get("success") or res.get("message") == "Company already exists":
            logger.info(f"Saved or found existing company {company_name}: Company ID {company_id}")
            return company_id, f"{WP_SITE_URL}/wp-content/uploads/companies.json"
        logger.error(f"Failed to save company {company_name}: {res}")
        return None, None
    except requests.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, None

def save_article_to_wordpress(index: int, job_data: Dict[str, Any], company_id: Optional[str], auth_headers: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
    """Save job data to WordPress."""
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped before saving job")
        return None, None

    job_title = job_data.get("job_title", "")
    company_name = job_data.get("company_name", "")
    job_id = generate_job_id(job_title, company_name)
    limited_access = not has_full_access()

    attachment_id = 0
    company_logo = job_data.get("company_logo", "") if not limited_access else ""
    if company_logo:
        try:
            session = create_session()
            logo_response = session.get(company_logo, headers=HEADERS, timeout=10)
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": auth_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo_job_{index}.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            media_response = session.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, timeout=10)
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for job {job_title}, Attachment ID: {attachment_id}")
        except requests.RequestException as e:
            logger.error(f"Failed to upload logo for job {job_title}: {str(e)}")

    application = job_data.get("final_application_email", "") or job_data.get("final_application_url", "") or ""
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": sanitize_text(job_data.get("job_description", "")) if not limited_access else locked_field(),
        "featured_media": attachment_id,
        "job_location": sanitize_text(job_data.get("location", COUNTRY)),
        "job_type": sanitize_text(job_data.get("job_type", "")),
        "job_salary": sanitize_text(job_data.get("job_salary", "")) if not limited_access else locked_field(),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": str(company_id) if company_id else "",
        "company_name": sanitize_text(company_name),
        "company_website": sanitize_text(job_data.get("company_website_url", ""), is_url=True) if not limited_access else "",
        "company_logo": str(attachment_id) if attachment_id else "",
        "company_tagline": sanitize_text(job_data.get("company_details", "")) if not limited_access else "",
        "company_address": sanitize_text(job_data.get("company_address", "")) if not limited_access else "",
        "company_industry": sanitize_text(job_data.get("company_industry", "")) if not limited_access else "",
        "company_founded": sanitize_text(job_data.get("company_founded", "")) if not limited_access else "",
        "company_twitter": "",
        "company_video": ""
    }

    try:
        session = create_session()
        response = session.post(WP_SAVE_JOB_URL, json=post_data, headers=auth_headers, timeout=15)
        response.raise_for_status()
        res = response.json()
        if res.get("success") or res.get("message") == "Job exists":
            logger.info(f"Saved or found existing job {job_title}: Job ID {job_id}")
            return job_id, f"{WP_SITE_URL}/wp-content/uploads/jobs.json"
        logger.error(f"Failed to save job {job_title}: {res}")
        return None, None
    except requests.RequestException as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}")
        return None, None

def load_processed_ids() -> Set[str]:
    """Load processed job IDs from file."""
    processed_ids = set()
    if os.path.exists(PROCESSED_IDS_FILE):
        try:
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(processed_ids)} processed job IDs")
        except Exception as e:
            logger.error(f"Failed to load processed IDs: {str(e)}")
    return processed_ids

def save_processed_id(job_id: str) -> None:
    """Append a single job ID to the processed IDs file."""
    try:
        with open(PROCESSED_IDS_FILE, "a") as f:
            f.write(f"{job_id}\n")
        logger.debug(f"Saved job ID {job_id}")
    except Exception as e:
        logger.error(f"Failed to save job ID {job_id}: {str(e)}")

def load_last_page() -> int:
    """Load the last processed page number."""
    if os.path.exists(LAST_PAGE_FILE):
        try:
            with open(LAST_PAGE_FILE, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            logger.error(f"Failed to load last page: {str(e)}")
    return 0

def save_last_page(page: int) -> None:
    """Save the last processed page number."""
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.debug(f"Saved last processed page: {page}")
    except Exception as e:
        logger.error(f"Failed to save last page: {str(e)}")

def scrape_job_details(job_url: str, auth_headers: Dict[str, str]) -> Optional[Tuple]:
    """Scrape job details from a LinkedIn job page."""
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped before scraping job details")
        return None
    try:
        session = create_session()
        response = session.get(job_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        company_logo = soup.select_one(".top-card-layout img")
        company_logo = (company_logo.get('data-delayed-url') or company_logo.get('src') or '') if company_logo else ''
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        company_url = soup.select_one(".topcard__org-name-link")
        company_url = re.sub(r'\?.*$', '', company_url['href']) if company_url and company_url.get('href') else ''
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else COUNTRY
        location = ', '.join(dict.fromkeys(part.strip() for part in location.split(',') if part.strip()))
        environment = next((elem.get_text().strip() for elem in soup.select(".topcard__flavor--metadata")
                           if 'remote' in elem.get_text().lower() or 'hybrid' in elem.get_text().lower() or 'on-site' in elem.get_text().lower()), '')
        level = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
        level = level.get_text().strip() if level else ''
        job_type = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type.get_text().strip(), job_type.get_text().strip()) if job_type else ''
        job_functions = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
        job_functions = job_functions.get_text().strip() if job_functions else ''
        industries = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
        industries = industries.get_text().strip() if industries else ''

        job_description = ''
        description_container = soup.select_one(".show-more-less-html__markup")
        if description_container:
            paragraphs = description_container.find_all(['p', 'li'], recursive=False) or [para.strip() for para in description_container.get_text(separator='\n').strip().split('\n\n') if para.strip()]
            seen = set()
            unique_paragraphs = []
            for para in paragraphs:
                para = sanitize_text(para.get_text().strip() if isinstance(para, BeautifulSoup) else para)
                if not para:
                    continue
                norm_para = normalize_for_deduplication(para)
                if norm_para and norm_para not in seen:
                    unique_paragraphs.append(para)
                    seen.add(norm_para)
            job_description = split_paragraphs('\n\n'.join(unique_paragraphs))
            job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()

        description_application_info = ''
        description_application_url = ''
        if has_full_access() and description_container:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
            else:
                for link in description_container.find_all('a', href=True):
                    if 'apply' in link['href'].lower() or 'careers' in link['href'].lower() or 'jobs' in link['href'].lower():
                        description_application_url = link['href']
                        description_application_info = link['href']
                        break

        application_anchor = soup.select_one("#teriary-cta-container > div > a")
        application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
        resolved_application_info = ''
        resolved_application_url = ''
        final_application_email = description_application_info if '@' in description_application_info else ''
        final_application_url = description_application_url or application_url

        if has_full_access() and application_url:
            try:
                time.sleep(random.uniform(2, 5))
                resp_app = session.get(application_url, headers=HEADERS, timeout=15, allow_redirects=True)
                resolved_application_url = resp_app.url
                app_soup = BeautifulSoup(resp_app.text, 'html.parser')
                emails = re.findall(email_pattern, resp_app.text)
                if emails:
                    resolved_application_info = emails[0]
                else:
                    for link in app_soup.find_all('a', href=True):
                        if 'apply' in link['href'].lower() or 'careers' in link['href'].lower() or 'jobs' in link['href'].lower():
                            resolved_application_info = link['href']
                            break
                final_application_email = resolved_application_info if '@' in resolved_application_info else final_application_email
                final_application_url = resolved_application_url or final_application_url
            except requests.RequestException as e:
                logger.error(f"Failed to resolve application URL: {str(e)}")
                final_application_url = application_url

        company_details = ''
        company_website_url = ''
        company_industry = ''
        company_size = ''
        company_headquarters = ''
        company_type = ''
        company_founded = ''
        company_specialties = ''
        company_address = location
        if has_full_access() and company_url:
            try:
                company_response = session.get(company_url, headers=HEADERS, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                company_details = company_soup.select_one("p.about-us__description")
                company_details = company_details.get_text().strip() if company_details else ''
                company_website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a")
                company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                if 'linkedin.com/redir/redirect' in company_website_url:
                    query_params = parse_qs(urlparse(company_website_url).query)
                    company_website_url = unquote(query_params['url'][0]) if 'url' in query_params else ''
                if company_website_url and 'linkedin.com' not in company_website_url:
                    try:
                        resp_company_web = session.get(company_website_url, headers=HEADERS, timeout=15, allow_redirects=True)
                        company_website_url = resp_company_web.url
                    except requests.RequestException:
                        company_website_url = ''
                company_industry = company_soup.select_one("dl > div:nth-child(2) > dd")
                company_industry = company_industry.get_text().strip() if company_industry else ''
                company_size = company_soup.select_one("dl > div:nth-child(3) > dd")
                company_size = company_size.get_text().strip() if company_size else ''
                company_headquarters = company_soup.select_one("dl > div:nth-child(4) > dd")
                company_headquarters = company_headquarters.get_text().strip() if company_headquarters else ''
                company_type = company_soup.select_one("dl > div:nth-child(5) > dd")
                company_type = company_type.get_text().strip() if company_type else ''
                company_founded = company_soup.select_one("dl > div:nth-child(6) > dd")
                company_founded = company_founded.get_text().strip() if company_founded else ''
                company_specialties = company_soup.select_one("dl > div:nth-child(7) > dd")
                company_specialties = company_specialties.get_text().strip() if company_specialties else ''
                company_address = company_headquarters or location
            except requests.RequestException as e:
                logger.error(f"Failed to scrape company page {company_url}: {str(e)}")

        if not has_full_access():
            job_description = company_logo = company_details = company_website_url = company_industry = \
            company_size = company_headquarters = company_type = company_founded = company_specialties = \
            final_application_email = final_application_url = description_application_info = resolved_application_info = ''

        return (
            job_title, company_logo, company_name, company_url, location, environment, job_type, level,
            job_functions, industries, job_description, job_url, company_details, company_website_url,
            company_industry, company_size, company_headquarters, company_type, company_founded,
            company_specialties, company_address, application_url, description_application_info,
            resolved_application_info, final_application_email, final_application_url
        )
    except requests.RequestException as e:
        logger.error(f"Failed to scrape job details from {job_url}: {str(e)}")
        return None

def crawl(auth_headers: Dict[str, str], processed_ids: Set[str]) -> None:
    """Crawl LinkedIn job listings and save to WordPress."""
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped by initial status check")
        print("Fetcher is not running. Exiting.")
        return

    success_count = failure_count = total_jobs = 0
    start_page = load_last_page()
    session = create_session()

    for page in range(start_page, 15):
        if check_fetcher_status(auth_headers) != 'running':
            logger.info("Fetcher stopped during page processing")
            print("Fetcher stopped by user. Exiting.")
            break
        url = f'https://www.linkedin.com/jobs/search?keywords={KEYWORD}&location={COUNTRY}&start={page * 25}'
        logger.info(f"Fetching job search page: {url}")
        try:
            time.sleep(random.uniform(2, 5))
            response = session.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            if "login" in response.url or "challenge" in response.url:
                logger.error("Login or CAPTCHA detected, stopping crawl")
                print("Login or CAPTCHA detected, stopping crawl")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list > li > div > a")
            urls = [a['href'] for a in job_list if a.get('href')]
            logger.info(f"Found {len(urls)} job URLs on page {page}")

            for index, job_url in enumerate(urls):
                if check_fetcher_status(auth_headers) != 'running':
                    logger.info("Fetcher stopped during job processing")
                    print("Fetcher stopped by user. Exiting.")
                    break
                job_data = scrape_job_details(job_url, auth_headers)
                if not job_data:
                    logger.error(f"Failed to scrape job: {job_url}")
                    print(f"Job (URL: {job_url}) failed to scrape")
                    failure_count += 1
                    total_jobs += 1
                    continue

                job_dict = {
                    "job_title": job_data[0], "company_logo": job_data[1], "company_name": job_data[2],
                    "company_url": job_data[3], "location": job_data[4], "environment": job_data[5],
                    "job_type": job_data[6], "level": job_data[7], "job_functions": job_data[8],
                    "industries": job_data[9], "job_description": job_data[10], "job_url": job_data[11],
                    "company_details": job_data[12], "company_website_url": job_data[13],
                    "company_industry": job_data[14], "company_size": job_data[15],
                    "company_headquarters": job_data[16], "company_type": job_data[17],
                    "company_founded": job_data[18], "company_specialties": job_data[19],
                    "company_address": job_data[20], "application_url": job_data[21],
                    "description_application_info": job_data[22], "resolved_application_info": job_data[23],
                    "final_application_email": job_data[24], "final_application_url": job_data[25],
                    "job_salary": ""
                }

                job_title = job_dict.get("job_title", "Unknown Job")
                company_name = job_dict.get("company_name", "")
                job_id = generate_job_id(job_title, company_name)

                if job_id in processed_ids:
                    logger.info(f"Skipping already processed job: {job_id} ({job_title} at {company_name})")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed")
                    total_jobs += 1
                    continue
                if not company_name or company_name.lower() == "unknown":
                    logger.info(f"Skipping job with unknown company: {job_title} (ID: {job_id})")
                    print(f"Job '{job_title}' (ID: {job_id}) skipped - unknown company")
                    failure_count += 1
                    total_jobs += 1
                    continue

                total_jobs += 1
                company_id, company_url = save_company_to_wordpress(index, job_dict, auth_headers)
                if company_id is None and has_full_access():
                    logger.error(f"Failed to save company for job {job_title}")
                    failure_count += 1
                    continue

                job_post_id, job_post_url = save_article_to_wordpress(index, job_dict, company_id, auth_headers)
                if job_post_id is None:
                    logger.error(f"Failed to save job {job_title}")
                    failure_count += 1
                    continue

                processed_ids.add(job_id)
                save_processed_id(job_id)
                logger.info(f"Processed and saved job: {job_id} - {job_title} at {company_name}")
                print(f"Job '{job_title}' at {company_name} (ID: {job_id}) posted to WordPress. Post ID: {job_post_id}, URL: {job_post_url}")
                success_count += 1

            save_last_page(page)
        except requests.RequestException as e:
            logger.error(f"Error fetching page {url}: {str(e)}")
            print(f"Error fetching page {url}: {str(e)}")
            failure_count += 1

    logger.info(f"Crawl completed. Total jobs: {total_jobs}, Success: {success_count}, Failures: {failure_count}")
    print(f"\n--- Summary ---\nTotal jobs processed: {total_jobs}\nSuccessfully posted: {success_count}\nFailed to post or scrape: {failure_count}")

def main() -> None:
    """Main function to run the scraper."""
    if not validate_environment():
        return
    if not fetch_credentials():
        logger.error("Cannot proceed without valid WordPress credentials")
        print("Error: Cannot proceed without valid WordPress credentials")
        return

    auth_string = f"{WP_USERNAME}:{WP_APP_PASSWORD}"
    auth_headers = {"Authorization": f"Basic {base64.b64encode(auth_string.encode()).decode()}"}
    processed_ids = load_processed_ids()
    crawl(auth_headers, processed_ids)

if __name__ == "__main__":
    main()
