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

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Constants for WordPress
WP_BASE_URL = os.getenv('WP_SITE_URL', 'https://mauritius.mimusjobs.com')
WP_SAVE_JOB_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/save-job"
WP_SAVE_COMPANY_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/save-company"
WP_CHECK_JOB_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/check-job"
WP_CREDENTIALS_URL = f"{WP_BASE_URL}/wp-json/fetcher/v1/get-credentials"
PROCESSED_IDS_FILE = "mauritius_processed_job_ids.csv"
LAST_PAGE_FILE = "last_processed_page.txt"
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

# Valid license key for full data scraping
VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/job-fetcher'

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
    return ' '.join(text.split())

def normalize_for_deduplication(text):
    """Normalize text for deduplication by removing spaces, punctuation, and converting to lowercase."""
    text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
    text = re.sub(r'\s+', '', text)      # Remove all whitespace
    return text.lower()

def generate_job_id(job_title, company_name):
    """Generate a unique job ID based on job title and company name."""
    combined = f"{job_title}_{company_name}"
    return hashlib.md5(combined.encode()).hexdigest()[:16]

def split_paragraphs(text, max_length=200):
    """Split large paragraphs into smaller ones, each up to max_length characters."""
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

def get_credentials():
    """Retrieve WordPress credentials from the /get-credentials endpoint."""
    try:
        response = requests.get(WP_CREDENTIALS_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.error(f"Failed to retrieve credentials: {data.get('message', 'Unknown error')}")
            return None, None, None
        return (
            data.get('wp_username', ''),
            data.get('wp_app_password', ''),
            data.get('license_key', '')
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching credentials from {WP_CREDENTIALS_URL}: {str(e)}")
        return None, None, None

def check_existing_job(job_id, auth_headers):
    """Check if a job with the given job_id exists using the plugin's endpoint."""
    check_url = f"{WP_CHECK_JOB_URL}?job_id={job_id}"
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.info(f"Job {job_id} does not exist on WordPress")
            return None, None
        logger.info(f"Found existing job {job_id} on WordPress: Post ID {data.get('id')}, URL {data.get('link')}")
        return data.get('id'), data.get('link')
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check job {job_id}: {str(e)}")
        return None, None

def save_company_to_wordpress(company_data, wp_headers, licensed):
    """Save company data to WordPress using the plugin's endpoint."""
    company_name = company_data.get("company_name", "")
    company_data = {
        "company_id": generate_job_id(company_name, company_name),  # Use company name as unique identifier
        "company_name": sanitize_text(company_name),
        "company_details": company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_logo": company_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else ""),
        "company_website": company_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": company_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else ""),
        "company_founded": company_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else ""),
        "company_type": company_data.get("company_type", UNLICENSED_MESSAGE if not licensed else ""),
        "company_address": company_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_specialties": company_data.get("company_specialties", UNLICENSED_MESSAGE if not licensed else ""),
        "company_headquarters": company_data.get("company_headquarters", UNLICENSED_MESSAGE if not licensed else ""),
        "company_size": company_data.get("company_size", UNLICENSED_MESSAGE if not licensed else "")
    }
    try:
        response = requests.post(WP_SAVE_COMPANY_URL, json=company_data, headers=wp_headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.error(f"Failed to save company {company_name}: {data.get('message', 'Unknown error')}")
            return None, None
        logger.info(f"Successfully saved company {company_name}: ID {data.get('id')}, URL {data.get('link')}")
        return data.get('id'), data.get('link')
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, None

def save_article_to_wordpress(job_data, company_id, auth_headers, licensed):
    """Save job data to WordPress using the plugin's endpoint."""
    job_title = job_data.get("job_title", "")
    job_id = generate_job_id(job_title, job_data.get("company_name", ""))
    
    # Check if job exists
    existing_post_id, existing_post_url = check_existing_job(job_id, auth_headers)
    if existing_post_id:
        logger.info(f"Skipping duplicate job: {job_id} ({job_title})")
        print(f"Job '{job_title}' (ID: {job_id}) skipped - already posted. ID: {existing_post_id}, URL: {existing_post_url}")
        return existing_post_id, existing_post_url

    job_data_post = {
        "job_id": job_id,
        "job_title": sanitize_text(job_title),
        "job_description": job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else ""),
        "job_location": job_data.get("location", "Mauritius"),
        "job_type": job_data.get("job_type", ""),
        "application": job_data.get("final_application_url", "") or job_data.get("final_application_email", ""),
        "company_id": str(company_id) if company_id else "",
        "company_name": sanitize_text(job_data.get("company_name", "")),
        "company_website": job_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else ""),
        "company_logo": job_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else ""),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": job_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else ""),
        "company_founded": job_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else ""),
        "company_type": job_data.get("company_type", UNLICENSED_MESSAGE if not licensed else ""),
        "company_address": job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_specialties": job_data.get("company_specialties", UNLICENSED_MESSAGE if not licensed else ""),
        "company_headquarters": job_data.get("company_headquarters", UNLICENSED_MESSAGE if not licensed else ""),
        "company_size": job_data.get("company_size", UNLICENSED_MESSAGE if not licensed else ""),
        "environment": job_data.get("environment", UNLICENSED_MESSAGE if not licensed else ""),
        "job_salary": job_data.get("job_salary", ""),
        "level": job_data.get("level", UNLICENSED_MESSAGE if not licensed else ""),
        "job_functions": job_data.get("job_functions", UNLICENSED_MESSAGE if not licensed else ""),
        "industries": job_data.get("industries", UNLICENSED_MESSAGE if not licensed else "")
    }
    
    try:
        response = requests.post(WP_SAVE_JOB_URL, json=job_data_post, headers=auth_headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logger.error(f"Failed to save job {job_title}: {data.get('message', 'Unknown error')}")
            return None, None
        logger.info(f"Successfully saved job {job_title}: ID {data.get('id')}, URL {data.get('link')}")
        return data.get('id'), data.get('link')
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to save job {job_title}: {str(e)}")
        return None, None

def load_processed_ids():
    """Load processed job IDs from file."""
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(line.strip() for line in f if line.strip())
            logger.info(f"Loaded {len(processed_ids)} processed job IDs from {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"Failed to load processed IDs from {PROCESSED_IDS_FILE}: {str(e)}")
    return processed_ids

def save_processed_id(job_id):
    """Append a single job ID to the processed IDs file."""
    try:
        with open(PROCESSED_IDS_FILE, "a") as f:
            f.write(f"{job_id}\n")
        logger.info(f"Saved job ID {job_id} to {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"Failed to save job ID {job_id} to {PROCESSED_IDS_FILE}: {str(e)}")

def load_last_page():
    """Load the last processed page number."""
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                page = int(f.read().strip())
                logger.info(f"Loaded last processed page: {page}")
                return page
    except Exception as e:
        logger.error(f"Failed to load last page from {LAST_PAGE_FILE}: {str(e)}")
    return 0

def save_last_page(page):
    """Save the last processed page number."""
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"Saved last processed page: {page} to {LAST_PAGE_FILE}")
    except Exception as e:
        logger.error(f"Failed to save last page to {LAST_PAGE_FILE}: {str(e)}")

def scrape_job_details(job_url, licensed):
    """Scrape job and company details from a LinkedIn job page."""
    logger.info(f'Fetching job details from: {job_url}')
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        logger.info(f'Scraped Job Title: {job_title}')

        company_logo = UNLICENSED_MESSAGE
        if licensed:
            company_logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            company_logo = (company_logo_elem.get('data-delayed-url') or company_logo_elem.get('src') or '') if company_logo_elem else ''
            logger.info(f'Scraped Company Logo URL: {company_logo}')

        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        logger.info(f'Scraped Company Name: {company_name}')

        company_url = UNLICENSED_MESSAGE
        if licensed:
            company_url_elem = soup.select_one(".topcard__org-name-link")
            company_url = company_url_elem['href'] if company_url_elem and company_url_elem.get('href') else ''
            if company_url:
                company_url = re.sub(r'\?.*$', '', company_url)
                logger.info(f'Scraped Company URL: {company_url}')

        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Mauritius'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f'Deduplicated location for {job_title}: {location}')

        environment = UNLICENSED_MESSAGE
        if licensed:
            env_element = soup.select(".topcard__flavor--metadata")
            for elem in env_element:
                text = elem.get_text().strip().lower()
                if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                    environment = elem.get_text().strip()
                    break
            logger.info(f'Scraped Environment: {environment}')

        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        logger.info(f'Scraped Type: {job_type}')

        level = UNLICENSED_MESSAGE
        if licensed:
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            logger.info(f'Scraped Level: {level}')

        job_functions = UNLICENSED_MESSAGE
        if licensed:
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
            logger.info(f'Scraped Job Functions: {job_functions}')

        industries = UNLICENSED_MESSAGE
        if licensed:
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            logger.info(f'Scraped Industries: {industries}')

        job_description = UNLICENSED_MESSAGE
        if licensed:
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                paragraphs = description_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    seen = set()
                    unique_paragraphs = []
                    for p in paragraphs:
                        para = sanitize_text(p.get_text().strip())
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                    job_description = split_paragraphs('\n\n'.join(unique_paragraphs))
                else:
                    raw_text = description_container.get_text(separator='\n').strip()
                    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                    seen = set()
                    unique_paragraphs = []
                    for para in paragraphs:
                        para = sanitize_text(para)
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                    job_description = split_paragraphs('\n\n'.join(unique_paragraphs))
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                logger.info(f'Scraped Job Description (length): {len(job_description)}')
            else:
                logger.warning(f"No job description container found for {job_title}")

        description_application_info = UNLICENSED_MESSAGE
        description_application_url = UNLICENSED_MESSAGE
        if licensed and job_description and job_description != UNLICENSED_MESSAGE:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
                logger.info(f'Found email in job description: {description_application_info}')
            else:
                links = description_container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                        description_application_url = href
                        description_application_info = href
                        logger.info(f'Found application link in job description: {description_application_info}')
                        break

        application_url = UNLICENSED_MESSAGE
        if licensed:
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
            logger.info(f'Scraped Application URL: {application_url}')

        resolved_application_info = UNLICENSED_MESSAGE
        resolved_application_url = UNLICENSED_MESSAGE
        if licensed and application_url and application_url != UNLICENSED_MESSAGE:
            try:
                time.sleep(5)
                resp_app = session.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                resolved_application_url = resp_app.url
                logger.info(f'Resolved Application URL: {resolved_application_url}')
                app_soup = BeautifulSoup(resp_app.text, 'html.parser')
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                emails = re.findall(email_pattern, resp_app.text)
                if emails:
                    resolved_application_info = emails[0]
                    logger.info(f'Found email in application page: {resolved_application_info}')
                else:
                    links = app_soup.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                            resolved_application_info = href
                            logger.info(f'Found application link in application page: {resolved_application_info}')
                            break
            except Exception as e:
                logger.error(f'Failed to follow application URL redirect: {str(e)}')
                resolved_application_url = description_application_url if description_application_url != UNLICENSED_MESSAGE else application_url

        final_application_email = description_application_info if description_application_info and '@' in description_application_info else ''
        final_application_url = description_application_url if description_application_url != UNLICENSED_MESSAGE else resolved_application_url
        if licensed:
            if final_application_email and resolved_application_info and '@' in resolved_application_info:
                final_application_email = resolved_application_info
            elif resolved_application_info and '@' in resolved_application_info:
                final_application_email = resolved_application_info
            if resolved_application_url and resolved_application_url != UNLICENSED_MESSAGE:
                final_application_url = resolved_application_url

        company_details = UNLICENSED_MESSAGE
        company_website_url = UNLICENSED_MESSAGE
        company_industry = UNLICENSED_MESSAGE
        company_size = UNLICENSED_MESSAGE
        company_headquarters = UNLICENSED_MESSAGE
        company_type = UNLICENSED_MESSAGE
        company_founded = UNLICENSED_MESSAGE
        company_specialties = UNLICENSED_MESSAGE
        company_address = UNLICENSED_MESSAGE

        if licensed and company_url and company_url != UNLICENSED_MESSAGE:
            logger.info(f'Fetching company page: {company_url}')
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')

                company_details_elem = company_soup.select_one("p.about-us__description") or company_soup.select_one("section.core-section-container > div > p")
                company_details = company_details_elem.get_text().strip() if company_details_elem else ''
                logger.info(f'Scraped Company Details: {company_details[:100] + "..." if company_details else ""}')

                company_website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a")
                company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                if 'linkedin.com/redir/redirect' in company_website_url:
                    parsed_url = urlparse(company_website_url)
                    query_params = parse_qs(parsed_url.query)
                    if 'url' in query_params:
                        company_website_url = unquote(query_params['url'][0])
                        logger.info(f'Extracted external company website from redirect: {company_website_url}')

                if company_website_url and 'linkedin.com' not in company_website_url:
                    try:
                        time.sleep(5)
                        resp_company_web = session.get(company_website_url, headers=headers, timeout=15, allow_redirects=True)
                        company_website_url = resp_company_web.url
                        logger.info(f'Resolved Company Website URL: {company_website_url}')
                    except Exception as e:
                        logger.error(f'Failed to resolve company website URL: {str(e)}')
                        company_website_url = ''

                def get_company_detail(label):
                    elements = company_soup.select("section.core-section-container.core-section-container--with-border > div > dl > div")
                    for elem in elements:
                        dt = elem.find("dt")
                        if dt and dt.get_text().strip().lower() == label.lower():
                            dd = elem.find("dd")
                            return dd.get_text().strip() if dd else ''
                    return ''

                company_industry = get_company_detail("Industry")
                company_size = get_company_detail("Company size")
                company_headquarters = get_company_detail("Headquarters")
                company_type = get_company_detail("Type")
                company_founded = get_company_detail("Founded")
                company_specialties = get_company_detail("Specialties")
                company_address = company_soup.select_one("#address-0")
                company_address = company_address.get_text().strip() if company_address else company_headquarters
                logger.info(f'Scraped Company Info: Industry={company_industry}, Size={company_size}, Headquarters={company_headquarters}, Type={company_type}, Founded={company_founded}, Specialties={company_specialties}, Address={company_address}')

            except Exception as e:
                logger.error(f'Error fetching company page: {company_url} - {str(e)}')

        return {
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
            "description_application_info": description_application_info,
            "resolved_application_info": resolved_application_info,
            "final_application_email": final_application_email,
            "final_application_url": final_application_url,
            "resolved_application_url": resolved_application_url,
            "job_salary": ""
        }

def crawl(auth_headers, processed_ids, licensed):
    """Crawl LinkedIn job listings and post to WordPress."""
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10

    for i in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords=&location=Mauritius&start={i * 25}'
        logger.info(f'Fetching job search page: {url}')
        time.sleep(random.uniform(5, 10))
        try:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            if "login" in response.url or "challenge" in response.url:
                logger.error("Login or CAPTCHA detected, stopping crawl")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list > li a")
            urls = [a['href'] for a in job_list if a.get('href')]
            logger.info(f'Found {len(urls)} job URLs on page: {url}')
            if not urls:
                logger.warning(f"No job URLs found on page {i}")

            for job_url in urls:
                job_data = scrape_job_details(job_url, licensed)
                if not job_data:
                    logger.error(f"No data scraped for job: {job_url}")
                    print(f"Job (URL: {job_url}) failed to scrape")
                    failure_count += 1
                    total_jobs += 1
                    continue

                job_title = job_data.get("job_title", "Unknown Job")
                company_name = job_data.get("company_name", "")
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
                company_id, company_url = save_company_to_wordpress(job_data, auth_headers, licensed)
                job_post_id, job_post_url = save_article_to_wordpress(job_data, company_id, auth_headers, licensed)

                if job_post_id:
                    processed_ids.add(job_id)
                    save_processed_id(job_id)
                    logger.info(f"Processed and saved job: {job_id} - {job_title} at {company_name}")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) posted to WordPress. ID: {job_post_id}, URL: {job_post_url}")
                    success_count += 1
                else:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed to post to WordPress")
                    failure_count += 1

            save_last_page(i + 1)

        except Exception as e:
            logger.error(f'Error fetching job search page: {url} - {str(e)}')
            failure_count += 1

    print("\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully posted: {success_count}")
    print(f"Failed to post or scrape: {failure_count}")

def main():
    """Main function to initiate crawling and posting."""
    license_key = sys.argv[1] if len(sys.argv) > 1 else ""
    licensed = license_key == VALID_LICENSE_KEY

    if not licensed:
        logger.warning("No valid license key provided. Scraping limited data.")
        print("Warning: No valid license key provided. Only basic job data will be scraped.")
    else:
        logger.info("Valid license key provided. Scraping full job data.")
        print("Valid license key provided. Scraping full job data.")

    # Retrieve credentials from the plugin
    wp_username, wp_app_password, retrieved_license_key = get_credentials()
    if not wp_username or not wp_app_password:
        logger.error("Failed to retrieve valid WordPress credentials. Exiting.")
        print("Error: Could not retrieve WordPress credentials. Check plugin settings.")
        return

    # Override license_key if retrieved from credentials
    if retrieved_license_key:
        license_key = retrieved_license_key
        licensed = license_key == VALID_LICENSE_KEY
        logger.info(f"Using license key from credentials: {license_key}")

    auth_string = f"{wp_username}:{wp_app_password}"
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }

    processed_ids = load_processed_ids()
    crawl(auth_headers=wp_headers, processed_ids=processed_ids, licensed=licensed)

if __name__ == "__main__":
    main()
