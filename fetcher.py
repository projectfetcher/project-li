import os
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

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Constants
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
VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/jobfetcher'

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
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
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

def check_existing_job(job_id, wp_url, auth_headers):
    """Check if a job exists using the plugin's check-job endpoint."""
    check_url = urljoin(wp_url, '/wp-json/fetcher/v1/check-job')
    try:
        response = requests.get(check_url, headers=auth_headers, params={'job_id': job_id}, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get('success', False):
            logger.info(f"Job {job_id} already exists: {data.get('message')}")
            return data.get('id'), data.get('link')
        return None, None
    except requests.RequestException as e:
        logger.error(f"Failed to check job {job_id}: {str(e)}")
        return None, None

def save_company_to_wordpress(company_data, wp_url, auth_headers):
    """Save company data to WordPress via the plugin's save-company endpoint."""
    company_name = sanitize_text(company_data.get("company_name", ""))
    company_id = company_data.get("company_id", generate_job_id(company_name, company_name))
    company_data["company_id"] = company_id
    company_data["timestamp"] = int(time.time())

    check_url = urljoin(wp_url, '/wp-json/fetcher/v1/save-company')
    try:
        response = requests.post(check_url, headers=auth_headers, json=company_data, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('success', False):
            logger.info(f"Successfully saved company {company_name}: ID {data.get('id')}, URL {data.get('link')}")
            return data.get('id'), data.get('link')
        else:
            logger.error(f"Failed to save company {company_name}: {data.get('message')}")
            return None, None
    except requests.RequestException as e:
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, None

def save_job_to_wordpress(job_data, company_id, wp_url, auth_headers):
    """Save job data to WordPress via the plugin's save-job endpoint."""
    job_title = sanitize_text(job_data.get("job_title", ""))
    job_id = job_data.get("job_id", generate_job_id(job_title, job_data.get("company_name", "")))
    job_data["job_id"] = job_id
    job_data["timestamp"] = int(time.time())
    job_data["company_id"] = str(company_id) if company_id else ""

    existing_post_id, existing_post_url = check_existing_job(job_id, wp_url, auth_headers)
    if existing_post_id:
        logger.info(f"Skipping duplicate job: {job_id} ({job_title})")
        print(f"Job '{job_title}' (ID: {job_id}) skipped - already posted. Post ID: {existing_post_id}, URL: {existing_post_url}")
        return existing_post_id, existing_post_url

    post_url = urljoin(wp_url, '/wp-json/fetcher/v1/save-job')
    try:
        response = requests.post(post_url, headers=auth_headers, json=job_data, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get('success', False):
            logger.info(f"Successfully posted job {job_title}: Post ID {data.get('id')}, URL {data.get('link')}")
            return data.get('id'), data.get('link')
        else:
            logger.error(f"Failed to post job {job_title}: {data.get('message')}")
            return None, None
    except requests.RequestException as e:
        logger.error(f"Failed to post job {job_title}: {str(e)}")
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
        if "login" in response.url or "challenge" in response.url:
            logger.error("Login or CAPTCHA detected, skipping job")
            return None
        soup = BeautifulSoup(response.text, 'html.parser')

        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = sanitize_text(job_title.get_text().strip()) if job_title else ''
        logger.info(f'Scraped Job Title: {job_title}')

        company_logo = ''
        if licensed:
            company_logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            company_logo = (company_logo_elem.get('data-delayed-url') or company_logo_elem.get('src') or '') if company_logo_elem else ''
            logger.info(f'Scraped Company Logo URL: {company_logo}')
        else:
            company_logo = ''

        company_name = soup.select_one(".topcard__org-name-link")
        company_name = sanitize_text(company_name.get_text().strip()) if company_name else ''
        logger.info(f'Scraped Company Name: {company_name}')

        company_url = ''
        if licensed:
            company_url_elem = soup.select_one(".topcard__org-name-link")
            company_url = company_url_elem['href'] if company_url_elem and company_url_elem.get('href') else ''
            if company_url:
                company_url = re.sub(r'\?.*$', '', company_url)
                logger.info(f'Scraped Company URL: {company_url}')
            else:
                logger.info('No Company URL found')
        else:
            company_url = ''

        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = sanitize_text(location.get_text().strip()) if location else 'Mauritius'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f'Deduplicated location for {job_title}: {location}')

        environment = ''
        if licensed:
            env_element = soup.select(".topcard__flavor--metadata")
            for elem in env_element:
                text = elem.get_text().strip().lower()
                if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                    environment = elem.get_text().strip()
                    break
            logger.info(f'Scraped Environment: {environment}')
        else:
            environment = ''

        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        job_type = JOB_TYPE_MAPPING.get(job_type, job_type)
        logger.info(f'Scraped Job Type: {job_type}')

        level = ''
        if licensed:
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = sanitize_text(level_elem.get_text().strip()) if level_elem else ''
            logger.info(f'Scraped Level: {level}')
        else:
            level = ''

        job_functions = ''
        if licensed:
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = sanitize_text(job_functions_elem.get_text().strip()) if job_functions_elem else ''
            logger.info(f'Scraped Job Functions: {job_functions}')
        else:
            job_functions = ''

        industries = ''
        if licensed:
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = sanitize_text(industries_elem.get_text().strip()) if industries_elem else ''
            logger.info(f'Scraped Industries: {industries}')
        else:
            industries = ''

        job_description = ''
        if licensed:
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                paragraphs = description_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"Raw paragraphs for {job_title}: {[sanitize_text(p.get_text().strip())[:50] for p in paragraphs if p.get_text().strip()]}")
                    for p in paragraphs:
                        para = sanitize_text(p.get_text().strip())
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                        elif norm_para:
                            logger.info(f"Removed duplicate paragraph in job description for {job_title}: {para[:50]}...")
                    job_description = '\n\n'.join(unique_paragraphs)
                else:
                    raw_text = description_container.get_text(separator='\n').strip()
                    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"Raw text paragraphs for {job_title}: {[sanitize_text(para)[:50] for para in paragraphs]}")
                    for para in paragraphs:
                        para = sanitize_text(para)
                        if not para:
                            continue
                        norm_para = normalize_for_deduplication(para)
                        if norm_para and norm_para not in seen:
                            unique_paragraphs.append(para)
                            seen.add(norm_para)
                        elif norm_para:
                            logger.info(f"Removed duplicate paragraph in job description for {job_title}: {para[:50]}...")
                    job_description = '\n\n'.join(unique_paragraphs)
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
                logger.info(f'Scraped Job Description (length): {len(job_description)}, Paragraphs: {len(job_description.split(delimiter))}')
            else:
                logger.warning(f"No job description container found for {job_title}")
        else:
            job_description = ''

        application_url = ''
        if licensed:
            application_anchor = soup.select_one("#teriary-cta-container > div > a")
            application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else ''
            logger.info(f'Scraped Application URL: {application_url}')
        else:
            application_url = ''

        description_application_info = ''
        if licensed and job_description:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
                logger.info(f'Found email in job description: {description_application_info}')
            else:
                links = description_container.find_all('a', href=True) if description_container else []
                for link in links:
                    href = link['href']
                    if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                        description_application_info = href
                        logger.info(f'Found application link in job description: {description_application_info}')
                        break
        else:
            description_application_info = ''

        resolved_application_url = ''
        if licensed and application_url:
            try:
                time.sleep(5)
                resp_app = session.get(application_url, headers=headers, timeout=15, allow_redirects=True)
                resolved_application_url = resp_app.url
                logger.info(f'Resolved Application URL: {resolved_application_url}')
            except Exception as e:
                logger.error(f'Failed to follow application URL redirect: {str(e)}')
                resolved_application_url = application_url

        application = description_application_info if description_application_info and '@' in description_application_info else resolved_application_url or application_url

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
            logger.info(f'Fetching company page: {company_url}')
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')

                company_details_elem = company_soup.select_one("p.about-us__description") or company_soup.select_one("section.core-section-container > div > p")
                company_details = sanitize_text(company_details_elem.get_text().strip()) if company_details_elem else ''
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
                else:
                    company_website_url = ''

                def get_company_detail(label):
                    elements = company_soup.select("section.core-section-container.core-section-container--with-border > div > dl > div")
                    for elem in elements:
                        dt = elem.find("dt")
                        if dt and dt.get_text().strip().lower() == label.lower():
                            dd = elem.find("dd")
                            return sanitize_text(dd.get_text().strip()) if dd else ''
                    return ''

                company_industry = get_company_detail("Industry")
                company_size = get_company_detail("Company size")
                company_headquarters = get_company_detail("Headquarters")
                company_type = get_company_detail("Type")
                company_founded = get_company_detail("Founded")
                company_specialties = get_company_detail("Specialties")
                company_address = company_soup.select_one("#address-0")
                company_address = sanitize_text(company_address.get_text().strip()) if company_address else company_headquarters
            except Exception as e:
                logger.error(f'Error fetching company page: {company_url} - {str(e)}')

        job_data = {
            "job_id": job_id,
            "job_title": job_title,
            "company_name": company_name,
            "job_location": location,
            "job_type": job_type,
            "job_description": job_description,
            "application": application,
            "job_listing_type": [job_type] if job_type else [],
            "job_listing_region": [location] if location else [],
            "job_salary": "",
            "company_website": company_website_url,
            "company_logo": company_logo,
            "company_details": company_details,
            "company_industry": company_industry,
            "company_founded": company_founded,
            "company_address": company_address
        }

        company_data = {
            "company_id": generate_job_id(company_name, company_name),
            "company_name": company_name,
            "company_details": company_details,
            "company_website": company_website_url,
            "company_industry": company_industry,
            "company_founded": company_founded,
            "company_address": company_address
        }

        return job_data, company_data

    except Exception as e:
        logger.error(f'Error in scrape_job_details for {job_url}: {str(e)}')
        return None, None

def crawl(wp_url, auth_headers, processed_ids, licensed):
    """Crawl LinkedIn job pages and save data to WordPress."""
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
                logger.warning(f"No job URLs found on page {i}. Possible selector issue or no jobs available.")

            for index, job_url in enumerate(urls):
                job_data, company_data = scrape_job_details(job_url, licensed)
                if not job_data or not company_data:
                    logger.error(f"No data scraped for job: {job_url}")
                    print(f"Job (URL: {job_url}) failed to scrape: No data returned")
                    failure_count += 1
                    total_jobs += 1
                    continue

                job_title = job_data.get("job_title", "Unknown Job")
                company_name = job_data.get("company_name", "")
                job_id = job_data["job_id"]

                if job_id in processed_ids:
                    logger.info(f"Skipping already processed job: {job_id} ({job_title} at {company_name})")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed.")
                    total_jobs += 1
                    continue

                if not company_name or company_name.lower() == "unknown":
                    logger.info(f"Skipping job with unknown company: {job_title} (ID: {job_id})")
                    print(f"Job '{job_title}' (ID: {job_id}) skipped - unknown company")
                    failure_count += 1
                    total_jobs += 1
                    continue

                total_jobs += 1

                company_id, company_url = save_company_to_wordpress(company_data, wp_url, auth_headers)
                job_post_id, job_post_url = save_job_to_wordpress(job_data, company_id, wp_url, auth_headers)

                if job_post_id:
                    processed_ids.add(job_id)
                    save_processed_id(job_id)
                    logger.info(f"Processed and saved job: {job_id} - {job_title} at {company_name}")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) successfully posted to WordPress. Post ID: {job_post_id}, URL {job_post_url}")
                    success_count += 1
                else:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed to post to WordPress. Check logs for details.")
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
    """Main function to run the job fetcher."""
    # Load configuration from environment variables
    wp_url = os.getenv('WP_SITE_URL', 'https://mauritius.mimusjobs.com')
    wp_username = os.getenv('WP_USERNAME', 'mary')
    wp_app_password = os.getenv('WP_APP_PASSWORD', 'Piab Mwog pfiq pdfK BOGH hDEy')
    license_key = os.getenv('LICENSE_KEY', '')

    # Validate license key (optional)
    licensed = True  # Full access regardless of license key
    if license_key:
        licensed = license_key == VALID_LICENSE_KEY
        if licensed:
            logger.info("Valid license key provided. Scraping full job data.")
            print("Valid license key provided. Scraping full job data.")
        else:
            logger.warning("Invalid license key provided, but proceeding with full data scraping.")
            print("Warning: Invalid license key provided, but proceeding with full data scraping.")
    else:
        logger.info("No license key provided, proceeding with full data scraping.")
        print("No license key provided, proceeding with full data scraping.")

    # Set up authentication headers
    auth_string = f"{wp_username}:{wp_app_password}"
    auth = base64.b64encode(auth_string.encode()).decode()
    wp_headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }

    processed_ids = load_processed_ids()
    crawl(wp_url, wp_headers, processed_ids, licensed)

if __name__ == "__main__":
    main()
