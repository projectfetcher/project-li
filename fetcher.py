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
import nltk
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from sentence_transformers import SentenceTransformer, util
import language_tool_python
import warnings

# Set CUDA_LAUNCH_BLOCKING for debugging
os.environ["CUDA_LAUNCH_BLOCKING"] = "1"

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.StreamHandler(),
    logging.FileHandler('fetcher.log')
])
logger = logging.getLogger(__name__)

# Suppress insecure request warnings
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Download NLTK resources
try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab')
try:
    nltk.data.find('taggers/averaged_perceptron_tagger')
except LookupError:
    nltk.download('averaged_perceptron_tagger')

# Initialize language tool
tool = language_tool_python.LanguageTool('en-US')

# Initialize model and tokenizer
device = torch.device("cpu")
model_name = "google/flan-t5-large"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
model.eval()
model.to(device)

# Initialize sentence transformer
similarity_model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}

# Environment variables
logger.debug("Loading environment variables")
WP_SITE_URL = os.getenv('WP_SITE_URL')
WP_USERNAME = os.getenv('WP_USERNAME')
WP_APP_PASSWORD = os.getenv('WP_APP_PASSWORD')
COUNTRY = os.getenv('COUNTRY')
KEYWORD = os.getenv('KEYWORD', '')
FETCHER_TOKEN = os.getenv('FETCHER_TOKEN', '')
logger.debug(f"Environment variables: WP_SITE_URL={WP_SITE_URL}, WP_USERNAME={WP_USERNAME}, WP_APP_PASSWORD={'***' if WP_APP_PASSWORD else None}, COUNTRY={COUNTRY}, KEYWORD={KEYWORD}, FETCHER_TOKEN={'***' if FETCHER_TOKEN else None}")

# WordPress endpoints
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

# Constants for paraphrasing
MAX_TOTAL_TOKENS = 3000
MAX_RETURN_SEQUENCES = 4

def sanitize_text(text, is_url=False, is_email=False):
    """Sanitize input text by removing unwanted characters and normalizing."""
    if not isinstance(text, str):
        text = str(text)
    text = text.strip()
    if not text:
        return ""
    if is_url or is_email:
        text = re.sub(r'[\r\t\f\v]', '', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text).strip()
        return text
    text = re.sub(r'#+\s*', '', text)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'—+', '', text)
    text = re.sub(r'←+', '', text)
    text = re.sub(r'[^\x20-\x7E\n\u00C0-\u017F]', '', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text).strip()
    if not is_url:
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
        text = re.sub(r'(\w)(\w)', r'\1 \2', text) if re.match(r'^\w+$', text) else text
    return text

def clean_description(text):
    """Clean paraphrased text using LanguageTool for grammar and style."""
    try:
        matches = tool.check(text)
        corrected_text = language_tool_python.utils.correct(text, matches)
        return corrected_text
    except Exception as e:
        logger.error(f"Error in grammar correction: {str(e)}")
        return text

def is_good_paraphrase(original: str, candidate: str) -> float:
    """Calculate cosine similarity between original and paraphrased text."""
    try:
        embeddings = similarity_model.encode([original, candidate], convert_to_tensor=True)
        sim_score = util.pytorch_cos_sim(embeddings[0], embeddings[1]).item()
        return sim_score
    except Exception as e:
        logger.error(f"Error computing similarity: {str(e)}")
        return 0.0

def is_grammatically_correct(text):
    """Check if text is grammatically correct with minimal issues."""
    matches = tool.check(text)
    return len(matches) < 3

def extract_nouns(text):
    """Extract nouns (NN, NNS, NNP, NNPS) from text using NLTK POS tagging."""
    try:
        tokens = nltk.word_tokenize(text)
        tagged = nltk.pos_tag(tokens)
        nouns = [word for word, pos in tagged if pos in ['NN', 'NNS', 'NNP', 'NNPS']]
        return nouns
    except Exception as e:
        logger.error(f"Error extracting nouns from {text}: {str(e)}")
        return []

def contains_nouns(paraphrase, required_nouns):
    """Check if the paraphrase contains all required nouns."""
    if not required_nouns:
        return True
    paraphrase_lower = paraphrase.lower()
    return all(noun.lower() in paraphrase_lower for noun in required_nouns)

def extract_capitalized_words(text):
    """Extract words with specific capitalization from the input text."""
    words = re.findall(r'\b[A-Z][a-zA-Z]*\b', text)
    return {word.lower(): word for word in words if len(word) > 1}

def restore_capitalization(paraphrased, capitalized_words):
    """Restore original capitalization of specified words in the paraphrased text."""
    result = paraphrased
    for lower_word, orig_word in capitalized_words.items():
        pattern = r'\b' + re.escape(lower_word) + r'\b'
        result = re.sub(pattern, orig_word, result, flags=re.IGNORECASE)
    return result

def paraphrase_strict_title(title, max_attempts=3, max_sub_attempts=2):
    """Paraphrase job title while preserving meaning and key nouns."""
    def has_repetitions(text):
        tokens = text.lower().split()
        seen = set()
        for i in range(len(tokens) - 2):
            ngram = tuple(tokens[i:i + 3])
            if ngram in seen:
                return True
            seen.add(ngram)
        return False

    def contains_banned_phrase(text, banned_list):
        critical_phrases = [
            "Rewrite the following", "Paraphrased title", "Professionally rewrite",
            "Keep it short", "Use different phrasing", "Short (5–12 words)",
            "Paraphrase", "Paraphrased", "Paraphrasing", "Paraphrased version",
            "Summary", "Summarised", "Summarized", "Summarizing", "Summarising",
            "None.", "None", "none", ".", ":"
        ]
        text_lower = text.lower()
        for phrase in critical_phrases:
            if phrase.lower() in text_lower:
                return True, phrase, text
        return False, None, None

    clean_title = sanitize_text(title)
    if not clean_title:
        logger.error("Input title is empty after sanitization.")
        return title

    nouns = extract_nouns(clean_title)
    capitalized_words = extract_capitalized_words(clean_title)
    nouns_str = ", ".join(nouns) if nouns else "none"
    logger.debug(f"Extracted nouns from title '{clean_title}': {nouns}")

    prompt = (
        f"Rewrite the following job title professionally, using different phrasing while preserving the meaning. "
        f"Keep it short (5–12 words) and avoid duplicating words. "
        f"Preserve the following nouns exactly as they are: {nouns_str}.\n{clean_title}"
    )

    encoding = tokenizer.encode_plus(
        prompt,
        return_tensors="pt",
        truncation=True,
        max_length=MAX_TOTAL_TOKENS
    ).to(device)

    target_word_count = len(clean_title.split())
    min_wc = max(1, int(target_word_count * 0.6))
    max_wc = min(12, int(target_word_count * 1.4))

    best_paraphrase = None
    best_score = -1

    for attempt in range(max_attempts):
        sub_attempt = 0
        while sub_attempt < max_sub_attempts:
            try:
                with torch.no_grad():
                    output = model.generate(
                        input_ids=encoding['input_ids'],
                        attention_mask=encoding['attention_mask'],
                        max_new_tokens=60,
                        do_sample=True,
                        top_k=40,
                        top_p=0.95,
                        temperature=0.8 + 0.1 * sub_attempt,
                        repetition_penalty=1.2,
                        no_repeat_ngram_size=3,
                        num_return_sequences=MAX_RETURN_SEQUENCES
                    )

                decoded_outputs = [
                    tokenizer.decode(seq, skip_special_tokens=True).strip()
                    for seq in output
                ]

                for d in decoded_outputs:
                    paraphrased = d.replace(prompt, "").strip() if prompt in d else d.strip()
                    paraphrased = clean_description(paraphrased)
                    paraphrased = restore_capitalization(paraphrased, capitalized_words)

                    if not paraphrased or len(paraphrased.split()) < 1:
                        continue

                    is_banned, banned_phrase, _ = contains_banned_phrase(paraphrased, [])
                    if is_banned:
                        continue
                    if has_repetitions(paraphrased):
                        continue
                    if not is_grammatically_correct(paraphrased):
                        continue
                    if not contains_nouns(paraphrased, nouns):
                        continue

                    sim = is_good_paraphrase(title, paraphrased)
                    wc = len(paraphrased.split())
                    length_penalty = abs(wc - target_word_count) / max(target_word_count, 1)
                    score = (sim + (1 - length_penalty)) / 2
                    first_diff = not paraphrased.lower().startswith(title.lower())

                    if min_wc <= wc <= max_wc and sim >= 0.6 and first_diff and score > best_score:
                        best_score = score
                        best_paraphrase = paraphrased

                sub_attempt += 1
                time.sleep(0.5 * (2 ** sub_attempt))

            except Exception as e:
                logger.error(f"Error during title paraphrase attempt {attempt + 1}.{sub_attempt + 1}: {str(e)}")
                sub_attempt += 1
                time.sleep(0.5 * (2 ** sub_attempt))

        time.sleep(1)

    return best_paraphrase if best_paraphrase else clean_title

def paraphrase_strict_description(text, max_attempts=2, max_sub_attempts=2):
    """Paraphrase job description while preserving key details."""
    def contains_prompt(para):
        prompt_phrases = [
            "Rephrase the following job description paragraph",
            "Rephrase the job description",
            "Paragraph professionally, preserving all key details",
            "Rewrite the following",
            "Rephrase the paragraph below",
            "Rephrase the following job description",
            "Preserving all key details",
            "Tone and structure",
            "Keep the length approximately the same",
            "Job description paragraph professionally",
            "Paraphrase", "Paraphrased", "Paraphrase the following",
            "Paraphrase the job description", "Paraphrasing",
            "Job description", "Job description paragraph",
        ]
        para_lower = para.lower()
        for phrase in prompt_phrases:
            if phrase.lower() in para_lower:
                return True, phrase, para
        return False, None, None

    clean_text = sanitize_text(text)
    if not clean_text:
        return text

    capitalized_words = extract_capitalized_words(clean_text)
    paragraphs = [p.strip() for p in clean_text.split('\n\n') if p.strip()]
    final_paraphrased = []

    for idx, para in enumerate(paragraphs):
        prompt = (
            f"Rephrase the following job description paragraph professionally, preserving all key details, tone, and structure. "
            f"Keep the length approximately the same:\n{para}"
        )

        prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
        prompt_token_len = len(prompt_tokens)
        if prompt_token_len > MAX_TOTAL_TOKENS - 200:
            para = " ".join(para.split()[:int((MAX_TOTAL_TOKENS - 200) / 4)])
            prompt = (
                f"Rephrase the following job description paragraph professionally, preserving all key details, tone, and structure. "
                f"Keep the length approximately the same:\n{para}"
            )
            prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
            prompt_token_len = len(prompt_tokens)

        available_output_tokens = max(200, MAX_TOTAL_TOKENS - prompt_token_len)
        target_word_count = len(para.split())
        min_wc = int(target_word_count * 0.75)
        max_wc = int(target_word_count * 1.25)

        encoding = tokenizer.encode_plus(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=MAX_TOTAL_TOKENS
        ).to(device)

        best_paraphrase = None
        best_score = -1

        for attempt in range(max_attempts):
            sub_attempt = 0
            while sub_attempt < max_sub_attempts:
                try:
                    with torch.no_grad():
                        output = model.generate(
                            input_ids=encoding['input_ids'],
                            attention_mask=encoding['attention_mask'],
                            max_new_tokens=available_output_tokens,
                            do_sample=True,
                            top_k=40,
                            top_p=0.95,
                            temperature=0.9 + 0.1 * sub_attempt,
                            repetition_penalty=1.1,
                            no_repeat_ngram_size=2,
                            num_return_sequences=MAX_RETURN_SEQUENCES
                        )

                    decoded = [tokenizer.decode(seq, skip_special_tokens=True).strip() for seq in output]

                    for d in decoded:
                        paraphrased = d.replace(prompt, "").strip() if prompt in d else d.strip()
                        paraphrased = clean_description(paraphrased)
                        paraphrased = restore_capitalization(paraphrased, capitalized_words)

                        if not paraphrased or len(paraphrased.split()) < 5:
                            continue
                        is_banned, _, _ = contains_prompt(paraphrased)
                        if is_banned:
                            continue

                        word_count = len(paraphrased.split())
                        similarity = is_good_paraphrase(para, paraphrased)
                        score = (similarity + (1 - abs(word_count - target_word_count) / max(target_word_count, 1))) / 2
                        first_diff = not paraphrased.split(".")[0].strip().lower().startswith(para.split(".")[0].strip().lower())

                        if min_wc <= word_count <= max_wc and similarity >= 0.6 and first_diff and is_grammatically_correct(paraphrased):
                            final_paraphrased.append(clean_description(paraphrased))
                            sub_attempt = max_sub_attempts
                            break

                        if first_diff and score > best_score:
                            best_score = score
                            best_paraphrase = paraphrased

                    sub_attempt += 1
                    time.sleep(0.5 * (2 ** sub_attempt))

                except Exception as e:
                    logger.error(f"Error during description paraphrase attempt {attempt + 1}.{sub_attempt + 1}: {str(e)}")
                    sub_attempt += 1
                    time.sleep(0.5 * (2 ** sub_attempt))

            if len(final_paraphrased) > idx:
                break
            time.sleep(1)

        if len(final_paraphrased) <= idx:
            final_paraphrased.append(best_paraphrase if best_paraphrase else para)

    return "\n\n".join(final_paraphrased)

def paraphrase_strict_tagline(company_tagline, max_attempts=5):
    """Paraphrase company tagline into a crisp, professional summary."""
    clean_text = sanitize_text(company_tagline)
    if not clean_text:
        return company_tagline

    capitalized_words = extract_capitalized_words(clean_text)
    target_word_count = max(len(clean_text.split()), 8)
    min_word_count = 4
    max_word_count = 15

    rejected_phrases = [
        "Paraphrased tagline", "Rewrite the following", "Original tagline",
        "Professionally rewritten", "Crisp and impactful", "Summary:",
        "Short and professional", "Keep it short", "###", "Tagline:",
        "Output:", "Company summary", "Paraphrased version", "Rephrased version",
        "Paraphrase", "Paraphrased", "Paraphrasing", "Summarized", "Summarised",
        "Summarizing", "Summarising", "Summary"
    ]

    def contains_rejected_phrase(text):
        lower = text.lower()
        for bad_phrase in rejected_phrases:
            if bad_phrase.lower() in lower:
                return True, bad_phrase, text
        return False, None, None

    input_prompt = (
        f"Rewrite the following tagline into a crisp, professional, and meaningful summary. "
        f"Keep it short and impactful (5–12 words):\n\n"
        f"### Original ###\n{clean_text}\n\n### Paraphrased Tagline ###"
    )

    encoding = tokenizer.encode_plus(
        input_prompt,
        return_tensors="pt",
        truncation=True,
        max_length=min(len(tokenizer.encode(input_prompt, add_special_tokens=True)) + 50, 512)
    ).to(device)

    best_paraphrase = None
    best_score = -1

    for attempt in range(max_attempts):
        try:
            with torch.no_grad():
                outputs = model.generate(
                    input_ids=encoding['input_ids'],
                    attention_mask=encoding['attention_mask'],
                    max_new_tokens=25,
                    do_sample=True,
                    top_k=50,
                    top_p=0.9,
                    temperature=0.9,
                    repetition_penalty=1.2,
                    no_repeat_ngram_size=2,
                    num_return_sequences=6
                )

            decoded_outputs = [
                tokenizer.decode(seq, skip_special_tokens=True).strip()
                for seq in outputs
            ]

            for d in decoded_outputs:
                paraphrased = d.split("### Paraphrased Tagline ###")[1].strip() if "### Paraphrased Tagline ###" in d else d.strip()
                paraphrased = clean_description(paraphrased)
                paraphrased = restore_capitalization(paraphrased, capitalized_words)

                is_banned, _, _ = contains_rejected_phrase(paraphrased)
                if is_banned:
                    continue
                if not is_grammatically_correct(paraphrased):
                    continue

                word_count = len(paraphrased.split())
                if word_count < min_word_count or word_count > max_word_count:
                    continue

                similarity = is_good_paraphrase(clean_text, paraphrased)
                length_score = 1 - abs(target_word_count - word_count) / target_word_count
                score = similarity * 0.7 + length_score * 0.3
                first_diff = not paraphrased.split(".")[0].strip().lower().startswith(clean_text.split(".")[0].strip().lower())

                if first_diff and score > best_score:
                    best_score = score
                    best_paraphrase = paraphrased

            time.sleep(2 ** attempt)

        except Exception as e:
            logger.error(f"Error during tagline paraphrase attempt {attempt + 1}: {str(e)}")

    return best_paraphrase if best_paraphrase else clean_text

def fetch_credentials():
    """Fetch WordPress credentials from the REST API if not provided in environment."""
    global WP_USERNAME, WP_APP_PASSWORD
    logger.debug("Attempting to fetch WordPress credentials")
    if WP_USERNAME and WP_APP_PASSWORD:
        logger.info("Credentials provided via environment variables")
        return True
    try:
        response = requests.get(WP_CREDENTIALS_URL, timeout=5, verify=False)
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
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch credentials from {WP_CREDENTIALS_URL}: {str(e)}")
        return False

def check_fetcher_status(auth_headers):
    """Check the fetcher status from WordPress."""
    try:
        response = requests.get(WP_FETCHER_STATUS_URL, headers=auth_headers, timeout=5, verify=False)
        response.raise_for_status()
        status = response.json().get('status', 'stopped')
        logger.info(f"Fetcher status: {status}")
        return status
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check fetcher status: {str(e)}")
        return 'stopped'

def normalize_for_deduplication(text):
    """Normalize text for deduplication."""
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    return text.lower()

def generate_job_id(job_title, company_name):
    """Generate a unique job ID based on job title and company name."""
    combined = f"{job_title}_{company_name}"
    job_id = hashlib.md5(combined.encode()).hexdigest()[:16]
    logger.debug(f"Generated job ID: {job_id}")
    return job_id

def split_paragraphs(text, max_length=200):
    """Split large paragraphs into smaller ones."""
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

def get_or_create_term(term_name, taxonomy, wp_url, auth_headers):
    """Get or create a taxonomy term in WordPress."""
    term_name = sanitize_text(term_name)
    if not term_name:
        return None
    check_url = f"{wp_url}?search={term_name}"
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=5, verify=False)
        response.raise_for_status()
        terms = response.json()
        for term in terms:
            if term['name'].lower() == term_name.lower():
                logger.info(f"Found existing {taxonomy} term: {term_name}, ID: {term['id']}")
                return term['id']
        post_data = {"name": term_name, "slug": term_name.lower().replace(' ', '-')}
        response = requests.post(wp_url, json=post_data, headers=auth_headers, timeout=5, verify=False)
        response.raise_for_status()
        term = response.json()
        logger.info(f"Created new {taxonomy} term: {term_name}, ID: {term['id']}")
        return term['id']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get or create {taxonomy} term {term_name}: {str(e)}")
        return None

def check_existing_job(job_title, company_name, auth_headers):
    """Check if a job already exists on WordPress."""
    check_url = f"{WP_URL}?search={job_title}&meta_key=_company_name&meta_value={company_name}"
    try:
        response = requests.get(check_url, headers=auth_headers, timeout=5, verify=False)
        response.raise_for_status()
        posts = response.json()
        if posts:
            logger.info(f"Found existing job: {job_title} at {company_name}, Post ID: {posts[0].get('id')}")
            return posts[0].get('id'), posts[0].get('link')
        return None, None
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to check existing job {job_title} at {company_name}: {str(e)}")
        return None, None

def save_company_to_wordpress(index, company_data, wp_headers):
    """Save company data to WordPress with paraphrased details and tagline."""
    logger.debug(f"Saving company (index={index}): {json.dumps(company_data, indent=2)[:200]}...")
    if check_fetcher_status(wp_headers) != 'running':
        logger.info("Fetcher stopped before saving company")
        return None, None

    company_name = sanitize_text(company_data.get("company_name", ""))
    company_details = sanitize_text(company_data.get("company_details", ""))
    company_logo = sanitize_text(company_data.get("company_logo", ""), is_url=True)
    company_website = sanitize_text(company_data.get("company_website_url", ""), is_url=True)
    company_industry = sanitize_text(company_data.get("company_industry", ""))
    company_founded = sanitize_text(company_data.get("company_founded", ""))
    company_type = sanitize_text(company_data.get("company_type", ""))
    company_address = sanitize_text(company_data.get("company_address", ""))

    # Paraphrase company details
    if company_details:
        logger.info(f"Paraphrasing company details for {company_name}")
        company_details = paraphrase_strict_description(company_details)
        logger.debug(f"Paraphrased company details: {company_details[:100]}...")

    # Paraphrase company tagline
    company_tagline = company_details  # Using company_details as tagline source
    if company_tagline:
        logger.info(f"Paraphrasing company tagline for {company_name}")
        company_tagline = paraphrase_strict_tagline(company_tagline)
        logger.debug(f"Paraphrased company tagline: {company_tagline[:100]}...")

    company_id = hashlib.md5(company_name.encode()).hexdigest()[:16]
    logger.debug(f"Generated company ID: {company_id} for {company_name}")

    attachment_id = 0
    if company_logo:
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": wp_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, verify=False)
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for {company_name}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"Failed to upload logo for {company_name}: {str(e)}")

    post_data = {
        "company_id": company_id,
        "company_name": company_name,
        "company_details": company_details,
        "featured_media": attachment_id,
        "company_website": company_website,
        "company_industry": company_industry,
        "company_founded": company_founded,
        "company_type": company_type,
        "company_address": company_address,
        "company_tagline": company_tagline,
        "company_twitter": "",
        "company_video": ""
    }

    try:
        response = requests.post(WP_SAVE_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15, verify=False)
        response.raise_for_status()
        res = response.json()
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
        logger.error(f"Failed to save company {company_name}: {str(e)}")
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers):
    """Save job data to WordPress with paraphrased title and description."""
    logger.debug(f"Saving job (index={index}): {json.dumps(job_data, indent=2)[:200]}...")
    if check_fetcher_status(auth_headers) != 'running':
        logger.info("Fetcher stopped before saving job")
        return None, None

    job_title = sanitize_text(job_data.get("job_title", ""))
    job_description = sanitize_text(job_data.get("job_description", ""))
    job_type = sanitize_text(job_data.get("job_type", ""))
    location = sanitize_text(job_data.get("location", COUNTRY))
    job_url = sanitize_text(job_data.get("job_url", ""), is_url=True)
    company_name = sanitize_text(job_data.get("company_name", ""))
    company_logo = sanitize_text(job_data.get("company_logo", ""), is_url=True)
    environment = sanitize_text(job_data.get("environment", "").lower())
    job_salary = sanitize_text(job_data.get("job_salary", ""))
    company_industry = sanitize_text(job_data.get("company_industry", ""))
    company_founded = sanitize_text(job_data.get("company_founded", ""))

    # Paraphrase job title
    if job_title:
        logger.info(f"Paraphrasing job title: {job_title}")
        job_title = paraphrase_strict_title(job_title)
        logger.debug(f"Paraphrased job title: {job_title}")

    # Paraphrase job description
    if job_description:
        logger.info(f"Paraphrasing job description for {job_title}")
        job_description = paraphrase_strict_description(job_description)
        job_description = split_paragraphs(job_description, max_length=200)
        logger.debug(f"Paraphrased job description: {job_description[:100]}...")

    job_id = generate_job_id(job_title, company_name)

    application = ''
    if '@' in job_data.get("description_application_info", ""):
        application = job_data.get("description_application_info", "")
    elif job_data.get("resolved_application_url", ""):
        application = job_data.get("resolved_application_url", "")
    else:
        application = job_data.get("application_url", "")

    attachment_id = 0
    if company_logo:
        try:
            logo_response = requests.get(company_logo, headers=headers, timeout=10)
            logo_response.raise_for_status()
            logo_headers = {
                "Authorization": auth_headers["Authorization"],
                "Content-Disposition": f'attachment; filename="{company_name}_logo_job_{index}.jpg"',
                "Content-Type": logo_response.headers.get("content-type", "image/jpeg")
            }
            media_response = requests.post(WP_MEDIA_URL, headers=logo_headers, data=logo_response.content, verify=False)
            media_response.raise_for_status()
            attachment_id = media_response.json().get("id", 0)
            logger.info(f"Uploaded logo for job {job_title}, Attachment ID: {attachment_id}")
        except Exception as e:
            logger.error(f"Failed to upload logo for job {job_title}: {str(e)}")

    post_data = {
        "job_id": job_id,
        "job_title": job_title,
        "job_description": job_description,
        "featured_media": attachment_id,
        "job_location": location,
        "job_type": job_type,
        "job_salary": job_salary,
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": str(company_id) if company_id else "",
        "company_name": company_name,
        "company_website": sanitize_text(job_data.get("company_website_url", ""), is_url=True),
        "company_logo": str(attachment_id) if attachment_id else "",
        "company_tagline": sanitize_text(job_data.get("company_details", "")),
        "company_address": sanitize_text(job_data.get("company_address", "")),
        "company_industry": company_industry,
        "company_founded": company_founded,
        "company_twitter": "",
        "company_video": ""
    }

    try:
        response = requests.post(WP_SAVE_JOB_URL, json=post_data, headers=auth_headers, timeout=15, verify=False)
        response.raise_for_status()
        res = response.json()
        if res.get("success"):
            logger.info(f"Successfully saved job {job_title}: Job ID {job_id}")
            return job_id, f"{WP_SITE_URL}/wp-content/uploads/jobs.json"
        elif res.get("message") == "Job exists":
            logger.info(f"Found existing job {job_title}: Job ID {job_id}")
            return job_id, f"{WP_SITE_URL}/wp-content/uploads/jobs.json"
        else:
            logger.error(f"Failed to save job {job_title}: {res}")
            return None, None
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

def crawl(auth_headers, processed_ids):
    """Crawl job listings and save to WordPress."""
    if check_fetcher_status(auth_headers) != 'running':
        print("Fetcher is not running. Exiting.")
        return
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()

    for i in range(start_page, 15):
        if check_fetcher_status(auth_headers) != 'running':
            print("Fetcher stopped by user. Exiting.")
            break
        url = f'https://www.linkedin.com/jobs/search?keywords={KEYWORD}&location={COUNTRY}&start={i * 25}'
        logger.info(f'Fetching job search page: {url}')
        time.sleep(random.uniform(5, 10))
        try:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            if "login" in response.url or "challenge" in response.url:
                print("Login or CAPTCHA detected, stopping crawl")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("#main-content > section > ul > li > div > a")
            urls = [a['href'] for a in job_list if a.get('href')]
            logger.info(f'Found {len(urls)} job URLs on page: {url}')

            for index, job_url in enumerate(urls):
                if check_fetcher_status(auth_headers) != 'running':
                    print("Fetcher stopped by user. Exiting.")
                    break
                job_data = scrape_job_details(job_url, auth_headers)
                if not job_data:
                    print(f"Job (URL: {job_url}) failed to scrape: No data returned")
                    failure_count += 1
                    total_jobs += 1
                    continue

                job_dict = {
                    "job_title": job_data[0],
                    "company_logo": job_data[1],
                    "company_name": job_data[2],
                    "company_url": job_data[3],
                    "location": job_data[4],
                    "environment": job_data[5],
                    "job_type": job_data[6],
                    "level": job_data[7],
                    "job_functions": job_data[8],
                    "industries": job_data[9],
                    "job_description": job_data[10],
                    "job_url": job_data[11],
                    "company_details": job_data[12],
                    "company_website_url": job_data[13],
                    "company_industry": job_data[14],
                    "company_size": job_data[15],
                    "company_headquarters": job_data[16],
                    "company_type": job_data[17],
                    "company_founded": job_data[18],
                    "company_specialties": job_data[19],
                    "company_address": job_data[20],
                    "application_url": job_data[21],
                    "description_application_info": job_data[22],
                    "resolved_application_info": job_data[23],
                    "final_application_email": job_data[24],
                    "final_application_url": job_data[25],
                    "job_salary": ""
                }

                job_title = job_dict.get("job_title", "Unknown Job")
                company_name = job_dict.get("company_name", "")
                job_id = generate_job_id(job_title, company_name)

                if job_id in processed_ids:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed.")
                    total_jobs += 1
                    continue

                if not company_name or company_name.lower() == "unknown":
                    print(f"Job '{job_title}' (ID: {job_id}) skipped - unknown company")
                    failure_count += 1
                    total_jobs += 1
                    continue

                total_jobs += 1
                company_id, company_url = save_company_to_wordpress(index, job_dict, auth_headers)
                if company_id is None:
                    failure_count += 1
                    continue
                job_post_id, job_post_url = save_article_to_wordpress(index, job_dict, company_id, auth_headers)
                if job_post_id is None:
                    failure_count += 1
                    continue

                processed_ids.add(job_id)
                save_processed_id(job_id)
                print(f"Job '{job_title}' at {company_name} (ID: {job_id}) successfully posted to WordPress. Post ID: {job_post_id}, URL {job_post_url}")
                success_count += 1

            save_last_page(i)

        except Exception as e:
            print(f"Error fetching page {url}: {str(e)}")
            failure_count += 1

    print("\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully posted: {success_count}")
    print(f"Failed to post or scrape: {failure_count}")

def scrape_job_details(job_url, auth_headers):
    """Scrape job details from a LinkedIn job page."""
    logger.debug(f"Scraping job details from: {job_url}")
    if check_fetcher_status(auth_headers) != 'running':
        return None
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.get(job_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        company_logo = soup.select_one("#main-content > section.core-rail > div > section.top-card-layout > div > a > img")
        company_logo = (company_logo.get('data-delayed-url') or company_logo.get('src') or '') if company_logo else ''
        company_name = soup.select_one(".topcard__org-name-link")
        company_name = company_name.get_text().strip() if company_name else ''
        company_url = soup.select_one(".topcard__org-name-link")
        company_url = re.sub(r'\?.*$', '', company_url['href']) if company_url and company_url.get('href') else ''
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else COUNTRY
        location = ', '.join(dict.fromkeys([part.strip() for part in location.split(',') if part.strip()]))
        environment = ''
        env_element = soup.select(".topcard__flavor--metadata")
        for elem in env_element:
            text = elem.get_text().strip().lower()
            if 'remote' in text or 'hybrid' in text or 'on-site' in text:
                environment = elem.get_text().strip()
                break
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
                job_description = '\n\n'.join(unique_paragraphs)
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
                job_description = '\n\n'.join(unique_paragraphs)
            job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
            job_description = split_paragraphs(job_description, max_length=200)

        description_application_info = ''
        description_application_url = ''
        if description_container:
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, job_description)
            if emails:
                description_application_info = emails[0]
            else:
                links = description_container.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if 'apply' in href.lower() or 'careers' in href.lower() or 'jobs' in href.lower():
                        description_application_url = href
                        description_application_info = href
                        break
        application_anchor = soup.select_one("#teriary-cta-container > div > a")
        application_url = application_anchor['href'] if application_anchor and application_anchor.get('href') else None
        resolved_application_info = ''
        resolved_application_url = ''
        final_application_email = description_application_info if description_application_info and '@' in description_application_info else ''
        final_application_url = description_application_url if description_application_url else ''
        if application_url:
            try:
                time.sleep(5)
                resp_app = session.get(application_url, headers=headers, timeout=15, allow_redirects=True, verify=False)
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
                if final_application_email and resolved_application_info and '@' in resolved_application_info:
                    final_application_email = final_application_email if final_application_email == resolved_application_info else final_application_email
                elif resolved_application_info and '@' in resolved_application_info:
                    final_application_email = resolved_application_info
                if description_application_url and resolved_application_url:
                    final_application_url = description_application_url if description_application_url == resolved_application_url else resolved_application_url
                elif resolved_application_url:
                    final_application_url = resolved_application_url
            except Exception as e:
                error_str = str(e)
                external_url_match = re.search(r'host=\'([^\']+)\'', error_str)
                final_application_url = f"https://{external_url_match.group(1)}" if external_url_match else (description_application_url or application_url or '')

        company_details = ''
        company_website_url = ''
        company_industry = ''
        company_size = ''
        company_headquarters = ''
        company_type = ''
        company_founded = ''
        company_specialties = ''
        company_address = ''
        if company_url:
            try:
                company_response = session.get(company_url, headers=headers, timeout=15)
                company_response.raise_for_status()
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                company_details_elem = company_soup.select_one("p.about-us__description") or company_soup.select_one("section.core-section-container > div > p")
                company_details = company_details_elem.get_text().strip() if company_details_elem else ''
                company_website_anchor = company_soup.select_one("dl > div:nth-child(1) > dd > a")
                company_website_url = company_website_anchor['href'] if company_website_anchor and company_website_anchor.get('href') else ''
                if 'linkedin.com/redir/redirect' in company_website_url:
                    parsed_url = urlparse(company_website_url)
                    query_params = parse_qs(parsed_url.query)
                    company_website_url = unquote(query_params['url'][0]) if 'url' in query_params else ''
                if company_website_url and 'linkedin.com' not in company_website_url:
                    try:
                        time.sleep(5)
                        resp_company_web = session.get(company_website_url, headers=headers, timeout=15, allow_redirects=True, verify=False)
                        company_website_url = resp_company_web.url
                    except Exception as e:
                        error_str = str(e)
                        external_url_match = re.search(r'host=\'([^\']+)\'', error_str)
                        company_website_url = f"https://{external_url_match.group(1)}" if external_url_match else ''
                else:
                    description_elem = company_soup.select_one("p.about-us__description")
                    if description_elem:
                        description_text = description_elem.get_text()
                        url_pattern = r'https?://(?!www\.linkedin\.com)[^\s]+'
                        urls = re.findall(url_pattern, description_text)
                        if urls:
                            company_website_url = urls[0]
                            try:
                                time.sleep(5)
                                resp_company_web = session.get(company_website_url, headers=headers, timeout=15, allow_redirects=True, verify=False)
                                company_website_url = resp_company_web.url
                            except Exception:
                                company_website_url = ''
                company_industry_elem = company_soup.select_one("dl > div:nth-child(2) > dd")
                company_industry = company_industry_elem.get_text().strip() if company_industry_elem else ''
                company_size_elem = company_soup.select_one("dl > div:nth-child(3) > dd")
                company_size = company_size_elem.get_text().strip() if company_size_elem else ''
                company_headquarters_elem = company_soup.select_one("dl > div:nth-child(4) > dd")
                company_headquarters = company_headquarters_elem.get_text().strip() if company_headquarters_elem else ''
                company_type_elem = company_soup.select_one("dl > div:nth-child(5) > dd")
                company_type = company_type_elem.get_text().strip() if company_type_elem else ''
                company_founded_elem = company_soup.select_one("dl > div:nth-child(6) > dd")
                company_founded = company_founded_elem.get_text().strip() if company_founded_elem else ''
                company_specialties_elem = company_soup.select_one("dl > div:nth-child(7) > dd")
                company_specialties = company_specialties_elem.get_text().strip() if company_specialties_elem else ''
                company_address = company_headquarters if company_headquarters else location
            except Exception as e:
                logger.error(f'Failed to scrape company page {company_url}: {str(e)}')
                company_address = location

        return (
            job_title, company_logo, company_name, company_url, location, environment,
            job_type, level, job_functions, industries, job_description, job_url,
            company_details, company_website_url, company_industry, company_size,
            company_headquarters, company_type, company_founded, company_specialties,
            company_address, application_url, description_application_info,
            resolved_application_info, final_application_email, final_application_url
        )
    except Exception as e:
        logger.error(f'Failed to scrape job details from {job_url}: {str(e)}')
        return None

def main():
    """Main function to run the crawler."""
    if not fetch_credentials():
        print("Error: Cannot proceed without valid WordPress credentials")
        return
    auth_string = f"{WP_USERNAME}:{WP_APP_PASSWORD}"
    auth_headers = {
        "Authorization": f"Basic {base64.b64encode(auth_string.encode()).decode()}"
    }
    processed_ids = load_processed_ids()
    crawl(auth_headers, processed_ids)

if __name__ == "__main__":
    logger.debug("Script execution started")
    main()
    logger.debug("Script execution completed")
