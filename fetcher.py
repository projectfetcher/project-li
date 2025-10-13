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
pip install nltk
import language_tool_python
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from sentence_transformers import SentenceTransformer, util
import warnings

# Create uploads directory if it doesn't exist
os.makedirs("uploads", exist_ok=True)

# Set CUDA_LAUNCH_BLOCKING for debugging
os.environ["CUDA_LAUNCH_BLOCKING"] = "1"

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

# Suppress insecure request warnings
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Download NLTK punkt_tab and averaged_perceptron_tagger if not already present
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
device = torch.device("cpu")  # Always CPU
model_name = "google/flan-t5-large"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
model.eval()
model.to(device)

# Initialize sentence transformer on CPU
similarity_model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

# Constants
MAX_TOTAL_TOKENS = 3000
MAX_RETURN_SEQUENCES = 4

# HTTP headers for scraping
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'
}
logger.debug(f"Initialized HTTP headers: {headers}")

# Constants for WordPress - dynamic from args
WP_USERNAME = "mary"  # default
WP_APP_PASSWORD = "Piab Mwog pfiq pdfK BOGH hDEy"  # default
PROCESSED_IDS_FILE = os.path.join("uploads", "processed_job_ids.json")
LAST_PAGE_FILE = os.path.join("uploads", "last_processed_page.txt")
logger.debug(f"File paths: PROCESSED_IDS_FILE={PROCESSED_IDS_FILE}, LAST_PAGE_FILE={LAST_PAGE_FILE}")
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
logger.debug(f"Job type mappings: {JOB_TYPE_MAPPING}")
logger.debug(f"French to English job type mappings: {FRENCH_TO_ENGLISH_JOB_TYPE}")

# Valid license key for full data scraping
VALID_LICENSE_KEY = "A1B2C-3D4E5-F6G7H-8I9J0-K1L2M-3N4O5"
UNLICENSED_MESSAGE = 'Get license: https://mimusjobs.com/job-fetcher'
logger.debug(f"Valid license key: {'*' * len(VALID_LICENSE_KEY)}")
logger.debug(f"Unlicensed message: {UNLICENSED_MESSAGE}")

def sanitize_text(text, is_url=False, is_email=False):
    """Sanitize input text by removing unwanted characters and normalizing."""
    logger.debug(f"sanitize_text called with text='{text[:50]}{'...' if len(text) > 50 else ''}', is_url={is_url}, is_email={is_email}")
    if not isinstance(text, str):
        text = str(text)
    text = text.strip()
    if not text:
        logger.debug("sanitize_text: Empty text, returning empty string")
        return ""
    if is_url or is_email:
        text = re.sub(r'[\r\t\f\v]', '', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text).strip()
        if is_url and not text.startswith(('http://', 'https://')):
            text = 'https://' + text
            logger.debug(f"sanitize_text: Added https:// prefix, text='{text}'")
        logger.debug(f"sanitize_text: Returning {'URL' if is_url else 'Email'}='{text}'")
        return text
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'#+\s*', '', text)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'—+', '', text)
    text = re.sub(r'←+', '', text)
    text = re.sub(r'[^\x20-\x7E\n\u00C0-\u017F]', '', text)
    text = re.sub(r'(\w)\.(\w)', r'\1. \2', text)
    text = re.sub(r'(\w)(\w)', r'\1 \2', text) if re.match(r'^\w+$', text) else text
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text).strip()
    logger.debug(f"sanitize_text: Normalized text, returning='{text[:50]}{'...' if len(text) > 50 else ''}'")
    return text

def normalize_for_deduplication(text):
    logger.debug(f"normalize_for_deduplication called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '', text)
    result = text.lower()
    logger.debug(f"normalize_for_deduplication: Returning='{result[:50]}{'...' if len(result) > 50 else ''}'")
    return result

def generate_id(combined):
    logger.debug(f"generate_id called with combined='{combined}'")
    id_hash = hashlib.md5(combined.encode()).hexdigest()[:16]
    logger.debug(f"generate_id: Generated id='{id_hash}'")
    return id_hash

def split_paragraphs(text, max_length=200):
    logger.debug(f"split_paragraphs called with text='{text[:50]}{'...' if len(text) > 50 else ''}', max_length={max_length}")
    paragraphs = text.split('\n\n')
    logger.debug(f"split_paragraphs: Split into {len(paragraphs)} paragraphs")
    result = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            logger.debug("split_paragraphs: Skipping empty paragraph")
            continue
        logger.debug(f"split_paragraphs: Processing paragraph='{para[:50]}{'...' if len(para) > 50 else ''}'")
        while len(para) > max_length:
            split_point = para.rfind(' ', 0, max_length)
            if split_point == -1:
                split_point = para.rfind('.', 0, max_length)
            if split_point == -1:
                split_point = max_length
            logger.debug(f"split_paragraphs: Splitting at position {split_point}, chunk='{para[:split_point][:50]}{'...' if len(para[:split_point]) > 50 else ''}'")
            result.append(para[:split_point].strip())
            para = para[split_point:].strip()
        if para:
            logger.debug(f"split_paragraphs: Adding final chunk='{para[:50]}{'...' if len(para) > 50 else ''}'")
            result.append(para)
    final_text = '\n\n'.join(result)
    logger.debug(f"split_paragraphs: Returning text with {len(result)} paragraphs, length={len(final_text)}")
    return final_text

def clean_description(text):
    """Clean paraphrased text using LanguageTool for grammar and style."""
    logger.debug(f"clean_description called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    verbose_phrases = [
        r'Paraphrased Version',
        r'Paraphrased Job Description for',
        r'This paraphrased version maintains the original content while improving clarity and readability\.'
    ]
    for phrase in verbose_phrases:
        text = re.sub(phrase, '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\*\*', '', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text).strip()
    text = re.sub(r'[\r\t\f\v]', '', text)
    try:
        matches = tool.check(text)
        corrected_text = language_tool_python.utils.correct(text, matches)
        logger.debug(f"clean_description: Corrected text='{corrected_text[:50]}{'...' if len(corrected_text) > 50 else ''}'")
        return corrected_text
    except Exception as e:
        logger.error(f"clean_description: Error in grammar correction: {str(e)}")
        return text

def is_good_paraphrase(original: str, candidate: str) -> float:
    """Calculate cosine similarity between original and paraphrased text."""
    logger.debug(f"is_good_paraphrase called with original='{original[:50]}{'...' if len(original) > 50 else ''}', candidate='{candidate[:50]}{'...' if len(candidate) > 50 else ''}'")
    try:
        embeddings = similarity_model.encode([original, candidate], convert_to_tensor=True)
        sim_score = util.pytorch_cos_sim(embeddings[0], embeddings[1]).item()
        logger.debug(f"is_good_paraphrase: Similarity score={sim_score:.2f}")
        return sim_score
    except Exception as e:
        logger.error(f"is_good_paraphrase: Error computing similarity: {str(e)}")
        return 0.0

def is_grammatically_correct(text):
    """Check if text is grammatically correct with minimal issues."""
    logger.debug(f"is_grammatically_correct called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    matches = tool.check(text)
    result = len(matches) < 3
    logger.debug(f"is_grammatically_correct: Found {len(matches)} grammar issues, returning {result}")
    return result

def extract_nouns(text):
    """Extract nouns (NN, NNS, NNP, NNPS) from text using NLTK POS tagging."""
    logger.debug(f"extract_nouns called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    try:
        tokens = nltk.word_tokenize(text)
        tagged = nltk.pos_tag(tokens)
        nouns = [word for word, pos in tagged if pos in ['NN', 'NNS', 'NNP', 'NNPS']]
        logger.debug(f"extract_nouns: Extracted nouns={nouns}")
        return nouns
    except Exception as e:
        logger.error(f"extract_nouns: Error extracting nouns: {str(e)}")
        return []

def contains_nouns(paraphrase, required_nouns):
    """Check if the paraphrase contains all required nouns."""
    logger.debug(f"contains_nouns called with paraphrase='{paraphrase[:50]}{'...' if len(paraphrase) > 50 else ''}', required_nouns={required_nouns}")
    if not required_nouns:
        return True
    paraphrase_lower = paraphrase.lower()
    result = all(noun.lower() in paraphrase_lower for noun in required_nouns)
    logger.debug(f"contains_nouns: Returning {result}")
    return result

def extract_capitalized_words(text):
    """Extract words with specific capitalization (e.g., proper nouns, acronyms)."""
    logger.debug(f"extract_capitalized_words called with text='{text[:50]}{'...' if len(text) > 50 else ''}'")
    words = re.findall(r'\b[A-Z][a-zA-Z]*\b', text)
    result = {word.lower(): word for word in words if len(word) > 1}
    logger.debug(f"extract_capitalized_words: Extracted words={list(result.values())}")
    return result

def restore_capitalization(paraphrased, capitalized_words):
    """Restore original capitalization of specified words in the paraphrased text."""
    logger.debug(f"restore_capitalization called with paraphrased='{paraphrased[:50]}{'...' if len(paraphrased) > 50 else ''}', capitalized_words={list(capitalized_words.values())}")
    result = paraphrased
    for lower_word, orig_word in capitalized_words.items():
        pattern = r'\b' + re.escape(lower_word) + r'\b'
        result = re.sub(pattern, orig_word, result, flags=re.IGNORECASE)
    logger.debug(f"restore_capitalization: Returning='{result[:50]}{'...' if len(result) > 50 else ''}'")
    return result

def paraphrase_strict_title(title, max_attempts=3, max_sub_attempts=2):
    """Paraphrase job title while preserving meaning and nouns."""
    logger.debug(f"paraphrase_strict_title called with title='{title}', max_attempts={max_attempts}, max_sub_attempts={max_sub_attempts}")
    def has_repetitions(text):
        tokens = text.lower().split()
        seen = set()
        for i in range(len(tokens) - 2):
            ngram = tuple(tokens[i:i + 3])
            if ngram in seen:
                logger.debug(f"has_repetitions: Found repetition in '{text}'")
                return True
            seen.add(ngram)
        return False

    def contains_banned_phrase(text):
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
                start_idx = text_lower.find(phrase.lower())
                context_start = max(0, start_idx - 20)
                context_end = min(len(text), start_idx + len(phrase) + 20)
                context_snippet = text[context_start:context_end]
                if context_start > 0:
                    context_snippet = "..." + context_snippet
                if context_end < len(text):
                    context_snippet = context_snippet + "..."
                logger.debug(f"contains_banned_phrase: Found '{phrase}' in context: '{context_snippet}'")
                return True, phrase, context_snippet
        return False, None, None

    def score_paraphrase(original, paraphrased, target_wc):
        sim = is_good_paraphrase(original, paraphrased)
        wc = len(paraphrased.split())
        length_penalty = abs(wc - target_wc) / max(target_wc, 1)
        score = (sim + (1 - length_penalty)) / 2
        logger.debug(f"score_paraphrase: sim={sim:.2f}, wc={wc}, target_wc={target_wc}, score={score:.2f}")
        return score, sim, wc

    clean_title = sanitize_text(title)
    if not clean_title:
        logger.error("paraphrase_strict_title: Input title is empty after sanitization")
        return title

    nouns = extract_nouns(clean_title)
    capitalized_words = extract_capitalized_words(clean_title)
    nouns_str = ", ".join(nouns) if nouns else "none"
    logger.debug(f"paraphrase_strict_title: Extracted nouns={nouns}, capitalized_words={list(capitalized_words.values())}")

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

    available_output_tokens = 60
    target_word_count = len(clean_title.split())
    min_wc = max(1, int(target_word_count * 0.6))
    max_wc = min(12, int(target_word_count * 1.4))

    best_paraphrase = None
    best_score = -1
    best_attempt = ""
    best_metadata = ""

    for attempt in range(max_attempts):
        sub_attempt = 0
        valid_paraphrase_found = False

        while not valid_paraphrase_found and sub_attempt < max_sub_attempts:
            try:
                with torch.no_grad():
                    output = model.generate(
                        input_ids=encoding['input_ids'],
                        attention_mask=encoding['attention_mask'],
                        max_new_tokens=available_output_tokens,
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

                for idx, d in enumerate(decoded_outputs):
                    paraphrased = d.replace(prompt, "").strip() if prompt in d else d.strip()
                    paraphrased = clean_description(paraphrased)
                    paraphrased = restore_capitalization(paraphrased, capitalized_words)

                    if not paraphrased or len(paraphrased.split()) < 1:
                        logger.info(f"paraphrase_strict_title: Rejected due to empty or too short: '{paraphrased}'")
                        print(f"⛔ Rejected due to empty or too short: \"{paraphrased}\"")
                        continue

                    is_banned, banned_phrase, context_snippet = contains_banned_phrase(paraphrased)
                    if is_banned:
                        logger.info(f"paraphrase_strict_title: Rejected due to banned phrase '{banned_phrase}' in context: '{context_snippet}'")
                        print(f"⛔ Rejected due to banned phrase '{banned_phrase}' in context: '{context_snippet}'")
                        continue
                    if has_repetitions(paraphrased):
                        logger.info(f"paraphrase_strict_title: Rejected due to repeated phrases: '{paraphrased}'")
                        print(f"⛔ Rejected due to repeated phrases: \"{paraphrased}\"")
                        continue
                    if not is_grammatically_correct(paraphrased):
                        logger.info(f"paraphrase_strict_title: Rejected due to grammar: '{paraphrased}'")
                        print(f"⛔ Rejected due to grammar: \"{paraphrased}\"")
                        continue
                    if not contains_nouns(paraphrased, nouns):
                        logger.info(f"paraphrase_strict_title: Rejected due to missing nouns: '{paraphrased}' (required: {nouns})")
                        print(f"⛔ Rejected due to missing nouns: \"{paraphrased}\" (required: {nouns})")
                        continue

                    score, sim, wc = score_paraphrase(title, paraphrased, target_word_count)
                    first_diff = not paraphrased.lower().startswith(title.lower())

                    print(f"📝 Attempt {attempt + 1}.{sub_attempt + 1}, Option {idx + 1}")
                    print(f"↪ Words: {wc}, Sim: {sim:.2f}, Score: {score:.2f}, First different: {first_diff}")
                    print(f"→ Paraphrased: {paraphrased}\n")

                    is_valid = (
                        min_wc <= wc <= max_wc
                        and sim >= (0.65 if target_word_count > 5 else 0.6)
                        and first_diff
                    )

                    if is_valid:
                        print(f"✅ Picked from attempt {attempt + 1}.{sub_attempt + 1}, option {idx + 1}")
                        print(f"→ {paraphrased}\n")
                        return paraphrased

                    if first_diff and score > best_score:
                        best_score = score
                        best_paraphrase = paraphrased
                        best_attempt = f"{attempt + 1}.{sub_attempt + 1}, option {idx + 1}"
                        best_metadata = (
                            f"↪ Words: {wc}, Sim: {sim:.2f}, Score: {score:.2f}, First different: {first_diff}\n"
                            f"→ Paraphrased: {paraphrased}"
                        )

                sub_attempt += 1
                time.sleep(0.5 * (2 ** sub_attempt))

            except Exception as e:
                logger.error(f"paraphrase_strict_title: Error during attempt {attempt + 1}, sub-attempt {sub_attempt + 1}: {str(e)}")
                sub_attempt += 1
                time.sleep(0.5 * (2 ** sub_attempt))

        time.sleep(1)

    if best_paraphrase:
        print(f"✅ Picked fallback from attempt {best_attempt}")
        print(best_metadata + "\n")
        return best_paraphrase

    print("❌ Fallback to original title.\n")
    return clean_title

def paraphrase_strict_company(text, max_attempts=2, max_sub_attempts=2):
    """Paraphrase company details while preserving key details."""
    logger.debug(f"paraphrase_strict_company called with text='{text[:50]}{'...' if len(text) > 50 else ''}', max_attempts={max_attempts}, max_sub_attempts={max_sub_attempts}")
    def contains_prompt(para):
        prompt_phrases = [
            "Rephrase the following company details paragraph",
            "Rephrase the company details",
            "Paragraph professionally, preserving all key details",
            "Rewrite the following",
            "Rephrase the paragraph below",
            "Rephrase the following company details",
            "Preserving all key details",
            "Tone and structure",
            "Keep the length approximately the same",
            "Do your company information paragraph need improvements",
            "Paraphrase", "Paraphrased", "Paraphrasing", "Paragraph", "Company details"
        ]
        para_lower = para.lower()
        for phrase in prompt_phrases:
            if phrase.lower() in para_lower:
                start_idx = para_lower.find(phrase.lower())
                context_start = max(0, start_idx - 20)
                context_end = min(len(para), start_idx + len(phrase) + 20)
                context_snippet = para[context_start:context_end]
                if context_start > 0:
                    context_snippet = "..." + context_snippet
                if context_end < len(para):
                    context_snippet = context_snippet + "..."
                logger.debug(f"contains_prompt: Found '{phrase}' in context: '{context_snippet}'")
                return True, phrase, context_snippet
        return False, None, None

    clean_text = sanitize_text(text)
    if not clean_text:
        logger.error("paraphrase_strict_company: Input text is empty after sanitization")
        return text

    capitalized_words = extract_capitalized_words(clean_text)
    logger.debug(f"paraphrase_strict_company: Extracted capitalized_words={list(capitalized_words.values())}")

    paragraphs = [p.strip() for p in clean_text.split('\n') if p.strip()]
    final_paraphrased = []

    for idx, para in enumerate(paragraphs):
        print(f"\n🔹 Paraphrasing Paragraph {idx + 1}/{len(paragraphs)}")

        prompt = (
            f"Rephrase the following company details paragraph professionally, preserving all key details, tone, and structure. "
            f"Keep the length approximately the same and avoid repeating the input format:\n{para}"
        )

        prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
        prompt_token_len = len(prompt_tokens)

        if prompt_token_len > MAX_TOTAL_TOKENS - 200:
            logger.warning(f"paraphrase_strict_company: Prompt for paragraph {idx + 1} too long, truncating to fit")
            para = " ".join(para.split()[:int((MAX_TOTAL_TOKENS - 200) / 4)])
            prompt = (
                f"Rephrase the following company details paragraph professionally, preserving all key details, tone, and structure. "
                f"Keep the length approximately the same:\n{para}"
            )
            prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
            prompt_token_len = len(prompt_tokens)

        available_output_tokens = max(200, MAX_TOTAL_TOKENS - prompt_token_len)
        target_word_count = len(para.split())
        tolerance = 0.25
        min_wc = int(target_word_count * (1 - tolerance))
        max_wc = int(target_word_count * (1 + tolerance))

        encoding = tokenizer.encode_plus(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=MAX_TOTAL_TOKENS
        ).to(device)

        best_paraphrase = None
        best_score = -1
        best_attempt = ""
        best_metadata = ""

        for attempt in range(max_attempts):
            sub_attempt = 0
            valid_paraphrase_found = False

            while not valid_paraphrase_found and sub_attempt < max_sub_attempts:
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

                    for option_index, d in enumerate(decoded):
                        paraphrased = d.replace(prompt, "").strip() if prompt in d else d.strip()
                        paraphrased = clean_description(paraphrased)
                        paraphrased = restore_capitalization(paraphrased, capitalized_words)

                        if not paraphrased or len(paraphrased.split()) < 5:
                            logger.info(f"paraphrase_strict_company: Rejected due to empty or too short: '{paraphrased}'")
                            print(f"⛔ Rejected due to empty or too short: \"{paraphrased}\"")
                            continue
                        is_banned, banned_phrase, context_snippet = contains_prompt(paraphrased)
                        if is_banned:
                            print(f"❌ Rejected due to prompt echo (phrase: '{banned_phrase}' in context: '{context_snippet}' in output: \"{paraphrased}\")")
                            logger.info(f"paraphrase_strict_company: Rejected due to prompt echo: '{banned_phrase}' in context: '{context_snippet}'")
                            continue

                        word_count = len(paraphrased.split())
                        similarity = is_good_paraphrase(para, paraphrased)
                        score = (similarity + (1 - abs(word_count - target_word_count) / max(target_word_count, 1))) / 2

                        first_sentence = paraphrased.split(".")[0].strip()
                        original_first = para.split(".")[0].strip()
                        first_diff = not first_sentence.lower().startswith(original_first.lower())

                        print(f"📝 Attempt {attempt + 1}.{sub_attempt + 1}, Option {option_index + 1}")
                        print(f"↪ Words: {word_count}, Sim: {similarity:.2f}, Score: {score:.2f}, First sentence different: {first_diff}")
                        print(f"→ First sentence: {first_sentence}\n")

                        is_valid = (
                            min_wc <= word_count <= max_wc
                            and similarity >= (0.65 if target_word_count > 10 else 0.6)
                            and first_diff
                            and is_grammatically_correct(paraphrased)
                        )

                        if is_valid:
                            print(f"✅ Picked from attempt {attempt + 1}.{sub_attempt + 1}, option {option_index + 1}")
                            final_paraphrased.append(clean_description(paraphrased))
                            valid_paraphrase_found = True
                            break

                        if first_diff and score > best_score:
                            best_score = score
                            best_paraphrase = paraphrased
                            best_attempt = f"{attempt + 1}.{sub_attempt + 1}, option {option_index + 1}"
                            best_metadata = (
                                f"↪ Words: {word_count}, Sim: {similarity:.2f}, Score: {score:.2f}, First sentence different: {first_diff}\n"
                                f"→ First sentence: {first_sentence}"
                            )

                    if not valid_paraphrase_found:
                        sub_attempt += 1
                        time.sleep(0.5 * (2 ** sub_attempt))

                except Exception as e:
                    logger.error(f"paraphrase_strict_company: Error during attempt {attempt + 1}, sub-attempt {sub_attempt + 1} for paragraph {idx + 1}: {str(e)}")
                    sub_attempt += 1
                    time.sleep(0.5 * (2 ** sub_attempt))

            if valid_paraphrase_found:
                break
            time.sleep(1)

        if not valid_paraphrase_found:
            if best_paraphrase:
                print(f"✅ Picked fallback from attempt {best_attempt}")
                print(best_metadata + "\n")
                final_paraphrased.append(clean_description(best_paraphrase))
            else:
                print(f"❌ Paragraph {idx + 1} fallback to original.\n")
                final_paraphrased.append(para)

    return "\n\n".join(final_paraphrased)

def paraphrase_strict_tagline(company_tagline, max_attempts=5):
    """Paraphrase company tagline into a crisp, professional summary."""
    logger.debug(f"paraphrase_strict_tagline called with tagline='{company_tagline}', max_attempts={max_attempts}")
    clean_text = sanitize_text(company_tagline)
    if not clean_text:
        logger.error(f"paraphrase_strict_tagline: Input text is empty after sanitization: {company_tagline}")
        print("Error: Input tagline is empty after sanitization.")
        return company_tagline

    capitalized_words = extract_capitalized_words(clean_text)
    logger.debug(f"paraphrase_strict_tagline: Extracted capitalized_words={list(capitalized_words.values())}")

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
                start_idx = lower.find(bad_phrase.lower())
                context_start = max(0, start_idx - 20)
                context_end = min(len(text), start_idx + len(bad_phrase) + 20)
                context_snippet = text[context_start:context_end]
                if context_start > 0:
                    context_snippet = "..." + context_snippet
                if context_end < len(text):
                    context_snippet = context_snippet + "..."
                logger.debug(f"contains_rejected_phrase: Found '{bad_phrase}' in context: '{context_snippet}'")
                return True, bad_phrase, context_snippet
        return False, None, None

    def first_sentence_diff(original, paraphrased):
        orig_first = original.split(".")[0].strip().lower()
        para_first = paraphrased.split(".")[0].strip().lower()
        return not para_first.startswith(orig_first)

    input_prompt = (
        f"Rewrite the following tagline into a crisp, professional, and meaningful summary. "
        f"Keep it short and impactful (5–12 words):\n\n"
        f"### Original ###\n{clean_text}\n\n### Paraphrased Tagline ###"
    )

    input_tokens = tokenizer.encode(input_prompt, add_special_tokens=True)
    max_length = min(len(input_tokens) + 50, 512)

    encoding = tokenizer.encode_plus(
        input_prompt,
        return_tensors="pt",
        truncation=True,
        max_length=max_length
    ).to(device)

    best_paraphrase = None
    best_score = -1
    best_meta = {"attempt": -1, "similarity": 0.0, "word_count": 0, "first_diff": False}

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
                    num_return_sequences=6,
                    eos_token_id=tokenizer.eos_token_id
                )

            decoded_outputs = [
                tokenizer.decode(seq, skip_special_tokens=True).strip()
                for seq in outputs
            ]

            paraphrases = []
            for d in decoded_outputs:
                if "### Paraphrased Tagline ###" in d:
                    parts = d.split("### Paraphrased Tagline ###")
                    paraphrased = clean_description(parts[1]) if len(parts) >= 2 else clean_description(d)
                else:
                    paraphrased = clean_description(d)
                paraphrased = restore_capitalization(paraphrased, capitalized_words)
                paraphrases.append(paraphrased)

            for paraphrased in paraphrases:
                is_banned, banned_phrase, context_snippet = contains_rejected_phrase(paraphrased)
                if is_banned:
                    logger.info(f"paraphrase_strict_tagline: Rejected due to banned phrase '{banned_phrase}' in context: '{context_snippet}'")
                    print(f"⛔ Rejected tagline due to banned phrase '{banned_phrase}' in context: '{context_snippet}'")
                    continue

                if not is_grammatically_correct(paraphrased):
                    logger.info(f"paraphrase_strict_tagline: Rejected due to grammar: '{paraphrased}'")
                    print(f"⛔ Rejected tagline due to grammar: \"{paraphrased}\"")
                    continue

                word_count = len(paraphrased.split())
                if word_count < min_word_count or word_count > max_word_count:
                    continue

                similarity = is_good_paraphrase(clean_text, paraphrased)
                length_score = 1 - abs(target_word_count - word_count) / max(target_word_count, 1)
                score = similarity * 0.7 + length_score * 0.3
                first_diff = first_sentence_diff(clean_text, paraphrased)

                print(f"Attempt {attempt + 1}: \"{paraphrased}\" | Words: {word_count} | Similarity: {similarity:.2f} | Score: {score:.2f} | First sentence different: {first_diff}")

                if first_diff and score > best_score:
                    best_paraphrase = paraphrased
                    best_meta = {
                        "attempt": attempt + 1,
                        "similarity": similarity,
                        "word_count": word_count,
                        "first_diff": first_diff
                    }

            if best_paraphrase and best_meta["first_diff"]:
                logger.info(
                    f"paraphrase_strict_tagline: Picked tagline from attempt {best_meta['attempt']} "
                    f"(words: {best_meta['word_count']}, similarity: {best_meta['similarity']:.2f}, score: {best_score:.2f})"
                )
                print(
                    f"\n✅ Picked tagline from attempt {best_meta['attempt']} "
                    f"(words: {best_meta['word_count']}, similarity: {best_meta['similarity']:.2f}, score: {best_score:.2f})"
                )
                return best_paraphrase

        except Exception as e:
            logger.error(f"paraphrase_strict_tagline: Error during attempt {attempt + 1}: {str(e)}")

        if attempt < max_attempts - 1:
            time.sleep(2 ** attempt)

    if best_paraphrase:
        logger.info(
            f"paraphrase_strict_tagline: Picked fallback tagline from attempt {best_meta['attempt']} "
            f"(words: {best_meta['word_count']}, similarity: {best_meta['similarity']:.2f}, score: {best_score:.2f})"
        )
        print(
            f"\n✅ Picked fallback tagline from attempt {best_meta['attempt']} "
            f"(words: {best_meta['word_count']}, similarity: {best_meta['similarity']:.2f}, score: {best_score:.2f})"
        )
        return best_paraphrase

    logger.warning("paraphrase_strict_tagline: No valid tagline candidates produced. Returning original")
    print("❌ Fallback to original tagline.\n")
    return clean_text

def paraphrase_strict_description(text, max_attempts=2, max_sub_attempts=2):
    """Paraphrase job description while preserving key details."""
    logger.debug(f"paraphrase_strict_description called with text='{text[:50]}{'...' if len(text) > 50 else ''}', max_attempts={max_attempts}, max_sub_attempts={max_sub_attempts}")
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
            "Job description", "Job description paragraph"
        ]
        para_lower = para.lower()
        for phrase in prompt_phrases:
            if phrase.lower() in para_lower:
                start_idx = para_lower.find(phrase.lower())
                context_start = max(0, start_idx - 20)
                context_end = min(len(para), start_idx + len(phrase) + 20)
                context_snippet = para[context_start:context_end]
                if context_start > 0:
                    context_snippet = "..." + context_snippet
                if context_end < len(para):
                    context_snippet = context_snippet + "..."
                logger.debug(f"contains_prompt: Found '{phrase}' in context: '{context_snippet}'")
                return True, phrase, context_snippet
        return False, None, None

    clean_text = sanitize_text(text)
    if not clean_text:
        logger.error("paraphrase_strict_description: Input text is empty after sanitization")
        return text

    capitalized_words = extract_capitalized_words(clean_text)
    logger.debug(f"paraphrase_strict_description: Extracted capitalized_words={list(capitalized_words.values())}")

    paragraphs = [p.strip() for p in clean_text.split('\n') if p.strip()]
    final_paraphrased = []

    for idx, para in enumerate(paragraphs):
        print(f"\n🔹 Paraphrasing Paragraph {idx + 1}/{len(paragraphs)}")

        prompt = (
            f"Rephrase the following job description paragraph professionally, preserving all key details, tone, and structure. "
            f"Keep the length approximately the same and avoid repeating the input format:\n{para}"
        )

        prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
        prompt_token_len = len(prompt_tokens)

        if prompt_token_len > MAX_TOTAL_TOKENS - 200:
            logger.warning(f"paraphrase_strict_description: Prompt for paragraph {idx + 1} too long, truncating to fit")
            para = " ".join(para.split()[:int((MAX_TOTAL_TOKENS - 200) / 4)])
            prompt = (
                f"Rephrase the following job description paragraph professionally, preserving all key details, tone, and structure. "
                f"Keep the length approximately the same:\n{para}"
            )
            prompt_tokens = tokenizer.encode(prompt, add_special_tokens=True)
            prompt_token_len = len(prompt_tokens)

        available_output_tokens = max(200, MAX_TOTAL_TOKENS - prompt_token_len)
        target_word_count = len(para.split())
        tolerance = 0.25
        min_wc = int(target_word_count * (1 - tolerance))
        max_wc = int(target_word_count * (1 + tolerance))

        encoding = tokenizer.encode_plus(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=MAX_TOTAL_TOKENS
        ).to(device)

        best_paraphrase = None
        best_score = -1
        best_attempt = ""
        best_metadata = ""

        for attempt in range(max_attempts):
            sub_attempt = 0
            valid_paraphrase_found = False

            while not valid_paraphrase_found and sub_attempt < max_sub_attempts:
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

                    for option_index, d in enumerate(decoded):
                        paraphrased = d.replace(prompt, "").strip() if prompt in d else d.strip()
                        paraphrased = clean_description(paraphrased)
                        paraphrased = restore_capitalization(paraphrased, capitalized_words)

                        if not paraphrased or len(paraphrased.split()) < 5:
                            logger.info(f"paraphrase_strict_description: Rejected due to empty or too short: '{paraphrased}'")
                            print(f"⛔ Rejected due to empty or too short: \"{paraphrased}\"")
                            continue
                        is_banned, banned_phrase, context_snippet = contains_prompt(paraphrased)
                        if is_banned:
                            print(f"❌ Rejected due to prompt echo (phrase: '{banned_phrase}' in context: '{context_snippet}' in output: \"{paraphrased}\")")
                            logger.info(f"paraphrase_strict_description: Rejected due to prompt echo: '{banned_phrase}' in context: '{context_snippet}'")
                            continue

                        word_count = len(paraphrased.split())
                        similarity = is_good_paraphrase(para, paraphrased)
                        score = (similarity + (1 - abs(word_count - target_word_count) / max(target_word_count, 1))) / 2

                        first_sentence = paraphrased.split(".")[0].strip()
                        original_first = para.split(".")[0].strip()
                        first_diff = not first_sentence.lower().startswith(original_first.lower())

                        print(f"📝 Attempt {attempt + 1}.{sub_attempt + 1}, Option {option_index + 1}")
                        print(f"↪ Words: {word_count}, Sim: {similarity:.2f}, Score: {score:.2f}, First sentence different: {first_diff}")
                        print(f"→ First sentence: {first_sentence}\n")

                        is_valid = (
                            min_wc <= word_count <= max_wc
                            and similarity >= (0.65 if target_word_count > 10 else 0.6)
                            and first_diff
                            and is_grammatically_correct(paraphrased)
                        )

                        if is_valid:
                            print(f"✅ Picked from attempt {attempt + 1}.{sub_attempt + 1}, option {option_index + 1}")
                            final_paraphrased.append(clean_description(paraphrased))
                            valid_paraphrase_found = True
                            break

                        if first_diff and score > best_score:
                            best_score = score
                            best_paraphrase = paraphrased
                            best_attempt = f"{attempt + 1}.{sub_attempt + 1}, option {option_index + 1}"
                            best_metadata = (
                                f"↪ Words: {word_count}, Sim: {similarity:.2f}, Score: {score:.2f}, First sentence different: {first_diff}\n"
                                f"→ First sentence: {first_sentence}"
                            )

                    if not valid_paraphrase_found:
                        sub_attempt += 1
                        time.sleep(0.5 * (2 ** sub_attempt))

                except Exception as e:
                    logger.error(f"paraphrase_strict_description: Error during attempt {attempt + 1}, sub-attempt {sub_attempt + 1} for paragraph {idx + 1}: {str(e)}")
                    sub_attempt += 1
                    time.sleep(0.5 * (2 ** sub_attempt))

            if valid_paraphrase_found:
                break
            time.sleep(1)

        if not valid_paraphrase_found:
            if best_paraphrase:
                print(f"✅ Picked fallback from attempt {best_attempt}")
                print(best_metadata + "\n")
                final_paraphrased.append(clean_description(best_paraphrase))
            else:
                print(f"❌ Paragraph {idx + 1} fallback to original.\n")
                final_paraphrased.append(para)

    return "\n\n".join(final_paraphrased)

def save_company_to_wordpress(index, company_data, wp_headers, licensed):
    logger.debug(f"save_company_to_wordpress called with index={index}, company_data={json.dumps(company_data, indent=2)[:200]}..., licensed={licensed}")
    company_name = company_data.get("company_name", "")
    company_details = company_data.get("company_details", UNLICENSED_MESSAGE if not licensed else "")
    company_logo = company_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    company_website = company_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else "")
    company_industry = company_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = company_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")
    company_type = company_data.get("company_type", UNLICENSED_MESSAGE if not licensed else "")
    company_address = company_data.get("company_address", UNLICENSED_MESSAGE if not licensed else "")

    # Paraphrase company details and tagline
    if company_details and company_details != UNLICENSED_MESSAGE:
        print(f"\nParaphrasing Company Details for {company_name}")
        print("-" * 30)
        print(f"Original Company Details: {company_details}")
        paraphrased_details = paraphrase_strict_company(company_details)
        paraphrased_details = re.sub(r'Job Title:\s*[^\n]*\n*', '', paraphrased_details, flags=re.IGNORECASE)
        paraphrased_details = re.sub(r'Job Description:\s*', '', paraphrased_details, flags=re.IGNORECASE)
        sentences = nltk.sent_tokenize(paraphrased_details)
        paragraphs = []
        current_paragraph = []
        sentence_count = 0
        for sentence in sentences:
            current_paragraph.append(sentence)
            sentence_count += 1
            if sentence_count >= 3:
                paragraphs.append(' '.join(current_paragraph))
                current_paragraph = []
                sentence_count = 0
        if current_paragraph:
            paragraphs.append(' '.join(current_paragraph))
        paraphrased_details = '\n\n'.join(paragraphs)
        print(f"Paraphrased Company Details: {paraphrased_details}")
        company_details = clean_description(paraphrased_details)
    else:
        logger.warning(f"save_company_to_wordpress: No company details to paraphrase for {company_name}")
        company_details = ""

    company_tagline = sanitize_text(company_data.get("company_details", ""))
    if company_tagline and company_tagline != UNLICENSED_MESSAGE:
        print(f"\nParaphrasing Company Tagline for {company_name}")
        print("-" * 30)
        print(f"Original Company Tagline: {company_tagline}")
        paraphrased_tagline = paraphrase_strict_tagline(company_tagline)
        paraphrased_tagline = re.sub(r'Job Title:\s*[^\n]*\n*', '', paraphrased_tagline, flags=re.IGNORECASE)
        paraphrased_tagline = re.sub(r'Job Description:\s*', '', paraphrased_tagline, flags=re.IGNORECASE)
        print(f"Paraphrased Company Tagline: {paraphrased_tagline}")
        company_tagline = clean_description(paraphrased_tagline)
    else:
        logger.warning(f"save_company_to_wordpress: No company tagline to paraphrase for {company_name}")
        company_tagline = ""

    company_id = generate_id(company_name)
    post_data = {
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_details": company_details,
        "company_logo": sanitize_text(company_logo, is_url=True),
        "company_website": sanitize_text(company_website, is_url=True),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_type": sanitize_text(company_type),
        "company_address": sanitize_text(company_address),
        "company_tagline": company_tagline,
        "company_twitter": "",
        "company_video": ""
    }
    logger.debug(f"save_company_to_wordpress: Prepared post_data={json.dumps(post_data, indent=2)[:200]}...")
    response = None
    try:
        response = requests.post(WP_COMPANY_URL, json=post_data, headers=wp_headers, timeout=15)
        logger.debug(f"save_company_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        if post['success']:
            logger.info(f"save_company_to_wordpress: Successfully saved company {company_name}: ID {post.get('id')}, Message {post.get('message')}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"save_company_to_wordpress: Company {company_name} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_company_to_wordpress: Failed to save company {company_name}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def save_article_to_wordpress(index, job_data, company_id, auth_headers, licensed):
    logger.debug(f"save_article_to_wordpress called with index={index}, job_data={json.dumps(job_data, indent=2)[:200]}..., company_id={company_id}, licensed={licensed}")
    job_title = job_data.get("job_title", "")
    job_description = job_data.get("job_description", UNLICENSED_MESSAGE if not licensed else "")
    job_type = job_data.get("job_type", "")
    location = job_data.get("location", "Mauritius")
    job_url = job_data.get("job_url", "")
    company_name = job_data.get("company_name", "")
    company_logo = job_data.get("company_logo", UNLICENSED_MESSAGE if not licensed else "")
    environment = job_data.get("environment", UNLICENSED_MESSAGE if not licensed else "").lower()
    job_salary = job_data.get("job_salary", "")
    company_industry = job_data.get("company_industry", UNLICENSED_MESSAGE if not licensed else "")
    company_founded = job_data.get("company_founded", UNLICENSED_MESSAGE if not licensed else "")

    # Paraphrase job title and description
    if job_title and job_description and job_description != UNLICENSED_MESSAGE:
        print(f"\n=== Processing Job #{index + 1} ===")
        print("Step 1: Original Job Text")
        print("-" * 30)
        print(f"Original Job Title: {job_title}")
        print(f"Original Job Description: {job_description}")
        print("\nStep 2: Paraphrasing Job Title and Description")
        print("-" * 30)
        try:
            paraphrased_title = paraphrase_strict_title(job_title)
            print(f"Paraphrased Job Title: {paraphrased_title}")
            words = paraphrased_title.split()
            if len(words) > 30:
                words = words[:30]
                while words and words[-1].lower() in {'and', 'or', 'at', 'in', 'of', 'for', 'with'}:
                    words.pop()
                paraphrased_title = ' '.join(words)
                logger.debug(f"save_article_to_wordpress: Truncated paraphrased title: {paraphrased_title}")
            rewritten_title = paraphrased_title
        except Exception as e:
            logger.error(f"save_article_to_wordpress: Error paraphrasing title: {str(e)}. Falling back to original title")
            print(f"Error paraphrasing title: {str(e)}. Falling back to original title")
            rewritten_title = job_title

        try:
            paraphrased_description = paraphrase_strict_description(job_description)
            print(f"Paraphrased Job Description: {paraphrased_description}")
            rewritten_description = clean_description(paraphrased_description)
        except Exception as e:
            logger.error(f"save_article_to_wordpress: Error paraphrasing description: {str(e)}. Falling back to original description")
            print(f"Error paraphrasing description: {str(e)}. Falling back to original description")
            rewritten_description = job_description
    else:
        logger.warning(f"save_article_to_wordpress: No valid job title or description to paraphrase for job {index + 1}")
        rewritten_title = job_title
        rewritten_description = job_description

    application = ''
    if '@' in job_data.get("description_application_info", ""):
        application = job_data.get("description_application_info", "")
    elif job_data.get("resolved_application_url", ""):
        application = job_data.get("resolved_application_url", "")
    else:
        application = job_data.get("application_url", "")
        if not application:
            logger.warning(f"save_article_to_wordpress: No valid application email or URL found for job {rewritten_title}")
    logger.debug(f"save_article_to_wordpress: Selected application='{application}'")

    job_id = generate_id(f"{rewritten_title}_{company_name}")
    post_data = {
        "job_id": job_id,
        "job_title": sanitize_text(rewritten_title),
        "job_description": rewritten_description,
        "job_type": sanitize_text(job_type),
        "location": sanitize_text(location),
        "job_url": sanitize_text(job_url, is_url=True),
        "environment": sanitize_text(environment),
        "job_salary": sanitize_text(job_salary),
        "application": sanitize_text(application, is_url=('@' not in application)),
        "company_id": company_id,
        "company_name": sanitize_text(company_name),
        "company_website_url": sanitize_text(job_data.get("company_website_url", UNLICENSED_MESSAGE if not licensed else ""), is_url=True),
        "company_logo": sanitize_text(company_logo, is_url=True),
        "company_details": job_data.get("company_details", UNLICENSED_MESSAGE if not licensed else ""),
        "company_address": job_data.get("company_address", UNLICENSED_MESSAGE if not licensed else ""),
        "company_industry": sanitize_text(company_industry),
        "company_founded": sanitize_text(company_founded),
        "company_twitter": "",
        "company_video": ""
    }
    logger.info(f"save_article_to_wordpress: Final job post payload for {rewritten_title}: {json.dumps(post_data, indent=2)[:200]}...")
    try:
        response = requests.post(WP_JOB_URL, json=post_data, headers=auth_headers, timeout=15)
        logger.debug(f"save_article_to_wordpress: POST response status={response.status_code}, headers={response.headers}, body={response.text[:200]}")
        response.raise_for_status()
        post = response.json()
        if post['success']:
            logger.info(f"save_article_to_wordpress: Successfully saved job {rewritten_title}: ID {post.get('id')}, Message {post.get('message')}")
            return post.get("id"), post.get("message")
        else:
            logger.info(f"save_article_to_wordpress: Job {rewritten_title} skipped: {post.get('message')}")
            return post.get("id"), post.get("message")
    except requests.exceptions.RequestException as e:
        logger.error(f"save_article_to_wordpress: Failed to save job {rewritten_title}: {str(e)}, Status: {response.status_code if response else 'None'}, Response: {response.text if response else 'None'}", exc_info=True)
        return None, None

def load_processed_ids():
    logger.debug(f"load_processed_ids called for file={PROCESSED_IDS_FILE}")
    processed_ids = set()
    try:
        if os.path.exists(PROCESSED_IDS_FILE):
            with open(PROCESSED_IDS_FILE, "r") as f:
                processed_ids = set(json.load(f))
            logger.info(f"load_processed_ids: Loaded {len(processed_ids)} processed job IDs from {PROCESSED_IDS_FILE}")
        else:
            logger.debug(f"load_processed_ids: File {PROCESSED_IDS_FILE} does not exist")
    except Exception as e:
        logger.error(f"load_processed_ids: Failed to load processed IDs from {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)
    logger.debug(f"load_processed_ids: Returning {len(processed_ids)} IDs")
    return processed_ids

def save_processed_ids(processed_ids):
    logger.debug(f"save_processed_ids called with {len(processed_ids)} IDs")
    try:
        with open(PROCESSED_IDS_FILE, "w") as f:
            json.dump(list(processed_ids), f)
        logger.info(f"save_processed_ids: Saved {len(processed_ids)} job IDs to {PROCESSED_IDS_FILE}")
    except Exception as e:
        logger.error(f"save_processed_ids: Failed to save processed IDs to {PROCESSED_IDS_FILE}: {str(e)}", exc_info=True)

def load_last_page():
    logger.debug(f"load_last_page called for file={LAST_PAGE_FILE}")
    try:
        if os.path.exists(LAST_PAGE_FILE):
            with open(LAST_PAGE_FILE, "r") as f:
                page = int(f.read().strip())
                logger.info(f"load_last_page: Loaded last processed page: {page}")
                return page
        logger.debug(f"load_last_page: File {LAST_PAGE_FILE} does not exist")
    except Exception as e:
        logger.error(f"load_last_page: Failed to load last page from {LAST_PAGE_FILE}: {str(e)}", exc_info=True)
    logger.debug("load_last_page: Returning default page 0")
    return 0

def save_last_page(page):
    logger.debug(f"save_last_page called with page={page}")
    try:
        with open(LAST_PAGE_FILE, "w") as f:
            f.write(str(page))
        logger.info(f"save_last_page: Saved last processed page: {page} to {LAST_PAGE_FILE}")
    except Exception as e:
        logger.error(f"save_last_page: Failed to save last page to {LAST_PAGE_FILE}: {str(e)}", exc_info=True)

def crawl(auth_headers, processed_ids, licensed, country, keyword):
    logger.debug(f"crawl called with processed_ids_count={len(processed_ids)}, licensed={licensed}, country={country}, keyword={keyword}")
    success_count = 0
    failure_count = 0
    total_jobs = 0
    start_page = load_last_page()
    pages_to_scrape = 10
    logger.debug(f"crawl: Starting from page {start_page}, scraping {pages_to_scrape} pages")

    for i in range(start_page, start_page + pages_to_scrape):
        url = f'https://www.linkedin.com/jobs/search?keywords={keyword}&location={country}&start={i * 25}'
        logger.info(f"crawl: Fetching job search page: {url}")
        time.sleep(random.uniform(5, 10))
        try:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
            session.mount('https://', HTTPAdapter(max_retries=retries))
            logger.debug(f"crawl: Sending GET request to {url} with headers={headers}")
            response = session.get(url, headers=headers, timeout=15)
            logger.debug(f"crawl: GET response status={response.status_code}, headers={response.headers}")
            response.raise_for_status()
            if "login" in response.url or "challenge" in response.url:
                logger.error(f"crawl: Login or CAPTCHA detected at {response.url}, stopping crawl")
                break
            soup = BeautifulSoup(response.text, 'html.parser')
            job_list = soup.select("ul.jobs-search__results-list > li a")
            urls = [a['href'] for a in job_list if a.get('href')]
            logger.info(f"crawl: Found {len(urls)} job URLs on page {i}: {urls}")
            if not urls:
                logger.warning(f"crawl: No job URLs found on page {i}. Possible selector issue or no jobs available.")

            for index, job_url in enumerate(urls):
                logger.debug(f"crawl: Processing job {index + 1}/{len(urls)}: {job_url}")
                job_data = scrape_job_details(job_url, licensed)
                if not job_data:
                    logger.error(f"crawl: No data scraped for job: {job_url}")
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
                    "resolved_application_url": job_data[26],
                    "job_salary": ""
                }
                logger.debug(f"crawl: Constructed job_dict={json.dumps(job_dict, indent=2)[:200]}...")

                job_title = job_dict.get("job_title", "Unknown Job")
                company_name = job_dict.get("company_name", "")

                job_id = generate_id(f"{job_title}_{company_name}")
                logger.debug(f"crawl: Generated job_id={job_id} for job_title='{job_title}', company_name='{company_name}'")

                if job_id in processed_ids:
                    logger.info(f"crawl: Skipping already processed job: {job_id} ({job_title} at {company_name})")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) skipped - already processed.")
                    total_jobs += 1
                    continue

                if not company_name or company_name.lower() == "unknown":
                    logger.info(f"crawl: Skipping job with unknown company: {job_title} (ID: {job_id})")
                    print(f"Job '{job_title}' (ID: {job_id}) skipped - unknown company")
                    failure_count += 1
                    total_jobs += 1
                    continue

                total_jobs += 1

                company_id, company_message = save_company_to_wordpress(index, job_dict, auth_headers, licensed)
                logger.debug(f"crawl: Company save result: company_id={company_id}, company_message={company_message}")
                if company_id is None:
                    failure_count += 1
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed - company save error.")
                    continue
                job_post_id, job_message = save_article_to_wordpress(index, job_dict, company_id, auth_headers, licensed)
                logger.debug(f"crawl: Job save result: job_post_id={job_post_id}, job_message={job_message}")

                if job_post_id is not None:
                    processed_ids.add(job_id)
                    logger.info(f"crawl: Processed and saved job: {job_id} - {job_title} at {company_name}")
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) saved successfully. Message: {job_message}")
                    success_count += 1
                else:
                    print(f"Job '{job_title}' at {company_name} (ID: {job_id}) failed to save. Check logs for details.")
                    failure_count += 1

            save_last_page(i + 1)

        except Exception as e:
            logger.error(f"crawl: Error fetching job search page: {url} - {str(e)}", exc_info=True)
            failure_count += 1
    save_processed_ids(processed_ids)
    logger.info(f"crawl: Completed. Total jobs processed: {total_jobs}, Successfully saved: {success_count}, Failed: {failure_count}")
    print("\n--- Summary ---")
    print(f"Total jobs processed: {total_jobs}")
    print(f"Successfully saved: {success_count}")
    print(f"Failed to save or scrape: {failure_count}")

def scrape_job_details(job_url, licensed):
    logger.debug(f"scrape_job_details called with job_url={job_url}, licensed={licensed}")
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))
        logger.debug(f"scrape_job_details: Sending GET request to {job_url} with headers={headers}")
        response = session.get(job_url, headers=headers, timeout=15)
        logger.debug(f"scrape_job_details: GET response status={response.status_code}, headers={response.headers}")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        job_title = soup.select_one("h1.top-card-layout__title")
        job_title = job_title.get_text().strip() if job_title else ''
        logger.info(f"scrape_job_details: Scraped Job Title: {job_title}")
        company_logo = ''
        if licensed:
            company_logo_elem = soup.select_one("div.top-card-layout__entity-info-container a img")
            company_logo = (company_logo_elem.get('data-delayed-url') or company_logo_elem.get('src') or '') if company_logo_elem else ''
            logger.info(f"scrape_job_details: Scraped Company Logo URL: {company_logo}")
        else:
            company_logo = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set company_logo={UNLICENSED_MESSAGE}")
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
        location = soup.select_one(".topcard__flavor.topcard__flavor--bullet")
        location = location.get_text().strip() if location else 'Mauritius'
        location_parts = [part.strip() for part in location.split(',') if part.strip()]
        location = ', '.join(dict.fromkeys(location_parts))
        logger.info(f"scrape_job_details: Deduplicated location for {job_title}: {location}")
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
        job_type_elem = soup.select_one(".description__job-criteria-list > li:nth-child(2) > span")
        job_type = job_type_elem.get_text().strip() if job_type_elem else ''
        job_type = FRENCH_TO_ENGLISH_JOB_TYPE.get(job_type, job_type)
        logger.info(f"scrape_job_details: Scraped Type: {job_type}")
        level = ''
        if licensed:
            level_elem = soup.select_one(".description__job-criteria-list > li:nth-child(1) > span")
            level = level_elem.get_text().strip() if level_elem else ''
            logger.info(f"scrape_job_details: Scraped Level: {level}")
        else:
            level = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set level={UNLICENSED_MESSAGE}")
        job_functions = ''
        if licensed:
            job_functions_elem = soup.select_one(".description__job-criteria-list > li:nth-child(3) > span")
            job_functions = job_functions_elem.get_text().strip() if job_functions_elem else ''
            logger.info(f"scrape_job_details: Scraped Job Functions: {job_functions}")
        else:
            job_functions = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_functions={UNLICENSED_MESSAGE}")
        industries = ''
        if licensed:
            industries_elem = soup.select_one(".description__job-criteria-list > li:nth-child(4) > span")
            industries = industries_elem.get_text().strip() if industries_elem else ''
            logger.info(f"scrape_job_details: Scraped Industries: {industries}")
        else:
            industries = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set industries={UNLICENSED_MESSAGE}")
        job_description = ''
        if licensed:
            description_container = soup.select_one(".show-more-less-html__markup")
            if description_container:
                paragraphs = description_container.find_all(['p', 'li'], recursive=False)
                if paragraphs:
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"scrape_job_details: Raw paragraphs for {job_title}: {[sanitize_text(p.get_text().strip())[:50] for p in paragraphs if p.get_text().strip()]}")
                    for p in paragraphs:
                        para = sanitize_text(p.get_text().strip())
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
                else:
                    raw_text = description_container.get_text(separator='\n').strip()
                    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]
                    seen = set()
                    unique_paragraphs = []
                    logger.debug(f"scrape_job_details: Raw text paragraphs for {job_title}: {[sanitize_text(para)[:50] for para in paragraphs]}")
                    for para in paragraphs:
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
                job_description = re.sub(r'(?i)(?:\s*Show\s+more\s*$|\s*Show\s+less\s*$)', '', job_description, flags=re.MULTILINE).strip()
                job_description = split_paragraphs(job_description, max_length=200)
                delimiter = "\n\n"
                logger.info(f'Scraped Job Description (length): {len(job_description)}, Paragraphs: {job_description.count(delimiter) + 1}')
            else:
                logger.warning(f"scrape_job_details: No job description container found for {job_title}")
        else:
            job_description = UNLICENSED_MESSAGE
            logger.debug(f"scrape_job_details: Unlicensed, set job_description={UNLICENSED_MESSAGE}")
        description_application_info = ''
        description_application_url = ''
        if licensed and job_description and job_description != UNLICENSED_MESSAGE:
            # Extract application information from job description
            email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
            description_emails = re.findall(email_pattern, job_description)
            description_urls = re.findall(url_pattern, job_description)
            logger.debug(f"scrape_job_details: Found emails={description_emails}, URLs={description_urls} in description")

            if description_emails:
                description_application_info = description_emails[0]
                logger.info(f"scrape_job_details: Extracted application email: {description_application_info}")
            elif description_urls:
                description_application_info = description_urls[0]
                logger.info(f"scrape_job_details: Extracted application URL: {description_application_info}")
            else:
                logger.debug(f"scrape_job_details: No application info found in description for {job_title}")

        # Extract company details
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
            company_page_link = soup.select_one(".topcard__org-name-link")
            if company_page_link and company_page_link.get('href'):
                company_page_url = company_page_link['href']
                company_page_url = re.sub(r'\?.*$', '', company_page_url)
                logger.debug(f"scrape_job_details: Found company page URL: {company_page_url}")
                time.sleep(random.uniform(2, 5))
                try:
                    company_response = session.get(company_page_url, headers=headers, timeout=15)
                    company_response.raise_for_status()
                    company_soup = BeautifulSoup(company_response.text, 'html.parser')
                    logger.debug(f"scrape_job_details: Successfully fetched company page: {company_page_url}")

                    # Company details (About section)
                    about_section = company_soup.select_one("section.org-about-module")
                    if about_section:
                        paragraphs = about_section.find_all('p', recursive=True)
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
                        company_details = '\n\n'.join(unique_paragraphs)
                        logger.info(f"scrape_job_details: Scraped company details (length): {len(company_details)}")

                    # Company website
                    website_link = company_soup.select_one("a[data-tracking-control-name='about_website']")
                    if website_link and website_link.get('href'):
                        company_website_url = website_link['href']
                        logger.info(f"scrape_job_details: Scraped company website: {company_website_url}")

                    # Other company metadata
                    details_list = company_soup.select("dl.about-page__company-details dt, dd")
                    current_key = None
                    for elem in details_list:
                        if elem.name == 'dt':
                            current_key = elem.get_text().strip().lower()
                        elif elem.name == 'dd' and current_key:
                            text = sanitize_text(elem.get_text().strip())
                            if current_key == 'industry':
                                company_industry = text
                            elif current_key == 'company size':
                                company_size = text
                            elif current_key == 'headquarters':
                                company_headquarters = text
                            elif current_key == 'type':
                                company_type = text
                            elif current_key == 'founded':
                                company_founded = text
                            elif current_key == 'specialties':
                                company_specialties = text
                            logger.debug(f"scrape_job_details: Scraped {current_key}: {text}")

                    # Company address (if available)
                    address_elem = company_soup.select_one("p.org-location")
                    if address_elem:
                        company_address = sanitize_text(address_elem.get_text().strip())
                        logger.info(f"scrape_job_details: Scraped company address: {company_address}")

                except requests.exceptions.RequestException as e:
                    logger.error(f"scrape_job_details: Failed to fetch company page {company_page_url}: {str(e)}")
            else:
                logger.warning(f"scrape_job_details: No company page link found for {company_name}")
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

        # Application URL handling
        application_url = ''
        resolved_application_info = ''
        final_application_email = ''
        final_application_url = ''
        resolved_application_url = ''

        apply_button = soup.select_one("a.apply-button")
        if apply_button and apply_button.get('href'):
            application_url = apply_button['href']
            logger.debug(f"scrape_job_details: Found apply button URL: {application_url}")

            if 'linkedin.com' not in application_url:
                try:
                    resolved_response = session.get(application_url, headers=headers, allow_redirects=True, timeout=10)
                    resolved_application_url = resolved_response.url
                    logger.info(f"scrape_job_details: Resolved application URL: {resolved_application_url}")
                except requests.exceptions.RequestException as e:
                    logger.error(f"scrape_job_details: Failed to resolve application URL {application_url}: {str(e)}")
                    resolved_application_url = application_url
            else:
                resolved_application_url = application_url
                logger.debug(f"scrape_job_details: Application URL is LinkedIn internal: {application_url}")

        # Determine final application info
        if description_application_info and '@' in description_application_info:
            final_application_email = description_application_info
            logger.info(f"scrape_job_details: Using email from description as final application: {final_application_email}")
        elif description_application_info and 'http' in description_application_info:
            final_application_url = description_application_info
            logger.info(f"scrape_job_details: Using URL from description as final application: {final_application_url}")
        elif resolved_application_url:
            final_application_url = resolved_application_url
            logger.info(f"scrape_job_details: Using resolved application URL as final: {final_application_url}")
        elif application_url:
            final_application_url = application_url
            logger.info(f"scrape_job_details: Using apply button URL as final: {final_application_url}")
        else:
            logger.warning(f"scrape_job_details: No valid application info found for {job_title}")

        # Job salary (not typically available on LinkedIn, but attempt to extract)
        job_salary = ''
        salary_elem = soup.select_one(".salary-info")
        if salary_elem:
            job_salary = sanitize_text(salary_elem.get_text().strip())
            logger.info(f"scrape_job_details: Scraped job salary: {job_salary}")
        else:
            logger.debug(f"scrape_job_details: No salary information found for {job_title}")

        job_data = [
            job_title, company_logo, company_name, company_url, location, environment,
            job_type, level, job_functions, industries, job_description, job_url,
            company_details, company_website_url, company_industry, company_size,
            company_headquarters, company_type, company_founded, company_specialties,
            company_address, application_url, description_application_info,
            resolved_application_info, final_application_email, final_application_url,
            resolved_application_url, job_salary
        ]
        logger.info(f"scrape_job_details: Successfully scraped job data for {job_title}")
        return job_data

    except requests.exceptions.RequestException as e:
        logger.error(f"scrape_job_details: Failed to scrape job {job_url}: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"scrape_job_details: Unexpected error for {job_url}: {str(e)}", exc_info=True)
        return None

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Job Scraper for LinkedIn with WordPress Integration")
    parser.add_argument('--license-key', type=str, default='', help='License key for full data scraping')
    parser.add_argument('--wp-username', type=str, default=WP_USERNAME, help='WordPress username')
    parser.add_argument('--wp-password', type=str, default=WP_APP_PASSWORD, help='WordPress application password')
    parser.add_argument('--country', type=str, default='Mauritius', help='Country for job search')
    parser.add_argument('--keyword', type=str, default='Software Engineer', help='Keyword for job search')
    args = parser.parse_args()

    logger.info(f"main: Starting scraper with args: license_key={'*' * len(args.license_key) if args.license_key else 'None'}, wp_username={args.wp_username}, country={args.country}, keyword={args.keyword}")

    # Validate license key
    licensed = args.license_key == VALID_LICENSE_KEY
    if not licensed:
        logger.warning(f"main: Invalid or no license key provided. Limited data scraping enabled. {UNLICENSED_MESSAGE}")
        print(f"Warning: Invalid or no license key provided. Limited data scraping enabled. {UNLICENSED_MESSAGE}")

    # WordPress API endpoints
    global WP_JOB_URL, WP_COMPANY_URL
    WP_JOB_URL = "https://mimusjobs.com/wp-json/mimus-jobs/v1/jobs"
    WP_COMPANY_URL = "https://mimusjobs.com/wp-json/mimus-jobs/v1/companies"
    logger.debug(f"main: WordPress URLs set: WP_JOB_URL={WP_JOB_URL}, WP_COMPANY_URL={WP_COMPANY_URL}")

    # WordPress authentication headers
    auth_string = f"{args.wp_username}:{args.wp_password}"
    encoded_auth = base64.b64encode(auth_string.encode()).decode()
    auth_headers = {
        'Authorization': f'Basic {encoded_auth}',
        'Content-Type': 'application/json',
        'User-Agent': headers['user-agent']
    }
    logger.debug(f"main: Prepared auth headers with encoded auth={'*' * len(encoded_auth)}")

    # Load processed IDs
    processed_ids = load_processed_ids()

    try:
        # Start crawling
        crawl(auth_headers, processed_ids, licensed, args.country, args.keyword)
    except KeyboardInterrupt:
        logger.info("main: Script interrupted by user. Saving processed IDs and last page.")
        print("\nScript interrupted. Saving progress...")
        save_processed_ids(processed_ids)
        print("Progress saved. Exiting.")
    except Exception as e:
        logger.error(f"main: Unexpected error: {str(e)}", exc_info=True)
        print(f"Unexpected error: {str(e)}. Check logs for details.")
        save_processed_ids(processed_ids)

if __name__ == "__main__":
    main()
