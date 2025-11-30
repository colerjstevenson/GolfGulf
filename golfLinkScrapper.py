# pip install requests beautifulsoup4 pandas lxml
import requests
import re
import csv
import json
import time
from bs4 import BeautifulSoup
import html as _html

SITEMAP_URLS = ["https://www.golflink.com/sitemap/golfcourse001.xml"]

# Step 1 — fetch sitemap and extract course URLs
def get_course_urls(sitemap_url):
    r = requests.get(sitemap_url, timeout=15)
    r.raise_for_status()
    pattern = re.compile(r'(https://www.golflink.com/golf-courses/.*?)<')
    matches = pattern.findall(r.text)
    urls = [url.strip() for url in matches]
    
    
    print(f"Found {len(urls)} course URLs.")
    return urls

# Step 2 — fetch HTML for each course page
def fetch_html(url):
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        print(f"Fetched {url} (length: {len(r.text)})")
        return r.text
    except Exception as e:
        print("Error fetching", url, e)
        return None

# Step 3 — extract course facts from HTML using regex
def extract_facts(html):
    data = {}
    
    pattern = re.compile(
        r'<meta name=(.*?) content=(.*?) />'
    )
    matches = pattern.findall(html)
    
    bad_keys = ["viewport","theme-color","format-detection","verify-v1","y_key","msvalidate.01","robots","MediaType","Abstract","category\" scheme=\"DMINSTR2"]
    data = {m[0][1:-1].strip(): m[1][1:-1].strip() for m in matches if m[0][1:-1].strip() not in bad_keys}
    
    matches = re.search(r'opened in (\d*)', html)
    if matches:
        data["established"] = matches.group(1).strip()
        
    matches = re.search(r'<a class=.button visit-web..*? href=(.*?)>', html)
    if matches:
        data["website"] = matches.group(1).strip()[1:-1]
    
    return data


def _normalize_whitespace(s: str) -> str:
    """Collapse whitespace/newlines and unescape HTML entities."""
    if s is None:
        return s
    # replace non-breaking spaces, then collapse whitespace
    s = s.replace('\xa0', ' ')
    s = _html.unescape(s)
    # replace common HTML line breaks with spaces
    s = s.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    # collapse multiple spaces
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def clean_value(val):
    """Clean a single extracted value which may contain HTML.

    Rules:
    - If value contains an <a href="..."> and href is a tel: link, return the phone number.
    - If value contains an <a href="..."> and href is an http/https link, return the href.
    - If value contains an <address> or plain html, return its visible text with whitespace normalized.
    - Otherwise, return the normalized text.
    """
    if val is None:
        return val
    s = str(val)
    # quick check: if no angle bracket, just normalize whitespace
    if '<' not in s and '>' not in s:
        return _normalize_whitespace(s)

    soup = BeautifulSoup(s, 'html.parser')
    # prefer anchor hrefs
    a = soup.find('a', href=True)
    if a:
        href = a.get('href', '').strip()
        if href.startswith('tel:'):
            # return the numeric part
            return href.split('tel:')[-1].strip()
        # return absolute URL for website links
        if href:
            return href
        # otherwise fall back to anchor text
        return _normalize_whitespace(a.get_text(separator=' '))

    # handle address tags specially to preserve line breaks as commas
    addr = soup.find('address')
    if addr:
        text = addr.get_text(separator=', ')
        return _normalize_whitespace(text)

    # otherwise return visible text
    text = soup.get_text(separator=' ')
    return _normalize_whitespace(text)


def clean_facts(d: dict) -> dict:
    """Return a cleaned copy of the facts dict with HTML removed and whitespace normalized."""
    out = {}
    for k, v in d.items():
        # keep the key as-is, clean the value
        out[k] = clean_value(v)
    return out

# Step 4 — iterate and collect all data
def scrape_all(sitemap_url):
    results = []
    urls = get_course_urls(sitemap_url)
    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] Scraping {url}")
        if "rates-tee-times" in url:
            continue
        
        html = fetch_html(url)
        if not html:
            print("  Skipping due to fetch error.")
            continue
        facts = extract_facts(html)
        facts = clean_facts(facts)
        facts["url"] = url
        results.append(facts)
        time.sleep(0.05)  # be polite
    return results

# Step 5 — save to JSON and CSV
def save_results(data, json_path="courses.json", csv_path="courses.csv"):
    # JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # CSV — collect all possible fieldnames
    fieldnames = sorted({k for d in data for k in d.keys()})
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"Saved {len(data)} courses to {json_path} and {csv_path}")

if __name__ == "__main__":
    for SITEMAP_URL in SITEMAP_URLS:
        print(f"Processing sitemap: {SITEMAP_URL}")
        all_data = scrape_all(SITEMAP_URL)
        save_results(all_data, json_path=f'data/golfLinkData.json', csv_path=f'data/golfLinkData.csv')
