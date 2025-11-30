"""
golfdigest_sitemap_scrape.py

pip install requests beautifulsoup4 lxml pandas
"""

import requests
import gzip
import io
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
import json
import csv
import pandas as pd

# CONFIG
SITEMAP_INDEX = "https://www.golfdigest.com/sitemaps/sitemap_golfdigest_index.xml"
USER_AGENT = "Mozilla/5.0 (compatible; sitemap-scraper/1.0; +https://example.com)"
RATE_LIMIT = 0.4  # seconds between requests

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def fetch_url_text(url):
    r = session.get(url, timeout=20)
    r.raise_for_status()
    return r.content  # return bytes to handle gzip

def parse_xml_from_bytes(bts):
    # try to parse bytes directly; if gzipped, decompress first
    if bts[:2] == b'\x1f\x8b':  # gzip magic
        with gzip.GzipFile(fileobj=io.BytesIO(bts)) as gz:
            bts = gz.read()
    # parse
    return ET.fromstring(bts)

def get_sitemap_list(index_url):
    print("Fetching sitemap index:", index_url)
    b = fetch_url_text(index_url)
    root = parse_xml_from_bytes(b)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    sitemaps = []
    # find <sitemap><loc>...</loc></sitemap> OR plain <urlset><url><loc>...
    for sitemap in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
        loc = sitemap.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
        if loc is not None and loc.text:
            sitemaps.append(loc.text.strip())
    # fallback: if index is actually a urlset, return the index itself
    if not sitemaps:
        sitemaps = [index_url]
    print(f"Found {len(sitemaps)} sitemap files in index.")
    return sitemaps

def extract_urls_from_sitemap(sitemap_url):
    print("  -> Downloading sitemap:", sitemap_url)
    b = fetch_url_text(sitemap_url)
    root = parse_xml_from_bytes(b)
    urls = []
    for loc in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
        if loc.text:
            urls.append(loc.text.strip())
    # fallback generic find if namespaced tags differ
    if not urls:
        for loc in root.findall(".//loc"):
            if loc.text:
                urls.append(loc.text.strip())
    print(f"     {len(urls)} URLs found in sitemap.")
    return urls

def collect_all_course_urls(index_url):
    sitemaps = get_sitemap_list(index_url)
    all_urls = []
    for sm in sitemaps:
        try:
            urls = extract_urls_from_sitemap(sm)
            all_urls.extend(urls)
        except Exception as e:
            print("    Failed to read sitemap", sm, e)
        time.sleep(RATE_LIMIT)
    # filter /courses/ pages (case-insensitive)
    course_urls = [u for u in all_urls if "/courses/" in u.lower()]
    # dedupe while preserving order
    seen = set()
    course_urls_unique = []
    for u in course_urls:
        if u not in seen:
            seen.add(u)
            course_urls_unique.append(u)
    print(f"Total course URLs discovered: {len(course_urls_unique)}")
    return course_urls_unique

# --- Simple example HTML extractor: adapt to exact site structure ---
def extract_course_facts(html_text):
    soup = BeautifulSoup(html_text, "lxml")
    out = {}
    # Example: title from <h1>
    h1 = soup.find("h1")
    if h1:
        out["name"] = h1.get_text(strip=True)
    # Example: look for dt/dd pairs like your earlier pattern
    pairs = soup.find_all(lambda tag: tag.name == "dt" and "course__general-facts-item-label" in (tag.get("class") or []))
    for dt in pairs:
        dd = dt.find_next_sibling("dd")
        if dd:
            key = dt.get_text(separator=" ", strip=True)
            val = dd.get_text(separator=" ", strip=True)
            out[key] = val
    # Fallback regex if classes differ (simple dt/dd pattern)
    if not out:
        m = re.findall(r'<dt[^>]*>([^<]+)</dt>\s*<dd[^>]*>([^<]+)</dd>', str(soup), flags=re.DOTALL|re.IGNORECASE)
        for k,v in m:
            out[k.strip()] = v.strip()
    return out

def scrape_course_pages(urls, max_pages=None):
    data = []
    total = len(urls) if max_pages is None else min(len(urls), max_pages)
    for i, url in enumerate(urls[:total], start=1):
        try:
            print(f"[{i}/{total}] GET {url}")
            r = session.get(url, timeout=20)
            r.raise_for_status()
            facts = extract_course_facts(r.text)
            facts["url"] = url
            data.append(facts)
        except Exception as e:
            print("  Error fetching", url, e)
        time.sleep(RATE_LIMIT)
    return data

def save_outputs(course_urls, scraped_data):
    with open("courses_urls.json", "w", encoding="utf-8") as f:
        json.dump(course_urls, f, ensure_ascii=False, indent=2)
    with open("courses_data.json", "w", encoding="utf-8") as f:
        json.dump(scraped_data, f, ensure_ascii=False, indent=2)
    # CSV using pandas for convenience (will normalize keys)
    if scraped_data:
        df = pd.json_normalize(scraped_data)
        df.to_csv("courses_data.csv", index=False)
    print("Saved courses_urls.json, courses_data.json, courses_data.csv")

def main():
    course_urls = collect_all_course_urls(SITEMAP_INDEX)
    # Save just the URLs first
    with open("courses_urls.json", "w", encoding="utf-8") as f:
        json.dump(course_urls, f, ensure_ascii=False, indent=2)
    # Optionally inspect a sample (uncomment to test single page)
    # sample = course_urls[:5]
    # scraped = scrape_course_pages(sample)
    # save_outputs(course_urls, scraped)

    # If you want to scrape everything, be polite:
    scraped = scrape_course_pages(course_urls, max_pages=None)  # set max_pages to limit for testing
    save_outputs(course_urls, scraped)

if __name__ == "__main__":
    main()
