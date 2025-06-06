import os
import re
import csv
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pymongo import MongoClient
# ------------------------------------------------------------------
# REPLACE the entire old download_pdf_direct() with this new version
# ------------------------------------------------------------------
from bs4 import BeautifulSoup
import shutil, io, tarfile
# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# For parallelization (optional)
import concurrent.futures

# -------------------- Utility Functions --------------------
def sanitize_filename(s):
    """
    Convert a string into a safe filename by removing or replacing characters
    that are not letters, digits, spaces, underscores, or hyphens.
    """
    s = re.sub(r'[^a-zA-Z0-9_\- ]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def replace_with_abbreviations(text, abbr_dict):
    """
    Replace full terms in 'text' with their abbreviations from 'abbr_dict'
    using regex patterns that ensure exact matching (negative lookbehind/lookahead).
    """
    if not text or not abbr_dict:
        return text

    result = text
    sorted_terms = sorted(abbr_dict.keys(), key=len, reverse=True)
    for term in sorted_terms:
        pattern = re.compile(r'(?<!\w)' + re.escape(term) + r'(?!\w)', re.IGNORECASE)
        result = pattern.sub(abbr_dict[term], result)
    return result

def generate_citation(details):
    """
    Generate a citation string for a paid article:
    Article Title
    Journal Name. PublicationDate;Volume(Issue):Pages
    doi: DOI
    """
    title = details.get("title", "No Title")
    journal = details.get("journal", "No Journal")
    pub_date = details.get("publication_date", "No Publication Date")
    volume = details.get("volume", "")
    issue = details.get("issue", "")
    pages = details.get("pages", "")
    doi = details.get("doi", "No DOI")

    vol_issue_pages = ""
    if volume or issue or pages:
        vol_issue_pages = f"{volume}"
        if issue:
            vol_issue_pages += f"({issue})"
        if pages:
            vol_issue_pages += f":{pages}"

    citation = f"{title}\n{journal}. {pub_date};{vol_issue_pages}\ndoi: {doi}"
    return citation

def setup_selenium():
    """
    Configure a headless Chrome browser with appropriate options.
    Returns a Selenium WebDriver instance.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# ------------------------------------------------------------------
# REPLACE the entire old download_pdf_direct() with this new version
# ------------------------------------------------------------------
from bs4 import BeautifulSoup
import os, time, requests, shutil, re, io, tarfile

def download_pdf_direct(url, filename, retries=3):
    """
    Robustly download a PDF from *url* into pdfs/<filename>
    (1) Follows normal HTTP redirects.
    (2) If the response is HTML, looks for:
          • <meta http-equiv="refresh" … URL=…pdf?download=1>
          • first <a href="…pdf">
    (3) Saves only when Content-Type is application/pdf.
    """
    os.makedirs("pdfs", exist_ok=True)
    tmp_path   = os.path.join("pdfs", filename + ".tmp")
    final_path = os.path.join("pdfs", filename)

    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/122.0.0.0 Safari/537.36")

    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            r = requests.get(url,
                             headers={"User-Agent": ua, "Referer": url},
                             stream=True, timeout=20, allow_redirects=True)
            ctype = r.headers.get("content-type", "").lower()

            # ✅ direct PDF
            if ctype.startswith("application/pdf"):
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                if os.path.getsize(tmp_path):
                    os.replace(tmp_path, final_path)
                    print(f"Downloaded → {final_path}")
                    return final_path
                else:
                    print(f"Empty file from {url}, retrying …")

            # 🛈 HTML wrapper – find the real PDF link
            elif ctype.startswith("text/html"):
                soup = BeautifulSoup(r.text, "html.parser")

                # meta refresh  (<meta http-equiv="refresh" content="0; url=...pdf?download=1">)
                meta = soup.find("meta", attrs={"http-equiv": lambda v: v and v.lower()=="refresh"})
                if meta and "url=" in meta.get("content","").lower():
                    url = requests.compat.urljoin(url,
                           meta["content"].split("=",1)[1].strip())
                    print("Following meta-refresh →", url)
                    time.sleep(1)
                    continue

                # first explicit link ending in .pdf
                link = soup.find("a", href=lambda h: h and h.lower().endswith(".pdf"))
                if link:
                    url = requests.compat.urljoin(url, link["href"])
                    print("Following link →", url)
                    time.sleep(1)
                    continue

                print(f"No PDF link found in HTML wrapper from {url}")

            else:
                print(f"Unexpected content-type '{ctype}' from {url}, retrying …")

        except Exception as e:
            print(f"Error downloading PDF (attempt {attempt}) from {url}: {e}")

        # clean up any partial file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        time.sleep(2)

    print(f"Failed to download PDF from {url} after {retries} attempts.")
    return None


def download_pmc_pdf(pmcid, new_filename=None):
    base_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
    driver = setup_selenium()
    pdf_path = None

    try:
        driver.get(base_url)
        wait = WebDriverWait(driver, 10)

        # ▶ locate by href, not link-text
        pdf_link_elem = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(@href, '.pdf')]")
            )
        )
        pdf_url = pdf_link_elem.get_attribute("href")
        if pdf_url.startswith("/"):
            pdf_url = "https://www.ncbi.nlm.nih.gov" + pdf_url

        if new_filename is None:
            new_filename = sanitize_filename(pmcid) + ".pdf"

        pdf_path = download_pdf_direct(pdf_url, new_filename)

    except Exception as e:
        print(f"No PDF element found or error occurred for PMC {pmcid}: {e}")

    finally:
        driver.quit()

    return pdf_path


def attempt_pdf_download(details, new_filename=None):
    """
    Attempts to download the PDF for a given article's details.
    If new_filename is provided, it is used as the filename.
    Returns the file path if downloaded, or "Not downloaded"/"Not available".
    """
    link = details.get("fulltext_link", "No Full Text Link")
    pmcid = details.get("pmcid", "No PMC ID")

    # If the link ends in ".pdf", try direct download
    if link != "No Full Text Link" and "pdf" in link.lower():
        if new_filename is None:
            new_filename = f"{sanitize_filename(details['pubmed_id'])}.pdf"
        pdf_file = download_pdf_direct(link, new_filename)
        return pdf_file if pdf_file else "Not downloaded"

    # Otherwise, if we have a PMCID, attempt a PMC download
    elif pmcid != "No PMC ID":
        pdf_file = download_pmc_pdf(pmcid, new_filename=new_filename)
        return pdf_file if pdf_file else "Not downloaded"

    else:
        return "Not available"
    
# -------------------- MongoDB Setup and CSV Import --------------------

def connect_to_mongo():
    """
    Connect to the local MongoDB.
    Returns a reference to the 'research_papers' database.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["research_papers"]
    return db

def import_keywords(db):
    """
    Merge keywords from Keywords.csv into the 'keywords' collection (upsert).
    """
    csv_path = r"C:\Users\saisr\OneDrive\Desktop\Capstone\Keywords.csv"  # or wherever your file is
    try:
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Pull from possible column names
                keyword = row.get("Keyword") or row.get("\ufeffKeyword") or row.get("keyword")
                if keyword:
                    db["keywords"].update_one(
                        {"Keyword": keyword},
                        {"$set": {"Keyword": keyword}},
                        upsert=True
                    )
        print("Keywords merged/updated from CSV.")
    except Exception as e:
        print(f"Error importing Keywords.csv: {e}")

def import_abbreviations(db):
    """
    Merge abbreviations from Abbreviations.csv into the 'abbreviations' collection (upsert).
    """
    csv_path = r"C:\Users\saisr\OneDrive\Desktop\Capstone\Abbreviations.csv"
    try:
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                full_term = row.get("Term") or row.get("\ufeffTerm") or row.get("term")
                abbr = row.get("Abbreviation") or row.get("\ufeffAbbreviation") or row.get("abbreviation")
                if full_term and abbr:
                    db["abbreviations"].update_one(
                        {"Term": full_term},
                        {"$set": {
                            "Term": full_term,
                            "Abbreviation": abbr
                        }},
                        upsert=True
                    )
        print("Abbreviations merged/updated from CSV.")
    except Exception as e:
        print(f"Error importing Abbreviations.csv: {e}")

def init_db():
    """
    Initialize the database by ensuring required collections exist,
    and merge in updated keywords/abbreviations from CSV each time.
    """
    db = connect_to_mongo()
    required_collections = ["articles", "keywords", "abbreviations", "run_logs"]
    existing_collections = db.list_collection_names()
    for coll in required_collections:
        if coll not in existing_collections:
            db.create_collection(coll)
            print(f"Created collection: {coll}")

    # Merge CSV-based data every time (so new entries are recognized)
    import_keywords(db)
    import_abbreviations(db)

    return db

def get_keywords(db):
    """
    Retrieve the list of keywords from the 'keywords' collection.
    """
    keywords = []
    for doc in db["keywords"].find():
        k = doc.get("Keyword") or doc.get("\ufeffKeyword") or doc.get("keyword")
        if k:
            keywords.append(k)
    print("Retrieved keywords:", keywords)
    return keywords

def get_last_successful_run_date(db):
    """
    Retrieve the end_time of the last run log that completed successfully and processed at least one article.
    Returns a datetime object or None if no such run exists.
    """
    last_log = db["run_logs"].find_one(
        {"status": "completed", "articles_processed": {"$gt": 0}},
        sort=[("end_time", -1)]
    )
    return last_log.get("end_time") if last_log and "end_time" in last_log else None

# -------------------- Abbreviation Mapping and Title Computation --------------------

def get_abbreviation_map(db):
    """
    Return a dict for abbreviation replacements (lower-cased keys).
    """
    abbr_map = {}
    for doc in db["abbreviations"].find():
        full_term = doc.get("Term") or doc.get("\ufeffTerm")
        abbr = doc.get("Abbreviation") or doc.get("\ufeffAbbreviation")
        if full_term and abbr:
            abbr_map[full_term.strip().lower()] = abbr.strip()
    return abbr_map

def compute_updated_title(details, abbr_map):
    """
    Construct an updated title by normalizing punctuation, replacing abbreviations,
    and appending the first author's LASTNAME in uppercase plus publication year.
    """
    original_title = details.get("title", "")
    if not original_title or original_title == "No Title Found":
        # Log that minimal metadata was found
        print(f"Warning: PubMed ID {details.get('pubmed_id')} has minimal metadata (no title).")

    # Normalize punctuation
    normalized_title = re.sub(r'[-–—]', ' ', original_title)  # hyphens/dashes => space
    normalized_title = re.sub(r'[.,;:()]+', ' ', normalized_title)
    normalized_title = re.sub(r'\s+', ' ', normalized_title).strip()

    # Replace full terms with abbreviations
    updated_title = replace_with_abbreviations(normalized_title, abbr_map)

    # Append first author's last name + pub year
    authors = details.get("authors", [])
    first_author_last = ""
    if authors:
        parts = authors[0].split()
        if parts:
            first_author_last = parts[-1].upper()

    pub_date = details.get("publication_date", "")
    pub_year = ""
    if pub_date and pub_date != "No Publication Date":
        pub_year = pub_date.split("-")[0]

    final_title = f"{updated_title} {first_author_last} {pub_year}".strip()
    return final_title

# -------------------- PubMed Helpers --------------------

def parse_pub_date(pub_date_tag):
    """
    Robustly parse different PubDate formats (e.g., '2023 Aug 10', partial dates).
    """
    if not pub_date_tag:
        return "No Publication Date"

    year = pub_date_tag.find("Year")
    month = pub_date_tag.find("Month")
    day = pub_date_tag.find("Day")

    date_str_parts = []
    if year: date_str_parts.append(year.text)
    if month: date_str_parts.append(month.text)
    if day: date_str_parts.append(day.text)

    date_str = " ".join(date_str_parts).strip()
    if not date_str:
        return "No Publication Date"

    # Attempt parsing:
    try:
        from dateutil import parser as date_parser
        dt = date_parser.parse(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        # If parsing fails, just return the raw string
        return date_str

def search_pubmed_date_range(query, start_date, end_date, max_results=1000):
    """
    Use PubMed eSearch API to get PubMed IDs between start_date and end_date for the given query.
    Includes pagination to handle >100 results. 
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    mindate = start_date.strftime("%Y/%m/%d")
    maxdate = end_date.strftime("%Y/%m/%d")

    all_ids = []
    retstart = 0
    batch_size = 100  # you can adjust this
    while True:
        params = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": batch_size,
            "retstart": retstart,
            "datetype": "pdat",
            "mindate": mindate,
            "maxdate": maxdate
        }
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error searching PubMed for query '{query}': {e}")
            break

        id_list = data["esearchresult"].get("idlist", [])
        all_ids.extend(id_list)

        if len(id_list) < batch_size or len(all_ids) >= max_results:
            break
        retstart += batch_size

    return all_ids[:max_results]

def fetch_pubmed_details(pubmed_id):
    """
    Use PubMed eFetch API to retrieve metadata for a single PubMed ID.
    Returns dict of article details (title, abstract, authors, etc.).
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": pubmed_id,
        "retmode": "xml"
    }
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching PubMed data for ID {pubmed_id}: {e}")
        return {"pubmed_id": pubmed_id, "error": f"Request error: {e}"}

    # Parse XML
    try:
        soup = BeautifulSoup(response.text, "xml")
    except Exception as e:
        print(f"XML parsing error for PubMed ID {pubmed_id}: {e}")
        return {"pubmed_id": pubmed_id, "error": f"XML parsing error: {e}"}

    # Title
    title_tag = soup.find("ArticleTitle")
    title = title_tag.text if title_tag else "No Title Found"

    # Collect all AbstractText sections
    abstract_tags = soup.find_all("AbstractText")
    if abstract_tags:
        abstract_full = []
        for atag in abstract_tags:
            label = atag.get("Label")
            text = atag.text
            if label:
                abstract_full.append(f"{label}: {text}")
            else:
                abstract_full.append(text)
        abstract = "\n".join(abstract_full)
    else:
        abstract = "No Abstract Found"

    # PubDate
    date_tag = soup.find("PubDate")
    publication_date = parse_pub_date(date_tag)

    # Keywords
    keyword_tags = soup.find_all("Keyword")
    keywords = [kw.text for kw in keyword_tags if kw.text]

    # Authors
    authors_list = []
    for author in soup.find_all("Author"):
        firstname = author.find("ForeName")
        lastname = author.find("LastName")
        if firstname and lastname:
            authors_list.append(f"{firstname.text} {lastname.text}")

    # Journal
    journal_tag = soup.find("Title")
    journal = journal_tag.text if journal_tag else "No Journal Found"

    # DOI
    doi_tag = soup.find("ArticleId", {"IdType": "doi"})
    doi = doi_tag.text if doi_tag else "No DOI Found"

    # Fulltext link
    fulltext_link_tag = soup.find("ELocationID", {"EIdType": "doi", "ValidYN": "Y"})
    fulltext_link = f"https://doi.org/{fulltext_link_tag.text}" if fulltext_link_tag else "No Full Text Link"

    # PMCID
    pmcid_tag = soup.find("ArticleId", {"IdType": "pmc"})
    pmcid = pmcid_tag.text if pmcid_tag else "No PMC ID"

    # (Optional) volume/issue/pages
    volume_tag = soup.find("Volume")
    issue_tag = soup.find("Issue")
    pages_tag = soup.find("MedlinePgn")
    volume = volume_tag.text if volume_tag else ""
    issue = issue_tag.text if issue_tag else ""
    pages = pages_tag.text if pages_tag else ""

    paper_data = {
        "pubmed_id": pubmed_id,
        "title": title,
        "abstract": abstract,
        "authors": authors_list,
        "keywords": keywords,
        "journal": journal,
        "publication_date": publication_date,
        "doi": doi,
        "fulltext_link": fulltext_link,
        "pmcid": pmcid,
        "volume": volume,
        "issue": issue,
        "pages": pages
    }
    return paper_data

# -------------------- Main Extraction Logic --------------------

def process_pubmed_id(pid, db, abbr_map, run_log_id):
    """
    1) Check if article in DB has a valid PDF. If yes, skip re-download.
    2) Fetch details from PubMed or handle any error.
    3) Re-use updated_title if present; otherwise, compute new.
    4) Download PDF using .tmp approach to avoid partial files.
    5) Insert/update DB record only after successful download, or mark "Not downloaded".
    """
    existing_doc = db["articles"].find_one({"pubmed_id": pid})

    # Skip if we already have a valid PDF
    if existing_doc:
        pdf_status = existing_doc.get("pdf_file", "")
        # If pdf_file is neither "Not downloaded", "Not available", None, nor empty,
        # we assume the PDF is already downloaded successfully
        if pdf_status not in [None, "Not downloaded", "Not available", ""]:
            print(f"PubMed ID {pid} already has PDF: {pdf_status}. Skipping re-download.")
            return {"pubmed_id": pid, "skipped": True}

    # Fetch metadata
    details = fetch_pubmed_details(pid)
    if "error" in details:
        # Log the error in run_logs
        db["run_logs"].update_one(
            {"_id": run_log_id},
            {"$push": {
                "errors": {
                    "pubmed_id": pid,
                    "error": details["error"],
                    "timestamp": datetime.now()
                }
            }}
        )
        return {"pubmed_id": pid, "error": details["error"]}

    # Re-use existing updated_title if available
    if existing_doc and "updated_title" in existing_doc:
        updated_title = existing_doc["updated_title"]
        print(f"Reusing existing updated_title for PubMed ID {pid}: {updated_title}")
    else:
        updated_title = compute_updated_title(details, abbr_map)

    details["updated_title"] = updated_title

    # Attempt PDF download
    new_pdf_filename = sanitize_filename(updated_title) + ".pdf"
    pdf_file = attempt_pdf_download(details, new_filename=new_pdf_filename)

    if pdf_file is None or pdf_file in ["Not downloaded", "Not available"]:
        details["pdf_file"] = "Not downloaded"
        details["access"] = "Paid"  # or "Unknown"
    else:
        details["pdf_file"] = pdf_file
        details["access"] = "Free"

    # Insert or update DB
    if existing_doc:
        db["articles"].update_one({"_id": existing_doc["_id"]}, {"$set": details})
        print(f"Updated article with PubMed ID: {pid}")
    else:
        db["articles"].insert_one(details)
        print(f"Inserted new article with PubMed ID: {pid}")

    return {
        "pubmed_id": pid,
        "access": details["access"],
        "pdf_file": details["pdf_file"]
    }

def run_extraction(use_parallel=False):
    """
    Automated extraction process that:
      1) Initializes DB & loads new keywords/abbreviations.
      2) Determines date range (last successful run vs 30 days).
      3) For each keyword, fetches PubMed IDs and processes them.
      4) Collects citations for Paid articles in a single list (paid_citations), 
         avoiding duplicates by tracking paid_ids_seen.
      5) AFTER all keywords, writes one set of text files to 'citations' folder,
         each file holding up to 50 citations.
    """

    db = init_db()
    keywords = get_keywords(db)
    if not keywords:
        print("No keywords found in the database. Exiting extraction.")
        return

    abbr_map = get_abbreviation_map(db)

    # Create a run log entry
    start_time = datetime.now()
    run_log = {
        "start_time": start_time,
        "status": "started",
        "keywords": keywords,
        "articles_processed": 0,
        "errors": []
    }
    run_log_id = db["run_logs"].insert_one(run_log).inserted_id
    print(f"Run started at {start_time}")

    try:
        # Determine date range
        last_successful = get_last_successful_run_date(db)
        if last_successful is None:
            start_date = datetime.now() - timedelta(days=30)
        else:
            start_date = last_successful
        end_date = datetime.now()

        total_articles_processed = 0

        # ONE list for ALL paid citations across all keywords
        paid_citations = []
        # A set of PubMed IDs so we don't add duplicate citations
        paid_ids_seen = set()

        # Loop over each keyword just once
        for keyword in keywords:
            print(f"Processing keyword: {keyword}")
            pubmed_ids = search_pubmed_date_range(keyword, start_date, end_date)
            if not pubmed_ids:
                print(f"No new articles found for keyword '{keyword}'.")
                continue

            # Parallel or serial
            if use_parallel:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for pid in pubmed_ids:
                        futures.append(executor.submit(
                            process_pubmed_id, pid, db, abbr_map, run_log_id
                        ))
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        if result and not result.get("skipped"):
                            total_articles_processed += 1
                            if result.get("access") == "Paid":
                                # Avoid duplicates
                                if result["pubmed_id"] not in paid_ids_seen:
                                    doc = db["articles"].find_one({"pubmed_id": result["pubmed_id"]})
                                    if doc:
                                        citation = generate_citation(doc)
                                        paid_citations.append(citation)
                                    paid_ids_seen.add(result["pubmed_id"])
            else:
                # Serial approach
                for pid in pubmed_ids:
                    result = process_pubmed_id(pid, db, abbr_map, run_log_id)
                    if result and not result.get("skipped"):
                        total_articles_processed += 1
                        if result.get("access") == "Paid":
                            # Avoid duplicates
                            if pid not in paid_ids_seen:
                                doc = db["articles"].find_one({"pubmed_id": pid})
                                if doc:
                                    citation = generate_citation(doc)
                                    paid_citations.append(citation)
                                paid_ids_seen.add(pid)

        # ========================================================
        # WRITE CITATIONS *ONCE* AFTER ALL KEYWORDS ARE PROCESSED
        # ========================================================
        if paid_citations:
            os.makedirs("citations", exist_ok=True)

            # Each file should have up to 50 citations
            num_files = (len(paid_citations) + 49) // 50
            for i in range(num_files):
                chunk = paid_citations[i*50 : (i+1)*50]
                citation_text = "\n\n".join(chunk)

                file_path = os.path.join("citations", f"paid_articles_citations_{i+1}.txt")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(citation_text)

                print(f"Paid article citations written to {file_path}")
        else:
            print("No paid articles found to generate citations.")

        # Mark the run as complete
        end_time = datetime.now()
        db["run_logs"].update_one(
            {"_id": run_log_id},
            {"$set": {
                "end_time": end_time,
                "status": "completed",
                "articles_processed": total_articles_processed
            }}
        )
        print(f"Extraction run completed at {end_time}. Total articles processed: {total_articles_processed}")

    except Exception as e:
        # If any error occurs, update the run log accordingly
        end_time = datetime.now()
        db["run_logs"].update_one(
            {"_id": run_log_id},
            {"$set": {
                "end_time": end_time,
                "status": "error",
                "error": str(e)
            }}
        )
        print(f"Extraction run encountered an error at {end_time}: {e}")

# Example usage:
if __name__ == "__main__":
    # Set `use_parallel=True` to fetch article details in parallel (optional).
    run_extraction(use_parallel=False)
