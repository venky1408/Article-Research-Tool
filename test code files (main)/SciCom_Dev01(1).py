import os
import re
import requests
import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# -------------------- Utility Functions --------------------
def sanitize_filename(s):
    """
    Convert a string into a safe filename by removing or replacing characters
    that are not letters, digits, spaces, underscores, or hyphens.
    """
    # Allow letters (a-z, A-Z), digits (0-9), underscores (_), hyphens (-), and spaces
    s = re.sub(r'[^a-zA-Z0-9_\- ]', '', s)
    
    # Optionally collapse multiple spaces into a single space and trim whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    
    return s

def replace_with_abbreviations(text, abbr_dict):
    """
    Replace full terms in 'text' with their abbreviations from 'abbr_dict'
    using regex patterns that ensure exact matching by using negative lookbehind
    and negative lookahead so that partial words are not replaced.
    """
    if not text or not abbr_dict:
        return text

    result = text
    # Sort the terms by length in descending order to replace longer phrases first
    sorted_terms = sorted(abbr_dict.keys(), key=len, reverse=True)
    for term in sorted_terms:
        # This pattern ensures the term is not part of a larger word.
        pattern = re.compile(r'(?<!\w)' + re.escape(term) + r'(?!\w)', re.IGNORECASE)
        result = pattern.sub(abbr_dict[term], result)
    return result

def generate_citation(details):
    """
    Generate a citation string for a paid article in the format:
    
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

def download_pdf_direct(url, filename):
    """
    Downloads a PDF directly from 'url' using requests, saves it to 'pdfs/<filename>'.
    Returns the file path if successful, else None.
    """
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/115.0.0.0 Safari/537.36"),
        "Referer": url,
        "Accept": "application/pdf"
    }
    try:
        r = requests.get(url, stream=True, timeout=10, headers=headers)
        content_type = r.headers.get("Content-Type", "").lower()
        if r.status_code == 200 and "pdf" in content_type:
            os.makedirs("pdfs", exist_ok=True)
            filepath = os.path.join("pdfs", filename)
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=4096):
                    if chunk:
                        f.write(chunk)
            print(f"Downloaded PDF to {filepath}")
            return filepath
        else:
            print(f"PDF not available at {url} (status code: {r.status_code}).")
            return None
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        return None

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

def download_pmc_pdf(pmcid, new_filename=None):
    """
    Build the PMC article URL, load it using Selenium,
    wait explicitly for the PDF link to be clickable,
    then download the PDF.
    If new_filename is provided, it will be used for saving the PDF.
    """
    base_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
    driver = setup_selenium()
    driver.get(base_url)
    
    wait = WebDriverWait(driver, 10)
    try:
        pdf_link_elem = wait.until(
            EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "PDF"))
        )
        pdf_url = pdf_link_elem.get_attribute("href")
        if pdf_url.startswith("/"):
            pdf_url = "https://www.ncbi.nlm.nih.gov" + pdf_url
        
        if new_filename is None:
            new_filename = sanitize_filename(pmcid) + ".pdf"
        pdf_path = download_pdf_direct(pdf_url, new_filename)
    except Exception as e:
        print(f"No PDF element found or error occurred: {e}")
        pdf_path = None
    finally:
        driver.quit()
    return pdf_path

def attempt_pdf_download(details, new_filename=None):
    """
    Attempts to download the PDF for a given article's details.
    If new_filename is provided, it is used as the filename.
    Returns the file path if downloaded, or an appropriate status string.
    """
    if details["fulltext_link"] != "No Full Text Link" and "pdf" in details["fulltext_link"].lower():
        if new_filename is None:
            new_filename = f"{sanitize_filename(details['pubmed_id'])}.pdf"
        pdf_file = download_pdf_direct(details["fulltext_link"], new_filename)
        return pdf_file if pdf_file else "Not downloaded"
    elif details["pmcid"] != "No PMC ID":
        pdf_file = download_pmc_pdf(details["pmcid"], new_filename=new_filename)
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

def init_db():
    """
    Initialize the database by ensuring that the required collections exist.
    Required collections:
      - articles: to store metadata of extracted articles (with updated titles)
      - keywords: to store the list of keywords for article extraction (imported from Keywords.csv on first run)
      - abbreviations: to store abbreviation mappings for renaming articles (imported from Abbreviations.csv on first run)
      - run_logs: to log details of each code run
    """
    db = connect_to_mongo()
    required_collections = ["articles", "keywords", "abbreviations", "run_logs"]
    existing_collections = db.list_collection_names()
    for coll in required_collections:
        if coll not in existing_collections:
            db.create_collection(coll)
            print(f"Created collection: {coll}")

    # Import keywords if empty
    if db["keywords"].count_documents({}) == 0:
        try:
            csv_path = r"C:\Users\saisr\OneDrive\Desktop\Capstone\Keywords.csv"
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                keyword_docs = [row for row in reader]
                if keyword_docs:
                    db["keywords"].insert_many(keyword_docs)
                    print("Inserted keywords from CSV.")
        except Exception as e:
            print(f"Error importing Keywords.csv: {e}")
    else:
        print("Keywords collection already populated.")
        
    # Import abbreviations if empty
    if db["abbreviations"].count_documents({}) == 0:
        try:
            csv_path = r"C:\Users\saisr\OneDrive\Desktop\Capstone\Abbreviations.csv"
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                abbrev_docs = [row for row in reader]
                if abbrev_docs:
                    db["abbreviations"].insert_many(abbrev_docs)
                    print("Inserted abbreviations from CSV.")
        except Exception as e:
            print(f"Error importing Abbreviations.csv: {e}")
    else:
        print("Abbreviations collection already populated.")
    return db

def get_keywords(db):
    """
    Retrieve the list of keywords from the 'keywords' collection.
    Checks for BOM-prefixed and common field names.
    """
    keywords = []
    for doc in db["keywords"].find():
        k = doc.get("\ufeffKeyword") or doc.get("Keyword") or doc.get("keyword")
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

# -------------------- Abbreviation Mapping and Updated Title Computation --------------------

def get_abbreviation_map(db):
    abbr_map = {}
    for doc in db["abbreviations"].find():
        # Retrieve the term using the available keys and strip extra spaces.
        full_term = (doc.get("Term") or doc.get("term") or doc.get("\ufeffTerm"))
        abbr = (doc.get("\ufeffAbbreviation") or doc.get("Abbreviation") or doc.get("abbreviation"))
        if full_term and abbr:
            # Remove trailing/leading spaces and store keys in lowercase for case-insensitive matching.
            abbr_map[full_term.strip().lower()] = abbr.strip()
    return abbr_map


def compute_updated_title(details, abbr_map):
    """
    Constructs an updated title in the format:
         <updated title> <FIRST_AUTHOR_LAST> <publication year>
    The updated title is created by first normalizing the title (e.g. replacing punctuation
    like hyphens, commas, semicolons, etc. with spaces), then replacing any matching full term
    with its abbreviation using the robust replacement function, and finally appending the first
    author's last name (in uppercase) and the publication year.
    """
    original_title = details.get("title", "")
    
    # Debug: Show original title before normalization
    print("DEBUG: Original title:", original_title)
    
    # Normalize title: replace hyphens/dashes and common punctuation with spaces
    normalized_title = re.sub(r'[-–—]', ' ', original_title)  # Replace hyphens/dashes with space
    normalized_title = re.sub(r'[.,;:()]+', ' ', normalized_title)  # Replace punctuation with space
    normalized_title = re.sub(r'\s+', ' ', normalized_title).strip()  # Collapse multiple spaces
    
    print("DEBUG: Normalized title:", normalized_title)
    
    # Replace full terms with abbreviations using our robust function
    updated_title = replace_with_abbreviations(normalized_title, abbr_map)
    print("DEBUG: Updated title after abbreviation:", updated_title)

    # Extract first author's last name in uppercase.
    authors = details.get("authors", [])
    first_author_last = ""
    if authors:
        parts = authors[0].split()
        if parts:
            first_author_last = parts[-1].upper()

    # Extract publication year (assumes format "YYYY-MM-DD" or similar)
    pub_date = details.get("publication_date", "")
    pub_year = ""
    if pub_date and pub_date != "No Publication Date":
        pub_year = pub_date.split("-")[0]

    final_title = f"{updated_title} {first_author_last} {pub_year}".strip()
    return final_title


# -------------------- PubMed API Functions --------------------

def search_pubmed_date_range(query, start_date, end_date, max_results=100):
    """
    Use PubMed eSearch API to get PubMed IDs for articles published between start_date and end_date.
    Returns a list of PubMed ID strings.
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    mindate = start_date.strftime("%Y/%m/%d")
    maxdate = end_date.strftime("%Y/%m/%d")
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": max_results,
        "datetype": "pdat",
        "mindate": mindate,
        "maxdate": maxdate
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    return data["esearchresult"].get("idlist", [])

def fetch_pubmed_details(pubmed_id):
    """
    Use PubMed eFetch API to retrieve detailed metadata for a single PubMed ID.
    Returns a dictionary containing title, abstract, authors, keywords, journal,
    publication date, DOI, full-text link, and PMCID.
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": pubmed_id,
        "retmode": "xml"
    }
    response = requests.get(base_url, params=params)
    soup = BeautifulSoup(response.text, "xml")
    
    title_tag = soup.find("ArticleTitle")
    title = title_tag.text if title_tag else "No Title Found"
    
    abstract_tag = soup.find("AbstractText")
    abstract = abstract_tag.text if abstract_tag else "No Abstract Found"
    
    keyword_tags = soup.find_all("Keyword")
    keywords = [kw.text for kw in keyword_tags] if keyword_tags else []
    
    authors_list = []
    for author in soup.find_all("Author"):
        firstname = author.find("ForeName")
        lastname = author.find("LastName")
        if firstname and lastname:
            authors_list.append(f"{firstname.text} {lastname.text}")
    
    journal_tag = soup.find("Title")
    journal = journal_tag.text if journal_tag else "No Journal Found"
    
    date_tag = soup.find("PubDate")
    if date_tag:
        year = date_tag.find("Year").text if date_tag.find("Year") else "N/A"
        month = date_tag.find("Month").text if date_tag.find("Month") else "N/A"
        day = date_tag.find("Day").text if date_tag.find("Day") else "N/A"
        publication_date = f"{year}-{month}-{day}"
    else:
        publication_date = "No Publication Date"
    
    doi_tag = soup.find("ArticleId", {"IdType": "doi"})
    doi = doi_tag.text if doi_tag else "No DOI Found"
    
    fulltext_link_tag = soup.find("ELocationID", {"EIdType": "doi", "ValidYN": "Y"})
    fulltext_link = f"https://doi.org/{fulltext_link_tag.text}" if fulltext_link_tag else "No Full Text Link"
    
    pmcid_tag = soup.find("ArticleId", {"IdType": "pmc"})
    pmcid = pmcid_tag.text if pmcid_tag else None

    paper_data = {
        "pubmed_id": pubmed_id,
        "title": title,
        "abstract": abstract,
        "keywords": keywords,
        "authors": authors_list,
        "journal": journal,
        "publication_date": publication_date,
        "doi": doi,
        "fulltext_link": fulltext_link,
        "pmcid": pmcid if pmcid else "No PMC ID"
    }
    return paper_data

# -------------------- Main Extraction Logic with Run Logging and Citation Generation --------------------

def run_extraction():
    """
    Automated extraction process:
      1. Initialize the database and required collections (importing keywords and abbreviations from CSV if first run).
      2. Log the start of the run.
      3. Determine the extraction date range:
         - First run: extract articles from the last month.
         - Subsequent runs: extract articles from the end_time of the last successful run until now.
      4. For each keyword, search PubMed for articles within the date range.
      5. For each article:
            - Fetch detailed metadata.
            - Compute an updated title by replacing matching abbreviation terms (case-insensitive)
              with their abbreviations, then appending the first author's last name (uppercase)
              and publication year.
            - Use a sanitized version of the updated title as the PDF filename.
            - Attempt to download the PDF.
            - Mark the article as 'Free' if PDF is downloaded, otherwise 'Paid'.
            - Generate a citation if the article is "Paid".
            - Update or insert the article in the 'articles' collection.
      6. Write out citations for paid articles to text files (max 50 citations per file).
      7. Log the end of the run with status "completed" (or "error" if an exception occurs).
    """
    db = init_db()
    keywords = get_keywords(db)
    if not keywords:
        print("No keywords found in the database. Exiting extraction.")
        return

    abbr_map = get_abbreviation_map(db)
    
    # Log the start of the run.
    start_time = datetime.now()
    run_log = {
        "start_time": start_time,
        "status": "started",
        "keywords": keywords,
        "articles_processed": 0,
        "details": []
    }
    run_log_id = db["run_logs"].insert_one(run_log).inserted_id
    print(f"Run started at {start_time}")

    try:
        last_successful = get_last_successful_run_date(db)
        if last_successful is None:
            start_date = datetime.now() - timedelta(days=30)
        else:
            start_date = last_successful
        end_date = datetime.now()
        total_articles_processed = 0

        paid_citations = []  # Initialize list for citations

        for keyword in keywords:
            print(f"Processing keyword: {keyword}")
            pubmed_ids = search_pubmed_date_range(keyword, start_date, end_date)
            for pid in pubmed_ids:
                details = fetch_pubmed_details(pid)
    
                # Compute updated title with robust abbreviation replacement
                updated_title = compute_updated_title(details, abbr_map)
                details["updated_title"] = updated_title

                # Create the PDF filename
                new_pdf_filename = sanitize_filename(updated_title) + ".pdf"

                # Attempt to download the PDF using the new filename
                pdf_file = attempt_pdf_download(details, new_filename=new_pdf_filename)
                details["pdf_file"] = pdf_file
                details["access"] = "Free" if pdf_file not in ["Not downloaded", "Not available"] else "Paid"

                # If article is paid, generate citation and add to list
                if details["access"] == "Paid":
                    citation = generate_citation(details)
                    paid_citations.append(citation)

                existing_doc = db["articles"].find_one({"pubmed_id": pid})
                if existing_doc:
                    db["articles"].update_one({"_id": existing_doc["_id"]}, {"$set": details})
                    print(f"Updated article with PubMed ID: {pid}")
                else:
                    db["articles"].insert_one(details)
                    print(f"Inserted new article with PubMed ID: {pid}")
                total_articles_processed += 1

        # Write out citations to text files (max 50 per file)
        if paid_citations:
            num_files = (len(paid_citations) - 1) // 50 + 1
            for i in range(num_files):
                chunk = paid_citations[i*50:(i+1)*50]
                citation_text = "\n\n".join(chunk)
                file_name = f"paid_articles_citations_{i+1}.txt"
                with open(file_name, "w", encoding="utf-8") as f:
                    f.write(citation_text)
                print(f"Paid articles citations written to {file_name}")
        else:
            print("No paid articles found to generate citations.")

        # Log run completion.
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

if __name__ == "__main__":
    run_extraction()