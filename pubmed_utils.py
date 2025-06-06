from bs4 import BeautifulSoup
import requests
import time
from datetime import datetime
from dateutil import parser as date_parser
from config import NCBI_API_KEY

# -------------------- PubMed Helpers --------------------

def parse_pub_date(pub_date_tag):
    if not pub_date_tag:
        return "No Publication Date"

    year = pub_date_tag.find("Year")
    month = pub_date_tag.find("Month")
    day = pub_date_tag.find("Day")

    date_str_parts = []
    if year:
        date_str_parts.append(year.text)
    if month:
        date_str_parts.append(month.text)
    if day:
        date_str_parts.append(day.text)

    date_str = " ".join(date_str_parts).strip()
    if not date_str:
        return "No Publication Date"

    try:
        dt = date_parser.parse(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str

def search_pubmed_date_range(query, start_date, end_date, max_results=1000):
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    mindate = start_date.strftime("%Y/%m/%d")
    maxdate = end_date.strftime("%Y/%m/%d")

    all_ids = []
    retstart = 0
    batch_size = 100
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
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": pubmed_id,
        "retmode": "xml",
        "api_key": NCBI_API_KEY
    }

    for attempt in range(3):
        try:
            response = requests.get(base_url, params=params, timeout=10)
            if response.status_code == 429:
                print(f"[429] Rate limit hit for ID {pubmed_id}. Sleeping 60s… (Attempt {attempt+1})")
                time.sleep(60)
                continue
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching PubMed data for ID {pubmed_id}: {e}")
            return {"pubmed_id": pubmed_id, "error": f"Request error: {e}"}

    try:
        soup = BeautifulSoup(response.text, "xml")
    except Exception as e:
        print(f"XML parsing error for PubMed ID {pubmed_id}: {e}")
        return {"pubmed_id": pubmed_id, "error": f"XML parsing error: {e}"}

    # Extract Title
    title_tag = soup.find("ArticleTitle")
    title = title_tag.text if title_tag else "No Title Found"

    # Extract Abstract
    abstract_tags = soup.find_all("AbstractText")
    abstract = "\n".join(
        f"{atag.get('Label') + ': ' if atag.get('Label') else ''}{atag.text}"
        for atag in abstract_tags
    ) if abstract_tags else "No Abstract Found"

    # Extract Pub Date
    date_tag = soup.find("PubDate")
    publication_date = parse_pub_date(date_tag)

    # Extract Keywords
    keyword_tags = soup.find_all("Keyword")
    keywords = [kw.text for kw in keyword_tags if kw.text]

    # Extract Authors
    authors_list = []
    for author in soup.find_all("Author"):
        firstname = author.find("ForeName")
        lastname = author.find("LastName")
        if firstname and lastname:
            authors_list.append(f"{firstname.text} {lastname.text}")

    # Extract Journal
    journal_tag = soup.find("Title")
    journal = journal_tag.text if journal_tag else "No Journal Found"

    # Extract DOI
    doi_tag = soup.find("ArticleId", {"IdType": "doi"})
    doi = doi_tag.text.strip() if doi_tag else "No DOI Found"

    # Fulltext Link
    fulltext_link_tag = soup.find("ELocationID", {"EIdType": "doi", "ValidYN": "Y"})
    fulltext_link = f"https://doi.org/{fulltext_link_tag.text.strip()}" if fulltext_link_tag else "No Full Text Link"

    # ✅ Safe PMCID extraction with forced prefix
    pmcid = "No PMC ID"
    article_ids = soup.find_all("ArticleId")
    for aid in article_ids:
        if aid.get("IdType") == "pmc":
            pmcid_raw = aid.text.strip()
            pmcid = pmcid_raw if pmcid_raw.startswith("PMC") else f"PMC{pmcid_raw}"
            break

    # ✅ Final safety: verify PMCID belongs to the same PubMed ID
    pubmed_id_tag = soup.find("ArticleId", {"IdType": "pubmed"})
    if pubmed_id_tag and pubmed_id_tag.text.strip() != pubmed_id:
        print(f"⚠️ PMCID mismatch: PMCID {pmcid} does not belong to PubMed ID {pubmed_id}")
        pmcid = "No PMC ID"

    # Determine Free vs Paid access
    access = "Paid"
    if pmcid.startswith("PMC"):
        oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
        try:
            oa_response = requests.get(oa_url, timeout=10)
            if oa_response.status_code == 200 and "<link" in oa_response.text:
                access = "Free"
        except Exception as e:
            print(f"OA API check failed for {pmcid}: {e}")

    # Extract volume/issue/pages
    volume_tag = soup.find("Volume")
    issue_tag = soup.find("Issue")
    pages_tag = soup.find("MedlinePgn")
    volume = volume_tag.text if volume_tag else ""
    issue = issue_tag.text if issue_tag else ""
    pages = pages_tag.text if pages_tag else ""

    # ✅ Construct paper_data (after pmcid is finalized)
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
        "pmcid": pmcid if pmcid != "No PMC ID" else None,  # force None if invalid
        "access": access,
        "volume": volume,
        "issue": issue,
        "pages": pages
    }

    return paper_data


