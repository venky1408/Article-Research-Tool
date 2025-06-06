import re
import pandas as pd
from config import KEYWORDS_CSV, ABBREVS_CSV

# -------------------- Utility Functions --------------------
def sanitize_filename(s):
    """
    Convert a string into a safe filename by removing or replacing characters
    that are not letters, digits, spaces, underscores, or hyphens.
    """
    s = re.sub(r'[^a-zA-Z0-9_\- ]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


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

def load_keywords_from_csv():
    try:
        df = pd.read_csv(KEYWORDS_CSV)
        return df["keyword"].dropna().tolist()
    except Exception as e:
        print(f"❌ Error loading keywords from {KEYWORDS_CSV}: {e}")
        return []
    
def load_abbreviation_map():
    try:
        df = pd.read_csv(ABBREVS_CSV)
        abbrev_map = dict(zip(df["Term"], df["Abbreviation"]))
        return abbrev_map
    except Exception as e:
        print(f"❌ Error loading abbreviations from {ABBREVS_CSV}: {e}")
        return {}