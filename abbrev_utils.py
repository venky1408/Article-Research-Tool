import re

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
        if len(pub_date) >= 2:
            year_month = f"{pub_year[0]}-{pub_year[1]}"
        else:
            year_month = pub_year[0]
    final_title = f"{updated_title} {first_author_last} {pub_year}".strip()
    return final_title

