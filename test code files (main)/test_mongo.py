import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient

def connect_to_mongo():
    """
    Connect to the local MongoDB (or replace the URI with your Atlas connection string).
    Returns a reference to the 'pubmed_data' collection.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["research_papers"]
    collection = db["pubmed_data"]
    return collection

def search_pubmed(query, max_results=10):
    """
    Use PubMed eSearch to get a list of PubMed IDs for a given query.
    Returns a list of PubMed ID strings.
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": max_results
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    
    # Safely extract the ID list
    id_list = data["esearchresult"].get("idlist", [])
    return id_list

def fetch_pubmed_details(pubmed_id):
    """
    Fetch detailed metadata (title, abstract, authors, etc.) for a single PubMed ID
    using the eFetch endpoint, and return a dictionary of extracted fields.
    """
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": pubmed_id,
        "retmode": "xml"
    }
    response = requests.get(base_url, params=params)
    # Make sure you've installed lxml: pip install lxml
    soup = BeautifulSoup(response.text, "xml")

    # Extract Title
    title_tag = soup.find("ArticleTitle")
    title = title_tag.text if title_tag else "No Title Found"

    # Extract Abstract
    abstract_tag = soup.find("AbstractText")
    abstract = abstract_tag.text if abstract_tag else "No Abstract Found"

    # Extract Keywords (note: these are not always present)
    keyword_tags = soup.find_all("Keyword")
    keywords = [kw.text for kw in keyword_tags] if keyword_tags else []

    # Extract Journal Name
    journal_tag = soup.find("Title")
    journal = journal_tag.text if journal_tag else "No Journal Found"

    # Extract Publication Date
    date_tag = soup.find("PubDate")
    if date_tag:
        year = date_tag.find("Year").text if date_tag.find("Year") else "N/A"
        month = date_tag.find("Month").text if date_tag.find("Month") else "N/A"
        day = date_tag.find("Day").text if date_tag.find("Day") else "N/A"
        publication_date = f"{year}-{month}-{day}"
    else:
        publication_date = "No Publication Date"

    # Extract Authors
    authors_list = []
    author_tags = soup.find_all("Author")
    for author in author_tags:
        lastname = author.find("LastName")
        firstname = author.find("ForeName")
        if lastname and firstname:
            authors_list.append(f"{firstname.text} {lastname.text}")

    # Return a dictionary with all extracted details
    paper_data = {
        "pubmed_id": pubmed_id,
        "title": title,
        "abstract": abstract,
        "keywords": keywords,
        "journal": journal,
        "publication_date": publication_date,
        "authors": authors_list
    }
    return paper_data

def scrape_pubmed_and_store(queries, max_results=10):
    """
    Takes in a list of queries and a max_results integer.
    For each query:
      1. Search for PubMed IDs.
      2. Fetch detailed metadata for each ID.
      3. Insert/Update in MongoDB, keeping track of which queries found this article.
    """
    collection = connect_to_mongo()

    for query in queries:
        print(f"\n=== Searching PubMed for query: '{query}' ===")
        pubmed_ids = search_pubmed(query, max_results)

        if not pubmed_ids:
            print(f"No articles found for query: {query}")
            continue
        
        print(f"Found {len(pubmed_ids)} PubMed IDs for query '{query}': {pubmed_ids}")

        for pid in pubmed_ids:
            details = fetch_pubmed_details(pid)

            # Check if this article already exists
            existing_doc = collection.find_one({"pubmed_id": pid})
            if existing_doc:
                # Already in the DB, so we just update its 'search_queries' field
                search_queries = existing_doc.get("search_queries", [])
                if query not in search_queries:
                    search_queries.append(query)
                    collection.update_one(
                        {"_id": existing_doc["_id"]},
                        {"$set": {"search_queries": search_queries}}
                    )
                    print(f"Updated PubMed ID {pid} to include new query '{query}'.")
                else:
                    print(f"PubMed ID {pid} already includes query '{query}'. Skipping.")
            else:
                # Insert a new document
                # Add a new field 'search_queries' to track which queries found this article
                details["search_queries"] = [query]
                collection.insert_one(details)
                print(f"Inserted '{details['title']}' (PubMed ID {pid}) into the database.")

def main():
    # Prompt the user for multiple queries (comma-separated)
    user_input = input(
        "Enter your PubMed search queries (comma-separated):\n"
        "Example: 'transcatheter aortic valve replacement, heart valve'\n> "
    )
    # Split by comma, strip whitespace
    queries = [q.strip() for q in user_input.split(",") if q.strip()]

    # Make sure we have at least one query
    if not queries:
        print("No queries entered. Exiting.")
        return

    # Prompt for max_results
    while True:
        try:
            max_results_input = input("Enter the number of articles to fetch per query: ")
            max_results = int(max_results_input)
            if max_results < 1:
                print("Please enter a positive integer.")
                continue
            break
        except ValueError:
            print("Invalid input. Please enter a valid integer.")
    
    # Call the scraping function
    scrape_pubmed_and_store(queries, max_results)

if __name__ == "__main__":
    main()
