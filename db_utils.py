from pymongo import MongoClient
import csv
from datetime import datetime
import os
from config import MONGO_URI, KEYWORDS_CSV, ABBREVS_CSV 


def connect_to_mongo():
    """
    Connect to the local MongoDB.
    Returns a reference to the 'research_papers' database.
    """
    client = MongoClient(MONGO_URI)
    db = client["research_papers"]
    return db

def import_keywords(db):
    """
    Merge keywords from keywords.csv into the 'keywords' collection (upsert).
    """
    try:
        with open(KEYWORDS_CSV, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                keyword = row.get("Keyword") or row.get("\ufeffKeyword") or row.get("keyword")
                if keyword:
                    db["keywords"].update_one(
                        {"Keyword": keyword},
                        {"$set": {"Keyword": keyword}},
                        upsert=True
                    )
        print("Keywords merged/updated from CSV.")
    except Exception as e:
        print(f"Error importing keywords.csv: {e}")

def import_abbreviations(db):
    """
    Merge abbreviations from abbreviations.csv into the 'abbreviations' collection (upsert).
    """
    try:
        with open(ABBREVS_CSV, newline='', encoding='utf-8') as csvfile:
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
        print(f"Error importing abbreviations.csv: {e}")

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
