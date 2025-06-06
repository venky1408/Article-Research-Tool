from pymongo import MongoClient
from datetime import datetime

# Connect to Mongo
client = MongoClient("mongodb://localhost:27017")  # or your actual Mongo URI
db = client["research_papers"]
collection = db["articles"]

# Convert datetime → string YYYY-MM-DD
docs = collection.find({"webscraped_date": {"$type": "date"}})

updated = 0
for doc in docs:
    dt = doc["webscraped_date"]
    clean_date = dt.strftime("%Y-%m-%d")
    collection.update_one(
        {"_id": doc["_id"]},
        {"$set": {"webscraped_date": clean_date}}
    )
    updated += 1

print(f"✅ Updated {updated} documents.")
