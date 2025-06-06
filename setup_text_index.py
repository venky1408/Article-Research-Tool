from pymongo import MongoClient
from config import MONGO_URI

db = MongoClient(MONGO_URI)["research_papers"]
db["article_text"].create_index("pubmed_id", unique=True)
print("✅  Unique index on article_text.pubmed_id created.")
