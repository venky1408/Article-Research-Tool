from pymongo import MongoClient
from config import MONGO_URI

def test_article_text_upsert():
    db = MongoClient(MONGO_URI)["research_papers"]
    sample_pid = "TEST123456"
    db["article_text"].delete_many({"pubmed_id": sample_pid})

    db["article_text"].update_one(
        {"pubmed_id": sample_pid},
        {"$set": {"full_text": "hello"}},
        upsert=True
    )

    doc = db["article_text"].find_one({"pubmed_id": sample_pid})
    assert doc and doc["full_text"] == "hello"