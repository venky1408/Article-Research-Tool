from flask import Flask, render_template, redirect, url_for, request, send_from_directory
from bson.objectid import ObjectId
from pymongo import MongoClient
import os, math
from dotenv import load_dotenv
from flask import Blueprint
from db_utils import connect_to_mongo

# Load environment
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
PDF_DIR   = os.getenv("PDF_DIR", "pdfs")

app = Flask(__name__)  # Use default static folder "static"

analytics_bp = Blueprint('analytics_bp', __name__)
db = connect_to_mongo()

@analytics_bp.route("/analytics")
def analytics():
    articles_coll = db["articles"]
    logs_coll = db["run_logs"]

    # Total articles
    total_articles = articles_coll.count_documents({})

    # Status breakdown
    status_counts = {
        "approved": articles_coll.count_documents({"status": "approved"}),
        "rejected": articles_coll.count_documents({"status": "rejected"}),
        "pending": articles_coll.count_documents({"status": {"$exists": False}})
    }

    # Access type breakdown
    access_counts = {
        "Free": articles_coll.count_documents({"access": "Free"}),
        "Paid": articles_coll.count_documents({"access": "Paid"})
    }

    # Last run date
    last_run = logs_coll.find_one({"status": "completed"}, sort=[("end_time", -1)])
    last_run_date = last_run["end_time"].strftime("%Y-%m-%d %H:%M:%S") if last_run else "N/A"

    # PDF download health
    downloaded_pdfs = articles_coll.count_documents({"pdf_file": {"$regex": ".pdf$"}})
    pdf_health = {
        "downloaded": downloaded_pdfs,
        "missing": total_articles - downloaded_pdfs,
        "percent": round((downloaded_pdfs / total_articles) * 100, 2) if total_articles else 0
    }

    # Keyword performance (top 10)
    keyword_stats = articles_coll.aggregate([
        {"$unwind": "$keywords"},
        {"$group": {"_id": "$keywords", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    keyword_stats = list(keyword_stats)

    return render_template("analytics.html",
        total_articles=total_articles,
        status_counts=status_counts,
        access_counts=access_counts,
        last_run_date=last_run_date,
        pdf_health=pdf_health,
        keyword_stats=keyword_stats
    )

app.register_blueprint(analytics_bp)

def get_db():
    client = MongoClient(MONGO_URI)
    return client["research_papers"]

@app.route("/")
def index():
    db = get_db()
    coll = db["articles"]

    # 1) Which tab?
    filter_category = request.args.get("filter", "pending")
    tag_filter      = request.args.get("tag", "").strip()

    # 2) Build query: Pending, Free, Paid exclude reviewed; Approved/Rejected show only status
    query = {}
    if filter_category == "pending":
        query["$or"] = [
            {"status": {"$exists": False}},
            {"status": "Pending"}          # ← include docs the scraper marked
        ]
    elif filter_category == "free":
        query = {
            "access": "Free",
            "status": "Pending"          # ← NEW
        }
    elif filter_category == "paid":
        query = {
            "access": "Paid",
            "status": "Pending"          # ← NEW
        }
    elif filter_category == "approved":
        query["status"] = "approved"
    elif filter_category == "rejected":
        query["status"] = "rejected"

    if tag_filter:
        query["approved_tags"] = tag_filter

    # 3) Pagination
    PAGE_SIZE = 20
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    skip = (page - 1) * PAGE_SIZE

    total = coll.count_documents(query)
    total_pages = math.ceil(total / PAGE_SIZE)

    docs = list(
        coll.find(query)
            .sort("publication_date", -1)
            .skip(skip)
            .limit(PAGE_SIZE)
    )

    # 4) Decorate each doc for the template
    for art in docs:
        art["pdf_name"]      = os.path.basename(art.get("pdf_file","")) \
                                  if art.get("pdf_file","").lower().endswith(".pdf") else ""
        art["suggested_tags"]= art.get("suggested_tags", [])
        art["approved_tags"] = art.get("approved_tags", [])

    # 5) Counts for nav-pills
    counts = {
        "pending": coll.count_documents({
            "$or": [
                {"status": {"$exists": False}},
                {"status": "Pending"}
            ]
        }),
        "free": coll.count_documents({"access": "Free", "status": "Pending"}),
        "paid": coll.count_documents({"access": "Paid", "status": "Pending"}),
        "approved": coll.count_documents({"status":"approved"}),
        "rejected": coll.count_documents({"status":"rejected"})
    }

    return render_template("index.html",
        articles=docs,
        filter_category=filter_category,
        tag_filter=tag_filter,
        counts=counts,
        page=page,
        total_pages=total_pages,
    )

# Serve PDFs from PDF_DIR folder explicitly
@app.route("/pdf/<path:filename>")
def serve_pdf(filename):
    return send_from_directory(PDF_DIR, filename)

# Approve/Reject now POST + htmx delete
@app.post("/approve/<article_id>")
def approve_article(article_id):
    db = get_db()
    db["articles"].update_one({"_id": ObjectId(article_id)},
                              {"$set": {"status": "approved"}})
    return ("", 200)          # ← was 204

@app.post("/reject/<article_id>")
def reject_article(article_id):
    db = get_db()
    db["articles"].update_one({"_id": ObjectId(article_id)},
                              {"$set": {"status": "rejected"}})
    return ("", 200)          # ← was 204

@app.post("/undo/<article_id>")
def undo_review(article_id):
    db = get_db()
    db["articles"].update_one({"_id": ObjectId(article_id)},
                              {"$unset": {"status": ""}})
    return ("", 200)          # ← was 204

@app.post("/move_to_folder/<article_id>")
def move_to_folder(article_id):
    db = get_db()
    article = db["articles"].find_one({"_id": ObjectId(article_id)})

    if not article:
        return "Article not found", 404

    status = article.get("status")
    pdf_filename = os.path.basename(article.get("pdf_file", ""))

    if not pdf_filename:
        return "No PDF associated with this article", 400

    src_path = os.path.join(PDF_DIR, pdf_filename)

    if status == "approved":
        dst_folder = os.path.join(PDF_DIR, "approved")
    elif status == "rejected":
        dst_folder = os.path.join(PDF_DIR, "rejected")
    else:
        return "Article must be approved or rejected before moving.", 400

    os.makedirs(dst_folder, exist_ok=True)
    dst_path = os.path.join(dst_folder, pdf_filename)

    try:
        os.rename(src_path, dst_path)
        db["articles"].update_one(
            {"_id": ObjectId(article_id)},
            {"$set": {"moved": True}}  # ✅ mark as moved
        )
        print(f"✅ Moved {pdf_filename} to {dst_folder}")
    except Exception as e:
        print(f"❌ Failed to move {pdf_filename}: {e}")
        return f"Move failed: {e}", 500

    return redirect(url_for("index"))


# Tag approval endpoints (unchanged)
@app.post("/approve_tag/<article_id>/<tag>")
def approve_tag(article_id, tag):
    db = get_db()
    db["articles"].update_one({"_id": ObjectId(article_id)},
                              {"$addToSet": {"approved_tags": tag}})
    return ("", 204)

@app.post("/add_tag/<article_id>")
def add_tag(article_id):
    tag = request.form.get("tag", "").strip()
    if tag:
        db = get_db()
        db["articles"].update_one({"_id": ObjectId(article_id)},
                                  {"$addToSet": {"approved_tags": tag}})
    return ("", 204)

if __name__ == "__main__":
    os.makedirs(PDF_DIR, exist_ok=True)
    app.run(debug=True, host="0.0.0.0", port=5000)

