from flask import Flask, render_template, redirect, url_for, request, send_from_directory
from pymongo import MongoClient
from bson.objectid import ObjectId
import os

# Set the static folder to your desired PDF directory
app = Flask(__name__, static_folder=r"C:\Users\saisr\OneDrive\Desktop\Capstone\pdfs")

def get_db():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["research_papers"]
    return db

@app.route("/")
def index():
    db = get_db()
    coll = db["articles"]

    filter_category = request.args.get("filter", "all")
    query_filter = {}
    if filter_category == "free":
        query_filter = {"access": "Free"}
    elif filter_category == "paid":
        query_filter = {"access": "Paid"}
    elif filter_category == "approved":
        query_filter = {"status": "approved"}
    elif filter_category == "rejected":
        query_filter = {"status": "rejected"}

    articles = list(coll.find(query_filter).sort("publication_date", -1))
    
    count_all = coll.count_documents({})
    count_free = coll.count_documents({"access": "Free"})
    count_paid = coll.count_documents({"access": "Paid"})
    count_approved = coll.count_documents({"status": "approved"})
    count_rejected = coll.count_documents({"status": "rejected"})

    return render_template("index.html",
                           articles=articles,
                           filter_category=filter_category,
                           count_all=count_all,
                           count_free=count_free,
                           count_paid=count_paid,
                           count_approved=count_approved,
                           count_rejected=count_rejected)

@app.route("/pdf/<path:filename>")
def serve_pdf(filename):
    return send_from_directory(app.static_folder, filename)

@app.route("/approve/<article_id>")
def approve_article(article_id):
    db = get_db()
    coll = db["articles"]
    coll.update_one({"_id": ObjectId(article_id)}, {"$set": {"status": "approved"}})
    return redirect(url_for("index"))

@app.route("/reject/<article_id>")
def reject_article(article_id):
    db = get_db()
    coll = db["articles"]
    coll.update_one({"_id": ObjectId(article_id)}, {"$set": {"status": "rejected"}})
    return redirect(url_for("index"))

@app.route("/refresh")
def refresh():
    return redirect(url_for("index"))

if __name__ == "__main__":
    os.makedirs(app.static_folder, exist_ok=True)
    app.run(debug=True, host="0.0.0.0", port=5000)