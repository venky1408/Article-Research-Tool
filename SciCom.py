"""
SciCom.py  ·  Main controller for the PubMed-to-Mongo pipeline
---------------------------------------------------------------------------
✓ Reads keywords & abbreviations from CSV (keywords.csv, abbreviations.csv)
✓ MongoDB workflow is untouched (articles, article_text, run_logs)
✓ NEW: guarantees pdfs/ exists, exports 16-column CSV, and writes
      citation TXT files in ≤50-line chunks for the entire run
"""

from config import (
    MONGO_URI,
    PDF_DIR,
    CITATION_DIR,
    KEYWORDS_CSV,
    ABBREVS_CSV,
)

from db_utils import connect_to_mongo, init_db, get_last_successful_run_date
from pubmed_utils import search_pubmed_date_range, fetch_pubmed_details
from pdf_utils import attempt_pdf_download
from abbrev_utils import compute_updated_title
from utils import (
    sanitize_filename,
    generate_citation,
    load_keywords_from_csv,
    load_abbreviation_map,
)
from pdf_text_utils import extract_pdf_text
from tag_utils import suggest_tags

import os
from datetime import datetime, timedelta
import concurrent.futures
import pandas as pd


# ----------------------------------------------------------------------
# Top-level extraction routine
# ----------------------------------------------------------------------
def run_extraction(use_parallel: bool = False) -> None:
    """Main runner; see module docstring."""
    db = init_db()

    # guarantee the PDF folder exists  (Update 1)
    os.makedirs(PDF_DIR, exist_ok=True)

    # ---------- inputs from CSV ----------
    keywords = load_keywords_from_csv()
    if not keywords:
        print("❌  No keywords found in keywords.csv. Exiting.")
        return
    abbr_map = load_abbreviation_map()

    # ---------- run-log (unchanged) ----------
    start_time = datetime.now()
    run_log_id = db["run_logs"].insert_one({
        "start_time": start_time,
        "status": "started",
        "keywords": keywords,
        "articles_processed": 0,
        "errors": []
    }).inserted_id
    print(f"🔷  Run started at {start_time:%Y-%m-%d %H:%M:%S}")

    total_articles_processed = 0
    paid_citations, paid_seen = [], set()
    export_rows = []                                    # Update 2

    try:
        last_successful = get_last_successful_run_date(db)
        start_date = last_successful if last_successful else datetime.now() - timedelta(days=30)
        end_date   = datetime.now()

        # ---------- keyword loop ----------
        for kw in keywords:
            print(f"\n🔍  Keyword: {kw}")
            ids = search_pubmed_date_range(kw, start_date, end_date)
            if not ids:
                print("   (no new articles)")
                continue
            print(f"➡️  Found {len(ids)} articles")

            if use_parallel:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
                    futures = [
                        ex.submit(process_pubmed_id, pid, db, abbr_map, run_log_id)
                        for pid in ids
                    ]
                    for fut in concurrent.futures.as_completed(futures):
                        res = fut.result()
                        if res and not res.get("skipped"):
                            export_rows.append(res)      # Update 2
                            total_articles_processed += 1
                            _maybe_collect_paid(res, paid_seen, paid_citations, db)
            else:
                for pid in ids:
                    res = process_pubmed_id(pid, db, abbr_map, run_log_id)
                    if res and not res.get("skipped"):
                        export_rows.append(res)          # Update 2
                        total_articles_processed += 1
                        _maybe_collect_paid(res, paid_seen, paid_citations, db)

        # ---------- single write of all citations (Update 4) ----------
        _write_all_citations(paid_citations)

        # ---------- CSV export (Update 5) ----------
        _export_to_csv(export_rows)

        # ---------- mark run complete ----------
        end_time = datetime.now()
        db["run_logs"].update_one(
            {"_id": run_log_id},
            {"$set": {
                "end_time": end_time,
                "status": "completed",
                "articles_processed": total_articles_processed
            }}
        )
        print(f"\n✅  Run finished at {end_time:%Y-%m-%d %H:%M:%S}  "
              f"({total_articles_processed} articles processed)")

    except Exception as e:
        _handle_run_error(e, db, run_log_id)


# ----------------------------------------------------------------------
# Process one PubMed ID  (unchanged logic + Update 3)
# ----------------------------------------------------------------------
def process_pubmed_id(pid: str, db, abbr_map: dict, run_log_id):
    details = fetch_pubmed_details(pid)
    if "error" in details:
        db["run_logs"].update_one(
            {"_id": run_log_id},
            {"$push": {"errors": {
                "pubmed_id": pid,
                "error": details["error"],
                "timestamp": datetime.now()
            }}}
        )
        return {"pubmed_id": pid, "skipped": True}

    existing = db["articles"].find_one({"pubmed_id": pid})
    details["updated_title"] = existing["updated_title"] if existing \
        else compute_updated_title(details, abbr_map)

    # ------- PDF handling -------
    pdf_name = sanitize_filename(details["updated_title"]) + ".pdf"
    # --- truncate if Windows path would be too long (≤140 chars keeps path <260) ---
    if len(pdf_name) > 140:                             #   ← NEW
        pdf_name = pdf_name[:140] + ".pdf"              #   ← NEW
    local_pdf = os.path.join(PDF_DIR, pdf_name)
    pdf_file  = local_pdf if os.path.exists(local_pdf) \
               else attempt_pdf_download(details, new_filename=pdf_name)
    full_text = extract_pdf_text(pdf_file) if pdf_file else ""

    db["article_text"].update_one(
        {"pubmed_id": pid},
        {"$set": {"full_text": full_text}},
        upsert=True
    )

    # ------- tagging & enrichment -------
    tag_source = " ".join([details.get("title",""), details.get("abstract",""), full_text])
    details["suggested_tags"]  = suggest_tags(tag_source, top_k=10)
    details.setdefault("status", "Pending")          # keep existing status if any
    # (Update 3: do NOT overwrite details["keywords"]; assume PubMed already set it)
    details["pdf_file"]        = pdf_file or None
    details["webscraped_date"] = datetime.now().strftime("%Y-%m-%d")

    if existing:
        db["articles"].update_one({"_id": existing["_id"]}, {"$set": details})
    else:
        db["articles"].insert_one(details)

    return details                                   # Update 2: return full dict


# ----------------------------------------------------------------------
# Helper utilities
# ----------------------------------------------------------------------
def _maybe_collect_paid(res, paid_seen, paid_citations, db):
    if res.get("access") != "Paid":
        return
    pid = res["pubmed_id"]
    if pid in paid_seen:
        return
    doc = db["articles"].find_one({"pubmed_id": pid})
    if doc:
        paid_citations.append(generate_citation(doc))
    paid_seen.add(pid)


# ---------- new global citation writer (Update 4) ----------
def _write_all_citations(citations: list[str]):
    if not citations:
        return
    os.makedirs(CITATION_DIR, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    for i in range(0, len(citations), 50):
        chunk  = citations[i:i+50]
        fname  = f"citations_{stamp}_{i//50+1}.txt"
        fpath  = os.path.join(CITATION_DIR, fname)
        # never overwrite
        counter = 1
        while os.path.exists(fpath):
            fpath = os.path.join(CITATION_DIR, f"citations_{stamp}_{i//50+1}_{counter}.txt")
            counter += 1
        with open(fpath, "w", encoding="utf-8") as f:
            f.write("\n\n".join(chunk))
        print(f"📑  Saved {len(chunk)} citations → {fpath}")


# ---------- CSV exporter (Update 5) ----------
def _export_to_csv(rows: list[dict]):
    if not rows:
        print("\nⓘ  Nothing new to export this run.")
        return

    fields = [
        "pubmed_id","title","abstract","authors","journal","publication_date",
        "doi","fulltext_link","pmcid","access","updated_title","pdf_file",
        "suggested_tags","webscraped_date","status","keywords"
    ]
    os.makedirs("exports", exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path  = f"exports/scraped_articles_{stamp}.csv"

    (pd.DataFrame(rows)
        .reindex(columns=fields)
        .fillna("")
        .to_csv(path, index=False))

    print(f"\n📄  Exported {len(rows)} rows → {path}")


def _handle_run_error(exc, db, run_log_id):
    end_time = datetime.now()
    db["run_logs"].update_one(
        {"_id": run_log_id},
        {"$set": {
            "end_time": end_time,
            "status": "error",
            "error": str(exc)
        }}
    )
    print(f"\n❌  Run aborted at {end_time:%Y-%m-%d %H:%M:%S}\n   → {exc}")


# ----------------------------------------------------------------------
# CLI entry-point
# ----------------------------------------------------------------------
if __name__ == "__main__":
    run_extraction(use_parallel=False)   # set True for ThreadPool if desired
