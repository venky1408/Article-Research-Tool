import os, time, requests, shutil, io, tarfile, urllib.parse
from bs4 import BeautifulSoup
from config import PDF_DIR
from utils import sanitize_filename
import xml.etree.ElementTree as ET

# Only use Method 2: PMCID-based download from PubMed Central OA

def download_pmc_pdf(pmcid, filename=None):
    if not pmcid or not pmcid.startswith("PMC"):
        print(f"❌ Invalid PMCID: {pmcid}")
        return None

    if filename is None:
        filename = sanitize_filename(pmcid) + ".pdf"

    return download_pmc_pdf_oa(pmcid, filename)

def download_pmc_pdf_oa(pmcid, filename):
    oa_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}"
    try:
        root = ET.fromstring(requests.get(oa_url, timeout=15).text)
    except Exception as e:
        print(f"OA API failed for {pmcid}: {e}")
        return None

    link = root.find(".//record/link[@format='pdf']")
    if link is not None:
        return _stream_pmc_pdf(link.attrib["href"], filename)

    tgz = root.find(".//record/link[@format='tgz']")
    if tgz is None:
        return None

    tgz_url = tgz.attrib["href"]
    if tgz_url.startswith("ftp://"):
        tgz_url = tgz_url.replace("ftp://", "https://", 1)
    buf = io.BytesIO(requests.get(tgz_url, timeout=30).content)

    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        member = next((m for m in tar.getmembers() if m.name.lower().endswith(".pdf")), None)
        if member:
            with tar.extractfile(member) as pdf_file:
                return _save_stream(pdf_file, filename)
    return None

def _stream_pmc_pdf(url, filename):
    if url.startswith("ftp://"):
        url = url.replace("ftp://", "https://", 1)
    r = requests.get(url, stream=True, timeout=30)
    if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/pdf"):
        return _save_stream(r.raw, filename)
    return None

def _save_stream(stream, filename):
    os.makedirs(PDF_DIR, exist_ok=True)
    path = os.path.join(PDF_DIR, filename)
    with open(path, "wb") as f:
        shutil.copyfileobj(stream, f)
    print(f"Downloaded PDF to → {path}")
    return path

def attempt_pdf_download(details, new_filename=None):
    pmcid = details.get("pmcid", "")
    if not pmcid or pmcid == "No PMC ID":
        print(f"⚠️ No valid PMCID. Skipping PDF download.")
        return "Not available"

    fname = new_filename or f"{sanitize_filename(pmcid)}.pdf"
    return download_pmc_pdf(pmcid, fname) or "Not downloaded"



