import requests, os, shutil, urllib.parse, io

def save_pmc_pdf(url, out_name, folder=r"C:\Users\saisr\OneDrive\Desktop\Capstone\pdfs"):
    """
    Download a PMC PDF even if the OA feed gives an ftp:// link.
    Returns the saved path or None on failure.
    """
    # 1️⃣ convert ftp:// → https://
    parts = urllib.parse.urlparse(url)
    if parts.scheme == "ftp":
        url = urllib.parse.urlunparse(("https",) + parts[1:])

    # 2️⃣ stream download
    r = requests.get(url, stream=True, timeout=30)
    if r.status_code != 200 or not r.headers.get("content-type","").startswith("application/pdf"):
        print("Download failed or not a PDF:", url)
        return None

    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, out_name)
    with open(path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    print("Saved →", path)
    return path


# ---------- quick test for PMC11947954 ----------
pmcid = "PMC11947954"
ftp_link = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_pdf/c4/70/101709.PMC11947954.pdf"
save_pmc_pdf(ftp_link, f"{pmcid}.pdf")
