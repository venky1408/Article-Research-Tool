import PyPDF2
from config import MAX_PDF_PAGES

def extract_pdf_text(file_path):
    """
    Return text from the first MAX_PDF_PAGES pages of the PDF.
    """
    parts = []
    try:
        with open(file_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            pages = min(len(reader.pages), MAX_PDF_PAGES)
            for i in range(pages):
                parts.append(reader.pages[i].extract_text() or "")
    except Exception as e:
        print(f"⚠️ Could not read {file_path}: {e}")
    return "\n".join(parts).strip()
