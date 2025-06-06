import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# MongoDB connection string
MONGO_URI = os.getenv("MONGO_URI")

# PubMed API key
NCBI_API_KEY = os.getenv("NCBI_API_KEY")

# Project paths
PDF_DIR = os.getenv("PDF_DIR", "pdfs")
CITATION_DIR = os.getenv("CITATION_DIR", "citations")
KEYWORDS_CSV = os.getenv("KEYWORDS_CSV", "input data/keywords.csv")
ABBREVS_CSV = os.getenv("ABBREVS_CSV", "input data/abbreviations.csv")

# Other configs
MAX_PDF_PAGES = int(os.getenv("MAX_PDF_PAGES", 2))
