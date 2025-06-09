# PubMed Article Scraper & Analyzer

This project automates the process of searching PubMed articles using keywords, downloading open-access PDFs, extracting text, storing data in MongoDB, and presenting results in a web UI. For more information please have a look at the "Scientific_Article_Scraper_Documentation_Complete" document.

---

## 🛠 Requirements

- Python 3.8+
- MongoDB installed and running locally
- Chrome (for manual verification of PDFs if needed)

---

## 📦 Setup Instructions

1. **Clone or unzip the project**
2. **Create a virtual environment** (recommended)

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Ensure MongoDB is running locally**  
   You can start it with:
```bash
mongod
```

5. **Run the main script**

```bash
python SciCom.py
```

This will start the scraping and processing pipeline.

---

## 📁 Folder Structure

```
Capstone/
├── data/
│   ├── abbreviations.csv
│   └── keywords.csv
├── pdfs/
├── citations/
├── SciCom.py
├── config.py
├── db_utils.py
├── pubmed_utils.py
├── requirements.txt
```

---

## ✅ Output

- Extracted PDFs saved in `pdfs/`
- Metadata and article text stored in MongoDB
- Citations saved in the `citations/` folder

---

For any issues, make sure your MongoDB is active and the Python version matches.

User Interface : - 
![image](https://github.com/user-attachments/assets/cf03a9cd-7054-4b31-b60b-3aa440bc4780)
![image](https://github.com/user-attachments/assets/8d0eccb8-72b8-48e8-bfda-445806e131ae)



