import requests
r = requests.get("https://ftp.ncbi.nlm.nih.gov/pub/pmc/articles/PMC12013706/pdf/PMC12013706.pdf", timeout=10)
print(r.status_code, r.headers.get("Content-Type"))
