from selenium import webdriver
from selenium.webdriver.chrome.options import Options

opts = Options()
opts.add_argument("--headless")
driver = webdriver.Chrome(options=opts)
driver.get("https://www.ncbi.nlm.nih.gov/")
print("Title:", driver.title)
driver.quit()
