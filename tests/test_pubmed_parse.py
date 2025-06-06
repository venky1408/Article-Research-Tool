from pubmed_utils import parse_pub_date
from bs4 import BeautifulSoup

def test_parse_pub_date_full():
    xml = "<PubDate><Year>2023</Year><Month>Aug</Month><Day>10</Day></PubDate>"
    tag = BeautifulSoup(xml, "xml").PubDate
    assert parse_pub_date(tag) == "2023-08-10"

def test_parse_pub_date_partial():
    xml = "<PubDate><Year>2019</Year><Month>Jul</Month></PubDate>"
    tag = BeautifulSoup(xml, "xml").PubDate
    # Accepts "2019-07-01" or "2019-07" depending on your function
    assert "2019" in parse_pub_date(tag)
