from utils import sanitize_filename

def test_sanitize_filename_basic():
    assert sanitize_filename("Hello  World!!.pdf") == "Hello Worldpdf"

def test_sanitize_filename_whitespace():
    assert sanitize_filename("  spaced   out  ") == "spaced out"
