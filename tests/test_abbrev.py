from abbrev_utils import replace_with_abbreviations

def test_abbrev_replacement():
    abbr_map = {"transcatheter aortic valve replacement": "TAVR"}
    text = "Outcomes after Transcatheter Aortic Valve Replacement procedures"
    result = replace_with_abbreviations(text, abbr_map)
    assert "TAVR" in result and "Transcatheter" not in result
