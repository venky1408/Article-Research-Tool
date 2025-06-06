import yake

# Configure once; tweak top_k or language as needed
_LANG = "en"
_MAX_NGRAM = 3
_DEDUPE_THRESHOLD = 0.9
_DEDUPE_ALGO = "seqm"
_WINDOW_SIZE = 1

_extractor = yake.KeywordExtractor(
    lan=_LANG,
    n=_MAX_NGRAM,
    dedupLim=_DEDUPE_THRESHOLD,
    dedupFunc=_DEDUPE_ALGO,
    windowsSize=_WINDOW_SIZE,
    top=20         # always pull up to 20; we'll trim later
)

def suggest_tags(text: str, top_k: int = 10) -> list[str]:
    """
    Return a list of up to `top_k` keyword strings suggested by YAKE,
    sorted by importance (highest first).
    """
    if not text:
        return []
    keywords = _extractor.extract_keywords(text)
    # keywords is a list of (phrase, score); lower score = more relevant
    top_kw = sorted(keywords, key=lambda x: x[1])[:top_k]
    return [phrase for phrase, score in top_kw]
