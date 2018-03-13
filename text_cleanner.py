
import re
import string
import unicodedata
from six import u
from stopwords import get_stopwords_by_language


# Taken from gensim
def to_string(text, encoding='utf8', errors='strict'):
    """Convert a string (bytestring in `encoding` or unicode), to unicode."""
    if isinstance(text, str):
        return text
    return str(text, encoding, errors=errors)


# Taken from gensim
RE_PUNCT = re.compile('([%s])+' % re.escape(string.punctuation), re.UNICODE)
def strip_punctuation(s):
    s = to_string(s)
    return RE_PUNCT.sub(" ", s)


# Taken from gensim
RE_NUMERIC = re.compile(r"[0-9]+", re.UNICODE)
def strip_numeric(s):
    s = to_string(s)
    return RE_NUMERIC.sub("", s)


RE_LAUGH = re.compile(r"\b(?:a*(?:ha)+h?|(?:a*(?:ja)+j?))\b")
def strip_laugh(s):
    s = to_string(s)
    return RE_LAUGH.sub("", s)


def remove_stopwords(sentence, language):
    stopwords = get_stopwords_by_language(language)
    return " ".join(w for w in sentence.split() if w not in stopwords)



# Taken from gensim
def deaccent(text):
    """
    Remove accentuation from the given string. Input text is either a unicode string or utf8
    encoded bytestring.
    """
    if not isinstance(text, str):
        # assume utf8 for byte strings, use default (strict) error handling
        text = text.decode('utf8')
    norm = unicodedata.normalize("NFD", text)
    result = u('').join(ch for ch in norm if unicodedata.category(ch) != 'Mn')
    return unicodedata.normalize("NFC", result)


def clean_text(s, language):
    functions = [lambda w: w.lower(), deaccent, strip_punctuation, strip_numeric, strip_laugh]
    for f in functions: s = f(s)
    return remove_stopwords(s, language)