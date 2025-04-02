from rapidfuzz import fuzz
from rapidfuzz import process

def name_matcher(name1, name2):
    score = fuzz.ratio(name1, name2)
    return score > 75

