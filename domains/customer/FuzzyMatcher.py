from rapidfuzz import fuzz
from rapidfuzz import process


def name_matcher(row, choices, threshold=80):
    best_match, score, _ = process.extractOne(row, choices)
    if score >= threshold:
        return best_match
    else:
        return None