import pandas as pd
from rapidfuzz import process, fuzz  # ultra-fast Levenshtein implementations
import re

# --- load the two CSVs ---
companies = pd.read_csv("Companies.csv")           # master list
tracker   = pd.read_csv("FullReprisalTracker.csv")   # needs Matched columns

# --- simple synonym / abbreviation map ---
ABBREV = {
    r"\bltd\b":      "limited",
    r"\bl\.?p\.?\b": "limited partnership",
    r"\bl\.?l\.?c\.?\b": "llc",
    r"\bco\b":       "company",
    "&":             " and ",
}

def normalise(txt: str) -> str:
    """lower, strip, remove punctuation, expand synonyms"""
    if pd.isna(txt):
        return ""
    txt = txt.lower()
    for pat, repl in ABBREV.items():
        txt = re.sub(pat, repl, txt)
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)     # keep alnum & spaces only
    return re.sub(r"\s+", " ", txt).strip()    # collapse spaces

# --- create helper columns ONCE ---
companies["norm"] = companies["CompNameCommon"].apply(normalise)
tracker["norm"]   = tracker["Company"].apply(normalise)  # whatever field holds the parsed name

choices = companies["norm"].tolist()                # list of cleaned names

TOP_K   = 5
THRESH  = 80        # keep anything scoring ≥ 80/100

def top_matches(query, k=5, threshold=80):
    if not query:
        return []
    
    # returns a numpy array of scores (float32) in the same order as choices
    scores = process.cdist([query],
                           choices,
                           scorer=fuzz.token_set_ratio)[0]

    # indices of the k highest scores above threshold (descending)
    idx = scores.argsort()[-k:][::-1]
    return [(i, scores[i]) for i in idx if scores[i] >= threshold]

# pre-allocate blank columns
for n in range(1, 6):
    for col in ["MatchedCompNameCommon", "MatchedAddress",
                "MatchedPostalCode", "MatchedCity",
                "MatchedProvince", "MatchedContact"]:
        tracker[f"{col}{'' if n==1 else n}"] = ""

# fill row-by-row
for ridx, row in tracker.iterrows():
    hits = top_matches(row["norm"])
    for rank, (cidx, score) in enumerate(hits, start=1):
        prefix = "" if rank == 1 else str(rank)
        src    = companies.loc[cidx]
        tracker.at[ridx, f"MatchedCompNameCommon{prefix}"] = src["CompNameCommon"]
        tracker.at[ridx, f"MatchedAddress{prefix}"]        = src["Address"]
        tracker.at[ridx, f"MatchedPostalCode{prefix}"]     = src["PostalCode"]
        tracker.at[ridx, f"MatchedCity{prefix}"]           = src["City"]
        tracker.at[ridx, f"MatchedProvince{prefix}"]       = src["Province"]
        tracker.at[ridx, f"MatchedContact{prefix}"]        = src["Contact"]

tracker.drop(columns=["norm"], inplace=True)
tracker.to_csv("FullReprisalTracker_updated.csv", index=False)
