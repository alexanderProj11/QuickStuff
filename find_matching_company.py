import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
import re

# ── 1. Load data ─────────────────────────────────────────────────────
companies = pd.read_csv("Companies.csv")
tracker   = pd.read_csv("FullReprisalTracker.csv")

import re, unicodedata, pandas as pd

# ── 2. Mega-normalise helper ──────────────────────────────────────────
CORP_SUFFIX = [
    r"ltd\.?", r"limited",
    r"inc\.?", r"incorporated",
    r"corp\.?", r"corporation",
    r"plc", r"public limited company",
    r"gmbh", r"s\.?p\.?a\.?", r"s\.?a\.?", r"s\.?r\.?l\.?",
    r"oy", r"as", r"ab", r"ag",
    r"pte\.?", r"pty\.?\s*ltd\.?",
    r"bhd\.?", r"sdn\s*bhd\.?",
    r"llc", r"l\.?l\.?p\.?", r"l\.?p\.?",
    r"co\.?", r"company",
    r"bv", r"nv", r"kg", r"kft", r"spa", r"sae",
]

NOISE_WORDS = {
    "group", "holdings", "holding", "global", "international",
    "services", "service", "solutions", "systems", "system",
    "industries", "industry", "enterprise", "enterprises",
    "société", "societe", "anonima", "anonime", "anonim",  # French / IT variants
}

# pre-compile big regex chunks
_re_suffix  = re.compile(r"\b(" + r"|".join(CORP_SUFFIX) + r")\b", re.I)
_re_amp     = re.compile(r"&")
_re_non_al  = re.compile(r"[^a-z0-9\s]")
_re_ws      = re.compile(r"\s+")

def normalise(text: str) -> str:
    """
    Clean a company name for fuzzy matching:
      • lower-case, ASCII-fold accents
      • expand &, drop corporate suffixes, punctuation
      • drop noise words (group, holdings…)
    """
    if pd.isna(text) or not str(text).strip():
        return ""

    # ASCII-fold accents   e.g. 'Société' -> 'Societe'
    text = unicodedata.normalize("NFKD", str(text))
    text = text.encode("ascii", "ignore").decode()

    text = text.lower()

    # &  -> ' and '
    text = _re_amp.sub(" and ", text)

    # remove corporate endings
    text = _re_suffix.sub(" ", text)

    # kill punctuation
    text = _re_non_al.sub(" ", text)

    # collapse & strip
    tokens = [t for t in _re_ws.split(text) if t and t not in NOISE_WORDS]
    return " ".join(tokens)

# ── 3. Create normalised columns once ────────────────────────────────
for col in ["CompNameCommon", "CompNameLegal", "CompNameAlias"]:
    companies[f"norm_{col}"] = companies[col].apply(normalise)

tracker["norm_query"] = tracker["Company"].apply(normalise)

# ── 4. Pre-compute choices arrays (one per name type) ────────────────
choices_common = companies["norm_CompNameCommon"].tolist()
choices_legal  = companies["norm_CompNameLegal"].tolist()
choices_alias  = companies["norm_CompNameAlias"].tolist()

# ── 5. Pre-allocate tracker output columns ───────────────────────────
for n in range(1, 6):
    suff = "" if n == 1 else str(n)
    for base in [
        "MatchedCompNameCommon", "MatchedCompNameLegal", "MatchedCompNameAlias",
        "MatchedAddress", "MatchedPostalCode", "MatchedCity",
        "MatchedProvince", "MatchedContact"
    ]:
        tracker[f"{base}{suff}"] = ""

TOP_K   = 5
THRESH  = 80       # min score to keep

# ── 6. Vectorised scorer ------------------------------------------------
def top5(row_norm: str):
    """Return list[(company_idx, score)] sorted by score desc."""
    if not row_norm:
        return []

    # compute three score vectors in C
    s_common = process.cdist([row_norm], choices_common, scorer=fuzz.token_set_ratio)[0]
    s_legal  = process.cdist([row_norm], choices_legal,  scorer=fuzz.token_set_ratio)[0]
    s_alias  = process.cdist([row_norm], choices_alias,  scorer=fuzz.token_set_ratio)[0]

    best_scores = np.maximum.reduce([s_common, s_legal, s_alias])

    # take top-k indices
    idx = best_scores.argsort()[-TOP_K:][::-1]
    return [(i, best_scores[i]) for i in idx if best_scores[i] >= THRESH]

# ── 7. Fill the tracker row-by-row -------------------------------------
for ridx, qnorm in tracker["norm_query"].items():
    for rank, (cidx, score) in enumerate(top5(qnorm), start=1):
        suff   = "" if rank == 1 else str(rank)
        source = companies.loc[cidx]

        # names
        tracker.at[ridx, f"MatchedCompNameCommon{suff}"] = source["CompNameCommon"]
        tracker.at[ridx, f"MatchedCompNameLegal{suff}"]  = source["CompNameLegal"]
        tracker.at[ridx, f"MatchedCompNameAlias{suff}"]  = source["CompNameAlias"]

        # address / contact
        for col in ["Address", "PostalCode", "City", "Province", "Contact"]:
            tracker.at[ridx, f"Matched{col}{suff}"] = source[col]

# ── 8. Save ────────────────────────────────────────────────────────────
tracker.drop(columns=["norm_query"], inplace=True)
tracker.to_csv("FullReprisalTracker_updated.csv", index=False)
print("👍  Tracker updated with fuzzy matches")
