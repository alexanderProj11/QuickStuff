"""
Clean SubSiteSICDesc ➜ NAICS hierarchy (2022)
Author: 2025-06-23
----------------------------------------------------------
INPUT : cleaned_unions.csv               ⟵ **CSV!**
        naics-scian-2022-structure-v1-eng.csv  (hierarchy)
OUTPUT: naics_clean.csv
        • all original cols  +
          … (same list of new columns)
----------------------------------------------------------
pip install pandas requests rapidfuzz tqdm autocorrect xlsxwriter
"""

import os, re, time, requests, pandas as pd
from rapidfuzz import process, utils
from tqdm.auto import tqdm
import unicodedata
from typing import List
import pandas as pd
from autocorrect import Speller
from thefuzz import process, fuzz
import logging
from datetime import datetime

# ------------------------------------------------------------------
# 0-bis.  LOGGING & TQDM — **drop-in replacement** -----------------
# ------------------------------------------------------------------
import logging, sys
from tqdm.auto import tqdm

LOG_LEVEL = os.getenv("NAICS_LOG", "DEBUG").upper()      # DEBUG by default

class TqdmHandler(logging.StreamHandler):
    """Send log records through tqdm.write() so they stay visible."""
    def emit(self, record):
        try:
            tqdm.write(self.format(record))
        except (KeyboardInterrupt, SystemExit):          # tqdm quirks
            raise
        except Exception:                                # never kill the run
            self.handleError(record)

logging.basicConfig(
    level   = getattr(logging, LOG_LEVEL, logging.DEBUG),
    format  = "%(asctime)s %(levelname)-7s | %(message)s",
    datefmt = "%H:%M:%S",
    handlers=[TqdmHandler(stream=sys.stderr)],
    force   = True                                       # ← crucial
)
tqdm.write(f"> Logging active at {LOG_LEVEL} level")



# ------------------------------------------------------------------
# 1. PARAMETERS  ----------------------------------------------------
# ------------------------------------------------------------------
IN_FILE          = "naics_part2.csv"
OUT_FILE         = "naics_part4.csv"
NAICS_CSV_ELEM = "naics-scian-2022-element-v1-eng.csv"
FUZZY_THRESHOLD  = 60
PAUSE_API        = 0.7          # polite delay between live queries
NAICS_API_TOKEN  = os.getenv("NAICS_API_TOKEN")   # if you have one

# optional, free fallback (sparser) – no key needed
OPEN_CORP_URL    = "https://api.opencorporates.com/v0.4/companies/search"

def load_elements(path: str) -> pd.DataFrame:
    """Return level-5 (Canadian-industry) rows from the ‘elements’ sheet."""
    df = (pd.read_csv(path, dtype=str, on_bad_lines="skip")
            .fillna("")
            .rename(columns=lambda c: c.strip()))
    df["clean_code"]        = df["Code"].str.zfill(6)
    df["clean_class_title"] = df["Class title"].str.strip().str.lower()
    df["clean_elem_desc"]   = df["Element Description"].str.strip().str.lower()
    return df

ELEM = load_elements(NAICS_CSV_ELEM)

# ------------------------------------------------------------------
# 2.  LOAD 2022 NAICS HIERARCHY  (works with the “structure” CSV) --
# ------------------------------------------------------------------
def load_structure(path: str) -> pd.DataFrame:
    """
    Return the raw Statistics-Canada structure sheet *exactly as published*
    with a few convenience columns added:

        • clean_code  … zero-padded string, always 6-chars
        • level_name  … one of  {'Sector','Subsector','Industry group',
                                 'Industry','Canadian industry'}
    """
    df = (pd.read_csv(path, dtype=str)
            .fillna("")
            .rename(columns=lambda c: c.strip()))          # keep official labels

    # a) zero-pad codes – lets us slice reliably later
    df["clean_code"] = df["Code"].str.zfill(6)

    # b) normalise level names so we can map lengths to the five buckets
    L2LEVEL = {2: "Sector", 3: "Subsector", 4: "Industry group",
               5: "Industry", 6: "Canadian industry"}
    df["level_name"] = df["clean_code"].str.rstrip("0").str.len().map(L2LEVEL)

    return df.set_index("clean_code")    # 6-digit *string* is now the index


# ------------------------------------------------------------------
# 3.  LOOK-UP TABLES  ---------------------------------------------
# ------------------------------------------------------------------
elem_df = (
    pd.read_csv(NAICS_CSV_ELEM, dtype=str)
      .fillna("")            
      .rename(columns=lambda c: c.strip())
)

PHRASE_TO_CODE   = {}
PHRASE_TO_TITLE  = {}

for _, rec in ELEM.iterrows():
    code   = rec["clean_code"]
    title  = rec["Class title"].strip()
    # 1️⃣ exact class title
    key = rec["clean_class_title"]
    PHRASE_TO_CODE[key]  = code
    PHRASE_TO_TITLE[key] = title
    # 2️⃣ every element description
    for phrase in rec["clean_elem_desc"].split(";"):     # handle semi-colon lists
        p = phrase.strip()
        if p:
            PHRASE_TO_CODE[p]  = code
            PHRASE_TO_TITLE[p] = title

NAICS_CHOICES = list(PHRASE_TO_CODE.keys())   


# ------------------------------------------------------------------
# 3. FUZZY DESCRIPTION ▸ NAICS CODE  -------------------------------
# ------------------------------------------------------------------

spell = Speller(lang="en")

# ----------------------------------------
# keyword / synonym normaliser -------------------------
#
#  – use lower-case keys!
#  – include your domain-specific typos & abbreviations here
#
SUBSTITUTIONS = {
    # obvious typos
    "agirculture": "agriculture",
    "abbatoirs":   "abattoirs",

    # recurring boiler-plate you want to strip out completely
    "per wcb":     "",            # catches “… per WCB …”
    "ne, nec":     "",            # misc. abbreviations that do not help classification
}

# one-shot replacements compiled just once
SUB_PATTERNS = [(re.compile(fr"\b{k}\b", flags=re.I), v) for k, v in SUBSTITUTIONS.items()]

# ------------- MAIN ROUTINE ---------------------------------
def clean_text(raw: str) -> str:
    """
    Normalise 'SubSiteSICDesc' style strings.

    – removes WCB comments, dates, hash tags, embedded SIC codes, etc.
    – collapses whitespace & punctuation
    – fixes common misspellings with autocorrect
    – returns *title-cased* string ready for lookup / vectorisation
    """
    if pd.isna(raw) or not str(raw).strip():
        return ""

    txt = str(raw)

    # 1. Unicode normalisation (accents → ASCII, weird dashes → '-')  :contentReference[oaicite:0]{index=0}
    txt = unicodedata.normalize("NFKD", txt)
    txt = txt.encode("ascii", errors="ignore").decode()

    # 2. Kill all line-breaks & tab characters  :contentReference[oaicite:1]{index=1}
    txt = txt.replace("\r", " ").replace("\n", " ")

    # 3. Strip parenthetical or slash/dash comments such as
    #    “/ 642 per WCB …” or “#108 per WCB June 20/11”  :contentReference[oaicite:2]{index=2}
    txt = re.sub(r"[#/]\s*\d{1,6}.*", " ", txt)

    # 4. Remove *leading* or *stand-alone* 3-plus-digit codes
    txt = re.sub(r"^[\s\-]*(\d{3,6})(?=\D|$)", " ", txt)         # leading SIC code
    txt = re.sub(r"\b\d{3,6}\b", " ", txt)                       # stand-alone codes

    # 5. Standardise special characters we want to keep (&, /, -)
    txt = re.sub(r"[,:;()\[\]]", " ", txt)                       # kill noisy punctuation
    txt = re.sub(r"[./]{2,}", "/", txt)                          # collapse multiple '/'
    txt = re.sub(r"[-]{2,}", "-", txt)                           # collapse multiple '-'

    # 6. Apply one-off substitutions / typo fixes
    for pat, repl in SUB_PATTERNS:
        txt = pat.sub(repl, txt)

    # 7. Collapse repeated whitespace, then lowercase for autocorrect
    txt = re.sub(r"\s+", " ", txt).strip().lower()

    # 8. Auto-correct frequent spelling errors  📑 :contentReference[oaicite:3]{index=3}
    txt = spell(txt)

    # 9. Title-case so every further comparison is deterministic  📑 :contentReference[oaicite:4]{index=4}
    txt = txt.title()

    # 10. Final sanity pass: if all that’s left is empty → return ''
    return txt if txt.strip(".- ") else ""

def fuzzy_naics(raw_text: str,
                choices,
                code_lookup,
                title_lookup,
                cutoff: int = 70):
    """
    Return (code, canonical_title, score) using both class-titles and element
    descriptions. If nothing beats `cutoff`, return (None, None, 0).
    """
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None, None, 0

    hit = process.extractOne(raw_text,
                             choices,
                             scorer=fuzz.token_set_ratio,
                             score_cutoff=cutoff)
    if hit is None:
        return None, None, 0

    phrase, score = hit[0], hit[1]
    code  = code_lookup[phrase]
    title = title_lookup[phrase]   # canonical “Class title”
    return code, title, score
# ------------------------------------------------------------------
# 4. OPTIONAL ONLINE LOOK-UP ---------------------------------------
# ------------------------------------------------------------------

# ────────────────────────────────────────────────────────────────────────────────
# 5. READ SOURCE & PROCESS ROWS  (rewritten, no web-look-ups)  – PATCHED SECTION
# ────────────────────────────────────────────────────────────────────────────────
df = pd.read_csv(IN_FILE, dtype=str, low_memory=False)

def is_blank(val: str) -> bool:
    """
    True  → value should be treated as 'empty / missing'
    False → value contains something useful
    """
    return (
        pd.isna(val) or                    # real NaN/None              ✔ pandas test
        str(val).strip().lower() in {"",   # empty -- after strip
                                     "nan",
                                     "none"}                            # str(NaN) fix
    )

def first_non_blank(row, *cols) -> str:
    """Return the first non-empty cell among the given columns, else ''. """
    for col in cols:
        val = row.get(col, "")
        if not is_blank(val):
            return str(val)
    return ""

df = pd.read_csv(IN_FILE, dtype=str, low_memory=False)

naics_e_code_col, naics_e_title_col = [], []

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Rows"):

    # ----- rule 5 : skip if NAICS_Code already holds a good value ---
    existing_code  = str(row.get("NAICS_Code", ""))
    existing_title = row.get("NAICS_Desc", "")
    if existing_code.isdigit():
        skip_fuzzy = True
        logging.debug(f"[{idx}] NAICS_Code already set → {existing_code}")
    else:
        skip_fuzzy = False
        existing_code  = ""
        existing_title = ""

    # ----- decide which source text to fuzzy-match -----------------
    raw_src = first_non_blank(row,
                              "Imported_SiteDescription",
                              "SubSiteSICDesc",
                              "IndustryType")

            
    if skip_fuzzy and is_blank(raw_src):
        best_code, best_title, best_score = None, None, 0
        logging.debug(f"[{idx}] no candidate text → skip fuzzy")
        
    else:
        desc_cln = clean_text(raw_src)
        best_code, best_title, best_score = fuzzy_naics(
            desc_cln,
            choices      = NAICS_CHOICES,
            code_lookup  = PHRASE_TO_CODE,
            title_lookup = PHRASE_TO_TITLE,
            cutoff       = FUZZY_THRESHOLD
        )
        if best_code:
            logging.info(f"[{idx}] ✔ fuzzy hit {best_code} ({best_score}) "
                         f"{best_title!r}")
        else:
            logging.debug(f"[{idx}] ✘ no fuzzy match for {desc_cln!r}")

    # ----- choose between previous NAICS_Desc vs new fuzzy result --
    chosen_code, chosen_title = "", ""
    if best_code and existing_title:
        # compare which description matches the raw text better
        old_score = fuzz.token_set_ratio(clean_text(existing_title),
                                         clean_text(raw_src))
        if old_score >= best_score:           # keep previous
            chosen_code, chosen_title = existing_code, existing_title
            logging.debug(f"[{idx}] previous NAICS_Desc wins ({old_score} ≥ "
                          f"{best_score})")
        else:                                 # take fuzzy result
            chosen_code, chosen_title = best_code, best_title
            logging.debug(f"[{idx}] fuzzy result wins ({best_score} > "
                          f"{old_score})")
    elif best_code:           # only fuzzy hit exists
        chosen_code, chosen_title = best_code, best_title
    elif existing_code:       # only previous exists
        chosen_code, chosen_title = existing_code, existing_title
    # else both blank → stay empty

    naics_e_code_col.append(chosen_code)
    naics_e_title_col.append(chosen_title)

# ------------------------------------------------------------------
# APPEND NEW COLUMNS & WRITE CSVs ----------------------------------
# ------------------------------------------------------------------
df["NAICS_e_code"]  = naics_e_code_col
df["NAICS_e_title"] = naics_e_title_col

df.to_csv(OUT_FILE, index=False)
pd.DataFrame({"NAICS_Code_Choices": sorted(set(naics_e_code_col))})\
  .to_csv("naics_code_choices.csv", index=False)
pd.DataFrame({"NAICS_Title_Choices": sorted(set(naics_e_title_col))})\
  .to_csv("naics_title_choices.csv", index=False)

print("✓ CSVs written:")
print(f"   • {OUT_FILE}")
print("   • naics_code_choices.csv")
print("   • naics_title_choices.csv")