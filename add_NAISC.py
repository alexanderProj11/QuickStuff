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
# 0.  LOGGING CONFIG  (before parameter section is fine) -----------
# ------------------------------------------------------------------
LOG_LEVEL = os.getenv("NAICS_LOG", "INFO").upper()   # e.g. export NAICS_LOG=DEBUG
logging.basicConfig(
    level   = LOG_LEVEL,
    format  = "%(asctime)s  %(levelname)-7s | %(message)s",
    datefmt = "%H:%M:%S"
)
tqdm.write(f"> Logging set to {LOG_LEVEL}")



# ------------------------------------------------------------------
# 1. PARAMETERS  ----------------------------------------------------
# ------------------------------------------------------------------
IN_FILE          = "cleaned_unions.csv"
OUT_FILE         = "naics_clean.csv"
NAICS_CSV_STRUCT = "naics-scian-2022-structure-v1-eng.csv"
FUZZY_THRESHOLD  = 80
PAUSE_API        = 0.7          # polite delay between live queries
NAICS_API_TOKEN  = os.getenv("NAICS_API_TOKEN")   # if you have one

# optional, free fallback (sparser) – no key needed
OPEN_CORP_URL    = "https://api.opencorporates.com/v0.4/companies/search"


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


STRUCT = load_structure(NAICS_CSV_STRUCT)

# ------------------------------------------------------------------
# 3.  LOOK-UP TABLES  ---------------------------------------------
# ------------------------------------------------------------------
# 111110  →  "Soybean farming"
TITLE_BY_CODE = STRUCT["Class title"].to_dict()     # exact StatsCan header

naics_df = STRUCT.reset_index()                         # bring columns back
naics_df["clean_title"] = naics_df["Class title"].str.strip().str.lower()

NAICS_CHOICES  = naics_df["clean_title"].tolist()       # → list[str]
TITLE_TO_CODE  = dict(zip(naics_df["clean_title"],      # → {'soybean farming': '111110', …}
                         naics_df["clean_code"]))


def split_chain(code6: str) -> dict:
    """
    Given a *six-digit string*, return the matching titles for the NAICS chain.
    Missing pieces return "", never raise.
    """
    if not isinstance(code6, str) or not code6.isdigit():
        return {k: "" for k in
                ["Sector","Subsector","Industry_group",
                 "Industry","Canadian_industry","Class_desc"]}

    code6 = code6.zfill(6)                       # safety pad
    return {
        "Sector"           : TITLE_BY_CODE.get(code6[:2].ljust(6, "0"),  ""),
        "Subsector"        : TITLE_BY_CODE.get(code6[:3].ljust(6, "0"),  ""),
        "Industry_group"   : TITLE_BY_CODE.get(code6[:4].ljust(6, "0"),  ""),
        "Industry"         : TITLE_BY_CODE.get(code6[:5].ljust(6, "0"),  ""),
        "Canadian_industry": TITLE_BY_CODE.get(code6,                    ""),
        "Class_desc"       : TITLE_BY_CODE.get(code6,                    "")
    }

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
                cutoff: int = 70):
    """
    Return (naics_code, naics_title, score).  If nothing clears the cutoff
    the function returns (None, None, 0) so the caller never crashes.
    """
    if not isinstance(raw_text, str) or not raw_text.strip():
        return None, None, 0

    # thefuzz returns None when score_cutoff isn't met
    result = process.extractOne(
        raw_text,
        choices,
        scorer=fuzz.token_set_ratio,
        score_cutoff=cutoff
    )

    if result is None:
        return None, None, 0

    # Accept both 2- and 3-tuple return styles
    choice = result[0]
    score  = result[1]

    naics_code = code_lookup.get(choice)          # may be None
    return naics_code, choice, score

# ------------------------------------------------------------------
# 4. OPTIONAL ONLINE LOOK-UP ---------------------------------------
# ------------------------------------------------------------------
def online_naics(company:str, city:str=""):
    """Try NAICS.com  → OpenCorporates fallback."""
    if NAICS_API_TOKEN:
        try:
            r = requests.get(
                "https://api.naics.com/v1/company/",
                params={"company":company,"city":city,"key":NAICS_API_TOKEN},
                timeout=15
            ).json()
            if r.get("primary_naics"):
                n = r["primary_naics"][0]
                return n["code"], n["title"]
        except Exception:
            pass

    # ---- OpenCorporates (free) ----
    try:
        q  = f'"{company}" {city}'
        oc = requests.get(OPEN_CORP_URL, params={"q":q,"per_page":1}, timeout=15).json()
        comps = oc.get("results",{}).get("companies",[])
        if comps:
            codes = comps[0]["company"].get("industry_codes",[])
            for c in codes:
                if c["industry_code_scheme"]=="US_NAICS_2017":
                    return c["code"], c.get("description","")
    except Exception:
        pass
    return None, None

# ------------------------------------------------------------------
# 5. READ SOURCE & PROCESS ROWS  (re-written, no web-look-ups)
# ------------------------------------------------------------------
df = pd.read_csv(IN_FILE, dtype=str, low_memory=False)

naics_code_col, naics_desc_col            = [], []
sector_col, subsector_col                 = [], []
ig_col, ind_col                           = [], []
canind_col, classdesc_col                 = [], []

def first_non_blank(row, *cols):
    """Return the first non-empty string among the given columns."""
    for c in cols:
        val = str(row.get(c, "")).strip()
        if val:
            return val
    return ""

for idx, row in tqdm(df.iterrows(), total=len(df), desc="Rows"):

    existing = str(row.get("NAICS_Code", "")).strip()
    if existing:                                    # ── rule 5
        codes   = [existing]
        titles  = [TITLE_BY_CODE.get(existing, "")]
        logging.debug(f"[{idx}] NAICS already present → {existing}")
    else:
        raw_src = first_non_blank(                 # ── rules 1-4
            row, "Imported_SiteDescription",
                 "SubSiteSICDesc",
                 "IndustryType"
        )

        if not raw_src:                            # nothing to match
            codes, titles = [], []
            logging.debug(f"[{idx}] no source text → skip fuzzy")
        else:
            desc_cln = clean_text(raw_src)
            code, title, score = fuzzy_naics(
                desc_cln,
                choices     = NAICS_CHOICES,
                code_lookup = TITLE_TO_CODE,
                cutoff      = FUZZY_THRESHOLD
            )
            if code:
                codes, titles = [code], [title]
                logging.info(f"[{idx}] ✔ fuzzy hit {code} ({score}) {title!r}")
            else:
                codes, titles = [], []
                logging.debug(f"[{idx}] ✘ no fuzzy match for {desc_cln!r}")

    # ---------- write results ----------
    naics_code_col.append(";".join(codes))
    naics_desc_col.append(";".join(titles))

    sectors, subsectors, igs, inds, caninds, cdescs = [], [], [], [], [], []
    for c in codes:
        chain = split_chain(c)
        sectors.append(chain["Sector"])
        subsectors.append(chain["Subsector"])
        igs.append(chain["Industry_group"])
        inds.append(chain["Industry"])
        caninds.append(chain["Canadian_industry"])
        cdescs.append(chain["Class_desc"])

    sector_col.append(";".join(sectors))
    subsector_col.append(";".join(subsectors))
    ig_col.append(";".join(igs))
    ind_col.append(";".join(inds))
    canind_col.append(";".join(caninds))
    classdesc_col.append(";".join(cdescs))


# ------------------------------------------------------------------
# 6. WRITE RESULTS  ➜  three separate CSVs
# ------------------------------------------------------------------
df["NAICS_Code"]        = naics_code_col
df["NAICS_Desc"]        = naics_desc_col
df["Sector"]            = sector_col
df["Subsector"]         = subsector_col
df["Industry_group"]    = ig_col
df["Industry"]          = ind_col
df["Canadian_industry"] = canind_col
df["Class_desc"]        = classdesc_col

# 6-A.  main cleaned data ------------------------------------------
MAIN_CSV = OUT_FILE               # e.g. "naics_clean.csv"
df.to_csv(MAIN_CSV, index=False)

# 6-B.  SharePoint choice lists ------------------------------------
code_choices  = sorted({c for row in naics_code_col  for c in str(row).split(";") if c})
desc_choices  = sorted({d for row in naics_desc_col for d in str(row).split(";") if d})

pd.DataFrame({"NAICS_Code_Choices":  code_choices}).to_csv(
    "naics_code_choices.csv",  index=False)

pd.DataFrame({"NAICS_Desc_Choices": desc_choices}).to_csv(
    "naics_desc_choices.csv", index=False)

print("✓ CSVs written:")
print(f"   • {MAIN_CSV}")
print("   • naics_code_choices.csv")
print("   • naics_desc_choices.csv")
