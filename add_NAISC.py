"""
Clean SubSiteSICDesc ➜ NAICS hierarchy (2022)
Author: 2025-06-23
----------------------------------------------------------
INPUT : subsites.xlsx            (first worksheet)
        naics-scian-2022-structure-v1-eng.csv  ⟵ hierarchy
OUTPUT: subsites_naics_clean.xlsx
        • all original cols  +
        • NAICS_Code            (6-digit; ‘;’-separated)
        • NAICS_Desc            (same order)
        • Sector
        • Subsector
        • Industry_group
        • Industry
        • Canadian_industry
        • Class_desc
----------------------------------------------------------
pip install pandas requests rapidfuzz tqdm openpyxl xlsxwriter
"""
import os, re, time, requests, pandas as pd
from rapidfuzz import process, utils
from tqdm.auto import tqdm
import unicodedata
from typing import List
import pandas as pd
from autocorrect import Speller 

# ------------------------------------------------------------------
# 1. PARAMETERS  ----------------------------------------------------
# ------------------------------------------------------------------
IN_FILE          = "cleaned_unions.csv"
OUT_FILE         = "naics_clean.xlsx"
NAICS_CSV_STRUCT = "naics-scian-2022-structure-v1-eng.csv"
FUZZY_THRESHOLD  = 80
PAUSE_API        = 0.7          # polite delay between live queries
NAICS_API_TOKEN  = os.getenv("NAICS_API_TOKEN")   # if you have one

# optional, free fallback (sparser) – no key needed
OPEN_CORP_URL    = "https://api.opencorporates.com/v0.4/companies/search"

# ------------------------------------------------------------------
# 2. LOAD & NORMALISE NAICS HIERARCHY  ------------------------------
# ------------------------------------------------------------------
def load_structure(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    # keep everything; index by the code for O(1) look-ups
    return df.set_index("code")

STRUCT = load_structure(NAICS_CSV_STRUCT)

def split_chain(code: str) -> dict[str, str]:
    """
    Given a 6-digit NAICS code, walk up the Parent chain and return:
      Sector, Subsector, Industry_group, Industry, Canadian_industry, Class_desc
    Empty strings if something is missing.
    """
    res = dict.fromkeys(
        ["Sector", "Subsector", "Industry_group", "Industry",
         "Canadian_industry", "Class_desc"], ""
    )
    row = STRUCT.loc[code] if code in STRUCT.index else None
    if row is None:
        return res

    # Level 5 == Canadian industry
    res["Canadian_industry"] = row.class_title
    res["Class_desc"]        = row.class_definition
    parent = row.parent

    while parent and parent in STRUCT.index:
        prow   = STRUCT.loc[parent]
        lvl    = int(prow.level)
        title  = prow.class_title

        if   lvl == 4: res["Industry"]         = title
        elif lvl == 3: res["Industry_group"]   = title
        elif lvl == 2: res["Subsector"]        = title
        elif lvl == 1: res["Sector"]           = title

        parent = prow.parent
    return res

# ------------------------------------------------------------------
# 3. FUZZY DESCRIPTION ▸ NAICS CODE  -------------------------------
# ------------------------------------------------------------------
titles_clean = STRUCT.loc[STRUCT.index.str.len()==6, "class_title"]\
                   .str.upper().str.replace(r"[^A-Z0-9 ]","",regex=True)
codes_6      = titles_clean.index.tolist()
titles_list  = titles_clean.tolist()

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

    # 1. Unicode normalisation (accents → ASCII, weird dashes → '-')  📑 :contentReference[oaicite:0]{index=0}
    txt = unicodedata.normalize("NFKD", txt)
    txt = txt.encode("ascii", errors="ignore").decode()

    # 2. Kill all line-breaks & tab characters  📑 :contentReference[oaicite:1]{index=1}
    txt = txt.replace("\r", " ").replace("\n", " ")

    # 3. Strip parenthetical or slash/dash comments such as
    #    “/ 642 per WCB …” or “#108 per WCB June 20/11”  📑 :contentReference[oaicite:2]{index=2}
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

def fuzzy_naics(desc: str):
    if not desc: return None, None, 0
    choice, score, idx = process.extractOne(
        utils.default_process(desc), titles_list, score_cutoff=FUZZY_THRESHOLD
    )
    return (codes_6[idx], STRUCT.loc[codes_6[idx]].class_title, score) if score else (None,None,0)

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
# 5. READ SOURCE & PROCESS ROWS ------------------------------------
# ------------------------------------------------------------------
df = pd.read_csv(IN_FILE)
naics_code_col, naics_desc_col = [], []

sector_col, subsector_col, ig_col, ind_col = [], [], [], []
canind_col, classdesc_col = [], []

for _, row in tqdm(df.iterrows(), total=len(df), desc="Rows"):
    raw_desc = row.get("SubSiteSICDesc", "")
    desc_cln = clean_text(raw_desc)

    codes, descs = [], []

    # A) fuzzy match first
    code, title, score = fuzzy_naics(desc_cln)
    if code:
        codes.append(code); descs.append(title)

    # B) fall back to live look-up once / row
    if not codes:
        c2, t2 = online_naics(str(row.get("CompanyName","")), str(row.get("City","")))
        if c2:
            codes.append(c2); descs.append(t2)
            time.sleep(PAUSE_API)

    # write columns (even if empty → "")
    naics_code_col.append(";".join(codes))
    naics_desc_col.append(";".join(descs))

    # split every code into hierarchy pieces, keep same order
    sectors, subsectors, igs, inds, caninds, cdescs = [], [], [], [], [], []
    for c in codes:
        ch = split_chain(c)
        sectors.append(ch["Sector"])
        subsectors.append(ch["Subsector"])
        igs.append(ch["Industry_group"])
        inds.append(ch["Industry"])
        caninds.append(ch["Canadian_industry"])
        cdescs.append(ch["Class_desc"])

    sector_col.append(";".join(sectors))
    subsector_col.append(";".join(subsectors))
    ig_col.append(";".join(igs))
    ind_col.append(";".join(inds))
    canind_col.append(";".join(caninds))
    classdesc_col.append(";".join(cdescs))

# ------------------------------------------------------------------
# 6. WRITE RESULTS  -------------------------------------------------
# ------------------------------------------------------------------
df["NAICS_Code"]        = naics_code_col
df["NAICS_Desc"]        = naics_desc_col
df["Sector"]            = sector_col
df["Subsector"]         = subsector_col
df["Industry_group"]    = ig_col
df["Industry"]          = ind_col
df["Canadian_industry"] = canind_col
df["Class_desc"]        = classdesc_col

# SharePoint choice sheets
code_choices  = sorted({c for row in naics_code_col for c in str(row).split(";") if c})
desc_choices  = sorted({d for row in naics_desc_col for d in str(row).split(";") if d})

with pd.ExcelWriter(OUT_FILE, engine="xlsxwriter") as xls:
    df.to_excel(xls, index=False, sheet_name="CleanedData")
    pd.DataFrame({"NAICS_Code_Choices": code_choices})\
      .to_excel(xls, index=False, sheet_name="Choices_Code")
    pd.DataFrame({"NAICS_Desc_Choices": desc_choices})\
      .to_excel(xls, index=False, sheet_name="Choices_Desc")

print(f"✓ Done → {OUT_FILE}")
