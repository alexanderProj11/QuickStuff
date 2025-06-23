import re
import pandas as pd

# ------------------------------------------------------------------
# 1)  Customise this mapping as you discover new unions
# ------------------------------------------------------------------
UNION_PATTERNS = {
    r'(?:united\s+steelworkers|uswa?|u\.?s\.?w\.?)': 'USW',
    r'(?:united\s+food.*commercial.*workers?|ufcw)': 'UFCW',
    r'(?:canadian\s+union\s+of\s+public\s+employees?|cupe)': 'CUPE',
    r'(?:manitoba\s+government.*employees.*union|mgeu)': 'MGEU',
    r'(?:manitoba\s+nurses\s+union|mnu)': 'MNU',
    r'(?:service\s+employees.*international.*union|seiu)': 'SEIU',
    r'(?:international\s+alliance.*theatrical.*stage.*employees?|iatse)': 'IATSE',
    r'(?:international\s+union.*operating\s+engineers?|iuo?e)': 'IUOE',
    r'(?:international\s+union.*painters.*allied\s+trades?|iupat)': 'IUPAT',
    r'(?:retail\s+wholesale.*department.*store.*union|rwdsu)': 'RWDSU',
    r'(?:sheet\s+metal.*workers.*international.*association|smwia)': 'SMWIA',
    r'(?:united\s+garment\s+workers.*america|ugwa)': 'UGWA',
    r'(?:workers\s+united)': 'WORKERS UNITED',
    r'(?:winnipeg\s+teachers.*association|wta)': 'WTA',
    r'(?:winnipeg\s+association\s+of\s+non[-\s]?teaching\s+employees|wante)': 'WANTE',
}

# Pre-compile the patterns for speed
UNION_REGEXES = [(re.compile(pat, re.I), abbr) for pat, abbr in UNION_PATTERNS.items()]

# A very tolerant "local-ID" regex
LOCAL_REGEX = re.compile(r'(?:local|loc|#|/|-)\s*([0-9]{2,6})', re.I)

def extract_unions(text: str) -> str:
    """Return semicolon-separated 'ABBR Local ####' (or just ABBR) list."""
    if not isinstance(text, str) or not text.strip():
        return ''
    
    found = []
    seen = set()
    for r, abbr in UNION_REGEXES:
        for m in r.finditer(text):
            # Examine a small window on either side of the match
            start, end = m.span()
            window = text[max(0, start-20): end+40]
            
            # 1) generic 'Local 1234' style
            locals_raw = LOCAL_REGEX.findall(window)
            # 2) 'ABBR 1234' style
            locals_raw += re.findall(rf'\b{abbr}\b[ \t\-/#]*([0-9]{{2,6}})', window, flags=re.I)
            
            locals_clean = list(dict.fromkeys(locals_raw))  # unique while preserving order
            if not locals_clean:
                locals_clean = [None]  # ensures at least one loop
            
            for loc in locals_clean:
                token = f"{abbr} Local {loc}" if loc else abbr
                if token not in seen:
                    found.append(token)
                    seen.add(token)
                    
    return '; '.join(found)

# ------------------------------------------------------------------
# 2)  Apply to your dataframe
# ------------------------------------------------------------------
df = pd.read_csv('sites_with_unclean_unions.csv')          # or wherever the data live
df['CleanedSubSiteUnion'] = df['SubSiteUnion'].apply(extract_unions)

# Optional: save the cleaned data
df.to_csv('cleaned_unions.csv', index=False)

