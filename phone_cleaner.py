"""
phone_cleaner.py
────────────────
Normalise & classify phone numbers contained in a SubSitePhoneMain-style column
and write them back to the DataFrame in two convenient ways:

1. A dict-of-lists for downstream processing  ➜  column “PhoneBuckets”
2. A flat, semicolon-separated string of all numbers  ➜  column “PhoneNumbers”

Dependencies
------------
pandas          ≥ 1.3
phonenumbers    ≥ 8.13
"""

import re
from typing import Dict, Set

import pandas as pd
import phonenumbers
from phonenumbers import (
    PhoneNumberMatcher,
    PhoneNumberFormat,
    NumberParseException,
)

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG – ***all previously highlighted constants are preserved and expanded***
# ────────────────────────────────────────────────────────────────────────────────
DEFAULT_REGION: str = "CA"      # change if your data is centred elsewhere
DEFAULT_AREA:   str = "204"     # injected when a 7-digit local number is found

TOLL_FREE_CODES: Set[str] = {
    "800", "888", "877", "866", "855", "844", "833", "822"
}

# Intentionally verbose keyword lists – feel free to tweak / localise further
KEYWORDS: Dict[str, list[str]] = {
    "cell": [
        "cell", "cel", "cel.", "mobile", "mob", "m", "c", "hand", "smartphone"
    ],
    "home": [
        "home", "res", "res.", "res:", "house", "h", "r", "residence"
    ],
    "work": [
        "work", "wk", "bus", "business", "w", "t"
    ],
    "office": [
        "office", "off", "o", "main", "site", "store", "shop", "pro shop",
        "church", "restaurant", "dir", "direct", "line", "hq", "admin",
        "lab", "spa", "salon"
    ],
    "fax": [
        "fax", "f", "fx"
    ],
    "tollfree": [
        "toll free", "toll-free", "tollfree"
    ],
}

# Pre-compiled 7/10/11-digit grabber (kept exactly as in the original code)
PHONE_RE = re.compile(
    r"""
    (?:\+?1[\s\-\.]?\s*)?          # optional country code
    (?:\(?\d{3}\)?[\s\-\.]?\s*)?   # optional area code
    \d{3}[\s\-\.]?\s*\d{4}         # local number
    """,
    re.VERBOSE,
)

# ────────────────────────────────────────────────────────────────────────────────
# HELPERS – all previous helpers are still here, some slightly enhanced
# ────────────────────────────────────────────────────────────────────────────────
def _clean_local(raw: str, default_area: str = DEFAULT_AREA) -> str:
    """
    Strip non-digits, prepend default area code if only 7 digits were present,
    and return a +1… E.164 string so that `phonenumbers` can re-parse it.
    """
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 7 and default_area:
        digits = default_area + digits
    if len(digits) == 10:
        digits = "1" + digits
    return "+" + digits


def _label_for(text: str, start: int, end: int) -> str:
    """
    Infer a semantic label (cell | home | office | …) from nearby words,
    preserving every rule from the original snippet and adding a few more.
    """
    window = text[max(0, start - 40) : end + 15].lower()

    # 1️⃣ Toll-free if the area code matches the toll-free list
    ac_match = re.match(r"\D*1?\D*(\d{3})", text[start:end])
    if ac_match and ac_match.group(1) in TOLL_FREE_CODES:
        return "tollfree"

    # 2️⃣ Keyword search (longest → shortest window)
    for label, kws in KEYWORDS.items():
        for kw in kws:
            if re.search(rf"\b{re.escape(kw)}\b", window):
                return label

    # 3️⃣ Single-letter shorthands right in front of the number
    single = text[max(0, start - 2) : start].strip().lower()
    if single in {"c", "m"}:
        return "cell"
    if single in {"t", "o", "w"}:
        return "work"
    if single in {"h", "r"}:
        return "home"

    return "other"


def extract_numbers(s: str, default_area: str = DEFAULT_AREA) -> Dict[str, list[str]]:
    """
    Return a mapping     {'cell': [...], 'office': [...], …}
    containing **unique, nicely formatted** phone numbers found in `s`.
    """
    buckets: Dict[str, Set[str]] = {
        k: set() for k in ["cell", "home", "work", "office", "fax", "tollfree", "other"]
    }
    if not isinstance(s, str):
        return buckets

    # Pass 1 – the robust phonenumbers matcher
    for match in PhoneNumberMatcher(s, DEFAULT_REGION):
        label = _label_for(s, match.start, match.end)
        formatted = phonenumbers.format_number(
            match.number, PhoneNumberFormat.NATIONAL
        )
        buckets[label].add(formatted)

    # Pass 2 – regex fallback for “naked” 7-digit locals, etc.
    for m in PHONE_RE.finditer(s):
        cleaned = _clean_local(m.group(), default_area)
        try:
            pn = phonenumbers.parse(cleaned, "US")  # region is irrelevant after +1
            formatted = phonenumbers.format_number(pn, PhoneNumberFormat.NATIONAL)
        except NumberParseException:
            formatted = m.group()
        label = _label_for(s, m.start(), m.end())
        buckets[label].add(formatted)

    # Convert the sets → sorted lists, drop empties
    return {k: sorted(v) for k, v in buckets.items() if v}


# ────────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ────────────────────────────────────────────────────────────────────────────────
def tidy_subsite_phone_column(
    df: pd.DataFrame,
    col: str = "SubSitePhoneFax",
    default_area: str = DEFAULT_AREA,
    delimiter: str = "; ",
    new_buckets_col: str = "PhoneFaxBuckets",
    new_flat_col: str = "PhoneFaxNumbers",
) -> pd.DataFrame:
    """
    ◼  Parses & classifies every row of `df[col]`.
    ◼  Adds two new columns (names configurable):
       1. `new_buckets_col` – dict of labelled lists
       2. `new_flat_col`    – every phone number seen, joined by `delimiter`
    ◼  Returns the **same** DataFrame instance for chaining.

    Example
    -------
    >>> df = tidy_subsite_phone_column(df)
    """
    buckets_series = df[col].apply(lambda x: extract_numbers(str(x), default_area))
    df[new_buckets_col] = buckets_series

    # Flatten → semicolon-separated string (unique per row, preserve input order)
    def _flatten(bucket_dict: Dict[str, list[str]]) -> str:
        seen = []
        for lst in bucket_dict.values():  # preserve bucket order (cell→home→…)
            for num in lst:
                if num not in seen:
                    seen.append(num)
        return delimiter.join(seen)

    df[new_flat_col] = buckets_series.apply(_flatten)
    return df


# ────────────────────────────────────────────────────────────────────────────────
# CLI / SCRIPT ENTRY-POINT (optional convenience)
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(
        description="Clean & classify phone numbers in a CSV or Parquet file."
    )
    parser.add_argument(
        "input_path",
        type=Path,
        help="CSV or Parquet file containing a 'SubSitePhoneMain' column.",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Where to write the cleaned file (extension .csv or .parquet). "
        "Defaults to '<input>_clean[.ext]'.",
    )
    parser.add_argument(
        "--delimiter",
        "-d",
        default="; ",
        help="Delimiter used in the flattened PhoneNumbers column (default '; ').",
    )
    args = parser.parse_args()

    # Detect format
    if args.input_path.suffix.lower() == ".csv":
        df = pd.read_csv(args.input_path)
    elif args.input_path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(args.input_path)
    else:
        raise ValueError("Input file must be .csv or .parquet")

    tidy_subsite_phone_column(df, delimiter=args.delimiter)

    out_path = (
        args.output
        or args.input_path.with_name(args.input_path.stem + "_clean" + args.input_path.suffix)
    )
    if out_path.suffix.lower() == ".csv":
        df.to_csv(out_path, index=False)
    else:
        df.to_parquet(out_path, index=False)

    print(f"Cleaned file written to: {out_path.resolve()}")
