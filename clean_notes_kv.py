#!/usr/bin/env python3
"""
clean_notes_company.py
──────────────────────
Parse a Lotus-Notes “structured text” export (LINK_Company_Data.txt).

Differences vs clean_notes_kv.py
• Default input  ........  LINK_Company_Data.txt
• Default output ........  LINK_Company_Data.tsv  (tab-separated)
• Safer field detection .. only tokens that look like Notes keys
                           (^[A-Za-z0-9_$]+$) are treated as new columns,
                           so narrative text with colons never explodes
                           the header.
Everything else – CLI switches, record-separator handling, header-union
logic – is unchanged.
"""
import argparse, csv, pathlib, re, sys, textwrap

US       = "\x1f"              # default record separator (ASCII 31 ␟)
KEY_RE   = re.compile(r"^[A-Za-z0-9_$]+$")   # valid Notes field name

# ────────── CLI ────────────────────────────────────────────────────
p = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent(__doc__).split("Everything")[0],
    epilog="Example: python clean_notes_company.py -d ','  -o out.csv",
)
p.add_argument(
    "src",
    nargs="?",
    default="LINK_Company_Data.txt",
    help="Path to the Notes export (default: LINK_Company_Data.txt)",
)
p.add_argument(
    "-o", "--out",
    default=None,     # we’ll derive from src if user omits it
    help="Destination file (default: <src>.tsv)",
)
p.add_argument(
    "-s", "--separator",
    default=US,
    help="Record separator char/regex (default: ASCII 31 ␟)",
)
p.add_argument(
    "-d", "--delimiter",
    default="\\t",
    help=r"Output delimiter (default: '\t' = tab).  Use ',' for CSV etc.",
)
args = p.parse_args()

# derive defaults ──────────────────────────────────────────────────
src_path  = pathlib.Path(args.src)
if args.out is None:
    dest_path = src_path.with_suffix(".tsv")
else:
    dest_path = pathlib.Path(args.out)

out_delim = args.delimiter.encode().decode("unicode_escape")
sep_regex = args.separator.encode().decode("unicode_escape")

# ────────── 1. split file into record blocks ───────────────────────
text   = src_path.read_text(encoding="utf-8", errors="replace")
blocks = [b.strip() for b in re.split(sep_regex, text) if b.strip()]
if not blocks:
    sys.exit("❌  No records found – adjust the --separator.")

# ────────── 2. turn one block into a dict ──────────────────────────
def parse_block(block: str) -> dict[str, str]:
    rec, last_key = {}, None
    for line in block.splitlines():
        line = line.rstrip()
        if not line:
            continue
        if ":" in line:
            left, right = line.split(":", 1)
            key = left.strip()
            if KEY_RE.match(key):           # accept only Notes-style keys
                last_key        = key
                rec[last_key]   = right.strip()
                continue
        if last_key:                        # continuation line
            sep = "\n" if "\n" in rec[last_key] else " "
            rec[last_key] += sep + line.strip()
    return rec

records = [parse_block(b) for b in blocks]

# ────────── 3. build union header (first-seen order) ───────────────
header, seen = [], set()
for r in records:
    for k in r:
        if k not in seen:
            seen.add(k)
            header.append(k)

# ────────── 4. write TSV/CSV ───────────────────────────────────────
with dest_path.open("w", encoding="utf-8", newline="") as fh:
    w = csv.writer(fh, delimiter=out_delim, lineterminator="\n",
                   quoting=csv.QUOTE_MINIMAL)
    w.writerow(header)
    for r in records:
        w.writerow([r.get(col, "") for col in header])

print(f"✅  {len(records):,} records → {dest_path} (delimiter={out_delim!r})")
print("   Open it in Excel or import into SharePoint ▸ List ▸ From Excel.")
