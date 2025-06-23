#!/usr/bin/env python3
"""
lotus_sr_to_csv.py
──────────────────
Parse a Lotus Notes ‘structured text’ export in which …

  • every field is on its own line,  e.g.  `SR_IncidentDate:  2020-05-12`
  • blocks/records are separated by ASCII form-feed  (0x0C, shown as )

The script:
  1. splits the file on the form-feed (or any user-supplied separator),
  2. converts each block into a dict {key → value}, keeping multi-line
     comments intact,
  3. builds a union header (first-seen order) so no row ever shifts,
  4. writes the table as CSV (or TSV, pipe, etc.).

Usage:
    python lotus_sr_to_csv.py LINK_Co_Data_LocationsByCommonName.txt
    python lotus_sr_to_csv.py LINK_Co_Data_LocationsByCommonName.txt -d "\t" -o sr.tsv
"""

import argparse, csv, pathlib, re, sys, textwrap

# ASCII form-feed ^L separates records in the Notes export
DEFAULT_SEP = "\f"
# -------------------------------------------------------------
KEY_RE = re.compile(r"^[A-Za-z0-9_$]+$")    # valid Notes field names
# -------------------------------------------------------------
###############################################################################
# 1 ───── CLI
###############################################################################
cli = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent(__doc__),
)
cli.add_argument("src", help="path to the Lotus Notes export (.txt)")
cli.add_argument("-o", "--out", default="service_requests.csv",
                 help="output file (default service_requests.csv)")
cli.add_argument("-s", "--sep", default=DEFAULT_SEP,
                 help=r"record separator (default form-feed \f)")
cli.add_argument("-d", "--delim", default=",",
                 help=r"output delimiter: ','  '\t'  '|' etc. (default ,)")
args = cli.parse_args()

out_delim = args.delim.encode().decode("unicode_escape")    # allow '\t'
sep       = args.sep.encode().decode("unicode_escape")

###############################################################################
# 2 ───── read & split the raw file into record blocks
###############################################################################
raw_text = pathlib.Path(args.src).read_text(encoding="utf-8", errors="replace")
blocks   = [b.strip() for b in re.split(sep, raw_text) if b.strip()]
if not blocks:
    sys.exit("❌  No records found. Check the --sep argument.")

###############################################################################
# 3 ───── helper: convert one block into a dict
###############################################################################
def parse_block(block: str) -> dict[str, str]:
    rec, last_key = {}, None
    for line in block.splitlines():
        line = line.rstrip()
        if not line:
            continue

        if ":" in line:
            left, right = line.split(":", 1)
            key = left.strip()
            if KEY_RE.match(key):           # ← tighter check
                last_key = key
                rec[key] = right.strip()
                continue

        # fall-through = continuation of previous field
        if last_key:
            sep = "\n" if "\n" in rec[last_key] else " "
            rec[last_key] += sep + line.strip()
    return rec

records = [parse_block(b) for b in blocks]

###############################################################################
# 4 ───── build header = union of every key (preserve first-seen order)
###############################################################################
seen, header = set(), []
for r in records:
    for k in r:
        if k not in seen:
            seen.add(k)
            header.append(k)

###############################################################################
# 5 ───── write CSV/TSV
###############################################################################
dest = pathlib.Path(args.out)
with dest.open("w", newline="", encoding="utf-8") as fh:
    w = csv.writer(fh, delimiter=out_delim, lineterminator="\n",
                   quoting=csv.QUOTE_MINIMAL)
    w.writerow(header)
    for r in records:
        w.writerow([r.get(col, "") for col in header])

print(f"✅  {len(records):,} records  →  {dest}  (delimiter={out_delim!r})")
print("    Open it directly in Excel or import into SharePoint › List › From Excel.")
