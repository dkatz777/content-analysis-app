from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
IMDB_DIR = DATA_DIR / "imdb"
BASICS = IMDB_DIR / "title.basics.tsv.gz"

def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve IMDb series title ID (tconst) from IMDb title.basics dataset.")
    parser.add_argument("--title", required=True, help="Series title to search for, e.g. 'The Sopranos'")
    parser.add_argument("--year", type=int, default=None, help="Optional start year to narrow results, e.g. 1999")
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()

    if not BASICS.exists():
        raise SystemExit(f"Missing {BASICS}. Download IMDb datasets into data/imdb/ first.")

    title_q = args.title.strip().lower()

    df = pd.read_csv(
        BASICS,
        sep="\t",
        compression="gzip",
        dtype="string",
        na_values=["\\N"],
        keep_default_na=True,
        low_memory=False,
        usecols=["tconst", "titleType", "primaryTitle", "originalTitle", "startYear", "endYear"],
    )

    # Filter to series
    df = df[df["titleType"] == "tvSeries"].copy()

    # Basic title match
    df["primary_lc"] = df["primaryTitle"].fillna("").str.lower()
    df["original_lc"] = df["originalTitle"].fillna("").str.lower()
    hits = df[(df["primary_lc"].str.contains(title_q, regex=False)) | (df["original_lc"].str.contains(title_q, regex=False))].copy()

    if args.year is not None and not hits.empty:
        hits["startYear_num"] = pd.to_numeric(hits["startYear"], errors="coerce")
        hits = hits[hits["startYear_num"] == args.year].copy()

    if hits.empty:
        print("No matches found. Try a shorter title string, remove year, or search via IMDb website (Option B).")
        return

    # Sort: exact primaryTitle match first, then closest startYear
    hits["exact"] = (hits["primary_lc"] == title_q) | (hits["original_lc"] == title_q)
    hits["startYear_num"] = pd.to_numeric(hits["startYear"], errors="coerce")
    hits.sort_values(["exact", "startYear_num"], ascending=[False, True], inplace=True)

    out = hits[["tconst", "primaryTitle", "originalTitle", "startYear", "endYear"]].head(args.limit)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()
