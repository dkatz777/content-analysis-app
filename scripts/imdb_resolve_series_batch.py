from __future__ import annotations

from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
IMDB_DIR = DATA_DIR / "imdb"

BASICS = IMDB_DIR / "title.basics.tsv.gz"

INPUT = DATA_DIR / "show_slugs.csv"
OUTPUT = DATA_DIR / "imdb_series_id_candidates.csv"


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")


def load_basics() -> pd.DataFrame:
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
    # Precompute lowercase fields once for speed
    df["primary_lc"] = df["primaryTitle"].fillna("").str.lower()
    df["original_lc"] = df["originalTitle"].fillna("").str.lower()
    df["startYear_num"] = pd.to_numeric(df["startYear"], errors="coerce")
    return df


def score_candidates(hits: pd.DataFrame, query_title: str) -> pd.DataFrame:
    """
    Produce a simple confidence score without needing year.
    Higher is better.
    """
    q = query_title.strip().lower()

    # Exact match gets a big boost
    hits["exact_match"] = ((hits["primary_lc"] == q) | (hits["original_lc"] == q)).astype(int)

    # Prefix match is useful
    hits["prefix_match"] = (hits["primary_lc"].str.startswith(q)).astype(int)

    # Title contains query already enforced, but we add a mild bonus if primaryTitle contains it
    hits["primary_contains"] = hits["primary_lc"].str.contains(q, regex=False).astype(int)

    # Prefer more "modern" series only slightly when no other signal.
    # This is intentionally mild to avoid biasing remakes too hard.
    hits["year_bonus"] = hits["startYear_num"].fillna(0) / 10000.0  # tiny

    # Total score
    hits["score"] = (
        hits["exact_match"] * 100
        + hits["prefix_match"] * 20
        + hits["primary_contains"] * 5
        + hits["year_bonus"]
    )

    hits.sort_values(["score", "startYear_num"], ascending=[False, True], inplace=True)
    return hits


def resolve_series(title: str, start_year: int | None, basics: pd.DataFrame) -> tuple[dict, pd.DataFrame] | tuple[None, pd.DataFrame]:
    """
    Returns (best_match_dict, top_candidates_df) or (None, candidates_df)
    """
    q = title.strip().lower()

    df = basics[basics["titleType"] == "tvSeries"].copy()

    # Candidate pool: contains query in primary or original title
    hits = df[
        (df["primary_lc"].str.contains(q, regex=False)) |
        (df["original_lc"].str.contains(q, regex=False))
    ].copy()

    if hits.empty:
        return None, hits

    if start_year is not None:
        hits = hits[hits["startYear_num"] == start_year].copy()
        if hits.empty:
            return None, hits

    hits = score_candidates(hits, title)

    top5 = hits[["tconst", "primaryTitle", "originalTitle", "startYear", "endYear", "score"]].head(5).copy()
    best = hits.iloc[0].to_dict()
    return best, top5


def parse_optional_year(val) -> int | None:
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    return None


def main() -> None:
    require_file(BASICS)
    require_file(INPUT)

    basics = load_basics()
    shows = pd.read_csv(INPUT).fillna("")

    # Validate input columns
    if "slug" not in shows.columns or "show_title" not in shows.columns:
        raise ValueError("data/show_slugs.csv must include columns: slug, show_title (optional: start_year)")

    results = []

    total = len(shows)
    for i, row in shows.iterrows():
        slug = str(row["slug"]).strip().lower()
        title = str(row["show_title"]).strip()
        year = parse_optional_year(row["start_year"]) if "start_year" in shows.columns else None

        print(f"[{i+1}/{total}] Resolving slug='{slug}' title='{title}' year={year if year else 'n/a'}")

        best, top = resolve_series(title, year, basics)

        if best is None:
            print("  -> NO MATCH")
            results.append({
                "slug": slug,
                "show_title": title,
                "start_year": year,
                "imdb_title_id": "",
                "imdb_primary_title": "",
                "imdb_start_year": "",
                "confidence_score": "",
                "needs_review": "yes",
                "match_status": "no_match",
                "top_candidates": "",
            })
            continue

        # Confidence heuristics:
        # - exact match + big score is high confidence
        # - if the best score is close to second best, mark for review
        best_score = float(best.get("score", 0))
        second_score = float(top.iloc[1]["score"]) if len(top) > 1 else 0.0
        margin = best_score - second_score

        needs_review = "no"
        if best_score < 90:
            needs_review = "yes"
        if margin < 15:
            needs_review = "yes"

        top_str = " | ".join(
            f'{r["tconst"]}:{r["primaryTitle"]}({r["startYear"]})[{int(r["score"])}]'
            for _, r in top.iterrows()
        )

        print(f"  -> MATCH {best['tconst']} '{best.get('primaryTitle','')}' ({best.get('startYear','')}) "
              f"score={int(best_score)} margin={int(margin)} review={needs_review}")

        results.append({
            "slug": slug,
            "show_title": title,
            "start_year": year,
            "imdb_title_id": best["tconst"],
            "imdb_primary_title": best.get("primaryTitle", ""),
            "imdb_start_year": best.get("startYear", ""),
            "confidence_score": int(best_score),
            "needs_review": needs_review,
            "match_status": "auto_match",
            "top_candidates": top_str,
        })

    out_df = pd.DataFrame(results)
    out_df.sort_values(["needs_review", "match_status", "slug"], ascending=[True, True, True], inplace=True)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUTPUT, index=False)

    print(f"\nDone. Wrote {len(out_df)} rows to {OUTPUT}")
    print("Next: review 'needs_review=yes' rows, then copy confirmed rows into data/show_external_ids.csv")


if __name__ == "__main__":
    main()
