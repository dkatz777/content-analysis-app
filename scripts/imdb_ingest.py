from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


DATA_DIR = Path("data")
IMDB_DIR = DATA_DIR / "imdb"

BASICS = IMDB_DIR / "title.basics.tsv.gz"
EPISODE = IMDB_DIR / "title.episode.tsv.gz"
RATINGS = IMDB_DIR / "title.ratings.tsv.gz"

SHOW_MAP = DATA_DIR / "show_external_ids.csv"


@dataclass(frozen=True)
class ImdbPaths:
    slug: str
    out_master: Path

    @staticmethod
    def for_slug(slug: str) -> "ImdbPaths":
        return ImdbPaths(
            slug=slug,
            out_master=DATA_DIR / f"{slug}_imdb_episodes_master.csv",
        )


def _require_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing required file: {p}")


def load_show_imdb_id(slug: str, show_map_path: Path = SHOW_MAP) -> str:
    _require_file(show_map_path)
    df = pd.read_csv(show_map_path, dtype=str).fillna("")
    if "slug" not in df.columns or "imdb_title_id" not in df.columns:
        raise ValueError("show_external_ids.csv must include columns: slug, imdb_title_id")

    row = df.loc[df["slug"].str.strip().str.lower() == slug.strip().lower()]
    if row.empty:
        raise ValueError(f"No row found for slug='{slug}' in {show_map_path}")

    imdb_id = row.iloc[0]["imdb_title_id"].strip()
    if not imdb_id:
        raise ValueError(f"Empty imdb_title_id for slug='{slug}' in {show_map_path}")

    if not imdb_id.startswith("tt"):
        raise ValueError(f"imdb_title_id should look like 'tt1234567', got '{imdb_id}'")

    return imdb_id


def read_imdb_tsv(path: Path, usecols: list[str], dtype: Optional[dict] = None) -> pd.DataFrame:
    _require_file(path)
    return pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        usecols=usecols,
        dtype=dtype,
        na_values=["\\N"],
        keep_default_na=True,
        low_memory=False,
    )


def build_episode_master_for_show(series_tconst: str) -> pd.DataFrame:
    # 1) Load title.episode for this series (parentTconst -> child episode tconst)
    ep = read_imdb_tsv(
        EPISODE,
        usecols=["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
        dtype={"tconst": "string", "parentTconst": "string", "seasonNumber": "string", "episodeNumber": "string"},
    )

    ep = ep.loc[ep["parentTconst"] == series_tconst].copy()
    if ep.empty:
        return pd.DataFrame()

    ep.rename(
        columns={
            "tconst": "imdb_episode_id",
            "parentTconst": "imdb_series_id",
            "seasonNumber": "season_number",
            "episodeNumber": "episode_number",
        },
        inplace=True,
    )

    # season_number and episode_number should be numeric when possible
    ep["season_number"] = pd.to_numeric(ep["season_number"], errors="coerce")
    ep["episode_number"] = pd.to_numeric(ep["episode_number"], errors="coerce")

    # 2) Join basics for episode titles and basic metadata
    basics = read_imdb_tsv(
        BASICS,
        usecols=["tconst", "titleType", "primaryTitle", "originalTitle", "startYear", "runtimeMinutes", "genres"],
        dtype={
            "tconst": "string",
            "titleType": "string",
            "primaryTitle": "string",
            "originalTitle": "string",
            "startYear": "string",
            "runtimeMinutes": "string",
            "genres": "string",
        },
    ).rename(columns={"tconst": "imdb_episode_id"})

    merged = ep.merge(basics, on="imdb_episode_id", how="left", validate="m:1")

    # 3) Join ratings
    ratings = read_imdb_tsv(
        RATINGS,
        usecols=["tconst", "averageRating", "numVotes"],
        dtype={"tconst": "string", "averageRating": "string", "numVotes": "string"},
    ).rename(columns={"tconst": "imdb_episode_id"})

    merged = merged.merge(ratings, on="imdb_episode_id", how="left", validate="m:1")

    # Normalize numeric types
    merged["averageRating"] = pd.to_numeric(merged["averageRating"], errors="coerce")
    merged["numVotes"] = pd.to_numeric(merged["numVotes"], errors="coerce")
    merged["startYear"] = pd.to_numeric(merged["startYear"], errors="coerce")
    merged["runtimeMinutes"] = pd.to_numeric(merged["runtimeMinutes"], errors="coerce")

    # Filter to actual episodes when titleType is present
    # IMDb uses titleType "tvEpisode" for episodes in basics
    if "titleType" in merged.columns:
        merged = merged.loc[(merged["titleType"].isna()) | (merged["titleType"] == "tvEpisode")].copy()

    # Canonical column names for your app
    merged.rename(
        columns={
            "primaryTitle": "episode_title",
            "originalTitle": "episode_title_original",
            "startYear": "episode_year",
            "runtimeMinutes": "runtime_minutes",
            "genres": "genres",
            "averageRating": "imdb_rating",
            "numVotes": "imdb_votes",
        },
        inplace=True,
    )

    # Sorting for stable output
    merged.sort_values(["season_number", "episode_number", "imdb_episode_id"], inplace=True)

    # Add placeholders for future enrichments
    merged["episode_air_date"] = pd.NA  # not available in datasets
    merged["season_label"] = merged["season_number"].apply(lambda x: f"S{x:.0f}" if pd.notna(x) else pd.NA)

    # Keep only the fields you will actually use
    cols = [
        "imdb_series_id",
        "imdb_episode_id",
        "season_number",
        "episode_number",
        "season_label",
        "episode_title",
        "episode_title_original",
        "episode_year",
        "episode_air_date",
        "runtime_minutes",
        "genres",
        "imdb_rating",
        "imdb_votes",
    ]
    return merged[cols]


def merge_master(existing: Optional[pd.DataFrame], incoming: pd.DataFrame) -> pd.DataFrame:
    """
    Additive merge keyed by imdb_episode_id.
    If incoming has a value for a field, it overwrites existing, otherwise keep existing.
    """
    if existing is None or existing.empty:
        return incoming.copy()

    key = "imdb_episode_id"
    if key not in existing.columns or key not in incoming.columns:
        raise ValueError("Both existing and incoming must include imdb_episode_id")

    existing = existing.copy()
    incoming = incoming.copy()

    # Align columns
    for c in incoming.columns:
        if c not in existing.columns:
            existing[c] = pd.NA
    for c in existing.columns:
        if c not in incoming.columns:
            incoming[c] = pd.NA

    existing.set_index(key, inplace=True)
    incoming.set_index(key, inplace=True)

    # Combine: prefer incoming non-null values
    combined = existing.combine_first(incoming)  # fills existing nulls with incoming
    # Now overwrite where incoming is non-null (incoming should be authoritative for these fields)
    for col in incoming.columns:
        mask = incoming[col].notna()
        combined.loc[mask, col] = incoming.loc[mask, col]

    combined.reset_index(inplace=True)
    return combined


def main() -> None:
    parser = argparse.ArgumentParser(description="Build or update per-show IMDb episode master from IMDb datasets.")
    parser.add_argument("--slug", required=True, help="Show slug, must exist in data/show_external_ids.csv")
    args = parser.parse_args()

    # Required files
    _require_file(BASICS)
    _require_file(EPISODE)
    _require_file(RATINGS)
    _require_file(SHOW_MAP)

    slug = args.slug.strip().lower()
    series_id = load_show_imdb_id(slug)

    out = ImdbPaths.for_slug(slug).out_master

    incoming = build_episode_master_for_show(series_id)
    if incoming.empty:
        raise SystemExit(f"No episodes found for IMDb series id {series_id}. Check show_external_ids.csv.")

    existing = None
    if out.exists():
        existing = pd.read_csv(out, dtype=str).replace({"": pd.NA})

        # Fix numeric columns back to numeric for merge behavior
        for c in ["season_number", "episode_number", "episode_year", "runtime_minutes", "imdb_rating", "imdb_votes"]:
            if c in existing.columns:
                existing[c] = pd.to_numeric(existing[c], errors="coerce")

    merged = merge_master(existing, incoming)

    # Stable typing and sort
    for c in ["season_number", "episode_number"]:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")
    merged.sort_values(["season_number", "episode_number", "imdb_episode_id"], inplace=True)

    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out, index=False)
    print(f"Wrote {len(merged)} rows to {out}")


if __name__ == "__main__":
    main()
