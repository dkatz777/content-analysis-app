from __future__ import annotations

import argparse
from pathlib import Path
import re
import pandas as pd

DATA_DIR = Path("data")

SHOWS_META = DATA_DIR / "shows_meta.csv"
SLUG_ALIASES = DATA_DIR / "slug_aliases.csv"           # optional
EXTERNAL_IDS = DATA_DIR / "show_external_ids.csv"      # imdb ids live here
IMDB_SUMMARY = DATA_DIR / "imdb_show_summary.csv"      # optional, created by another script

OUT_INDEX = DATA_DIR / "shows_index.csv"

MASTER_RE = re.compile(r"^(?P<slug>.+)_master\.csv$", re.IGNORECASE)


def list_master_slugs(data_dir: Path) -> list[str]:
    """
    Canonical masters are YouTube masters ONLY: data/<slug>_master.csv
    Exclude source masters like <slug>_imdb_episodes_master.csv.
    """
    slugs = []
    for p in data_dir.glob("*_master.csv"):
        name = p.name.lower()

        # Exclude non-YouTube masters (source masters)
        if name.endswith("_imdb_episodes_master.csv"):
            continue
        if name.endswith("_reddit_posts_master.csv"):
            continue
        if name.endswith("_reddit_daily_master.csv"):
            continue

        # Accept only true YouTube masters: <slug>_master.csv
        # (This also implicitly excludes *_something_master.csv if you add more later)
        if name.count("_master.csv") != 1:
            continue
        # Extra guard: reject anything that has another "_..._master.csv" pattern
        if name.endswith("_master.csv") and "_imdb_" in name:
            continue

        slug = name[:-len("_master.csv")]
        slugs.append(slug)

    return sorted(set(slugs))



def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def build_slug_alias_map() -> dict[str, str]:
    df = read_csv_if_exists(SLUG_ALIASES)
    if df.empty:
        return {}
    if "old_slug" not in df.columns or "slug" not in df.columns:
        raise ValueError("slug_aliases.csv must include columns: old_slug, slug")
    out = {}
    for _, r in df.iterrows():
        old_slug = str(r["old_slug"]).strip().lower()
        new_slug = str(r["slug"]).strip().lower()
        if old_slug and new_slug:
            out[old_slug] = new_slug
    return out


def normalize_slug(s: str, alias_map: dict[str, str]) -> str:
    base = str(s).strip().lower()
    return alias_map.get(base, base)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build data/shows_index.csv from canonical masters and enrichment files.")
    parser.add_argument("--include_youtube_rollups", action="store_true",
                        help="Compute basic YouTube rollups (slower). Defaults to off.")
    args = parser.parse_args()

    canonical_slugs = list_master_slugs(DATA_DIR)
    print(f"Canonical YouTube masters detected: {len(canonical_slugs)}")
    for s in canonical_slugs[:10]:
        print(f"  - {s}")
    if len(canonical_slugs) > 10:
        print("  ...")   
   
    if not canonical_slugs:
        raise SystemExit("No canonical masters found at data/<slug>_master.csv")

    alias_map = build_slug_alias_map()

    # Base index is canonical slug list
    index = pd.DataFrame({"slug": canonical_slugs})

    # Enrich from shows_meta.csv (if present)
    meta = read_csv_if_exists(SHOWS_META)
    if not meta.empty:
        # Find a slug column in shows_meta (support a couple common names)
        slug_col = None
        for c in ["slug", "show_slug", "show", "id"]:
            if c in meta.columns:
                slug_col = c
                break

        if slug_col is not None:
            meta = meta.copy()
            meta["slug_norm"] = meta[slug_col].apply(lambda x: normalize_slug(x, alias_map))

            # Drop duplicate meta rows per slug_norm, keep first
            meta = meta.drop_duplicates(subset=["slug_norm"], keep="first")

            # IMPORTANT: drop the original slug column BEFORE renaming to avoid collisions
            meta = meta.drop(columns=[slug_col], errors="ignore")

            # Rename slug_norm to slug for joining
            meta = meta.rename(columns={"slug_norm": "slug"})

            # Join
            index = index.merge(meta, on="slug", how="left")
        else:
            print("Warning: shows_meta.csv exists but no recognizable slug column found, skipping join.")
            
    # Coverage based on whether ANY meta column joined in
    joined_cols = [c for c in meta.columns if c != "slug"]
    if joined_cols:
        any_meta = index[joined_cols].notna().any(axis=1).sum()
        print(f"shows_meta coverage: {any_meta}/{len(index)} slugs had at least one meta field")



    # Enrich from show_external_ids.csv (IMDb title id is required for ingest)
    external = read_csv_if_exists(EXTERNAL_IDS)
    if not external.empty:
        if "slug" not in external.columns or "imdb_title_id" not in external.columns:
            raise ValueError("show_external_ids.csv must include columns: slug, imdb_title_id")
        external = external.copy()
        external["slug"] = external["slug"].apply(lambda x: normalize_slug(x, alias_map))
        external = external.drop_duplicates(subset=["slug"], keep="first")
        index = index.merge(external, on="slug", how="left", suffixes=("", "_external"))

    # Enrich from imdb_show_summary.csv if present
    imdb_sum = read_csv_if_exists(IMDB_SUMMARY)
    if not imdb_sum.empty:
        if "slug" not in imdb_sum.columns:
            raise ValueError("imdb_show_summary.csv must include column: slug")
        imdb_sum = imdb_sum.copy()
        imdb_sum["slug"] = imdb_sum["slug"].apply(lambda x: normalize_slug(x, alias_map))
        imdb_sum = imdb_sum.drop_duplicates(subset=["slug"], keep="first")
        index = index.merge(imdb_sum, on="slug", how="left", suffixes=("", "_imdb"))

    # Optional: compute lightweight YouTube rollups from each canonical master
    if args.include_youtube_rollups:
        rollups = []
        for i, slug in enumerate(canonical_slugs, start=1):
            master_path = DATA_DIR / f"{slug}_master.csv"
            try:
                df = pd.read_csv(master_path, low_memory=False)
                # Try common view columns; tolerate missing.
                views_col = None
                for c in ["view_count", "views", "viewCount"]:
                    if c in df.columns:
                        views_col = c
                        break
                if views_col is not None:
                    views = pd.to_numeric(df[views_col], errors="coerce").fillna(0)
                    total_views = int(views.sum())
                    max_views = int(views.max()) if len(views) else 0
                else:
                    total_views, max_views = 0, 0

                rollups.append({
                    "slug": slug,
                    "yt_master_rows": int(len(df)),
                    "yt_total_views": total_views,
                    "yt_max_video_views": max_views,
                })
                print(f"[{i}/{len(canonical_slugs)}] rollup slug={slug} rows={len(df)}")
            except Exception as e:
                rollups.append({"slug": slug, "yt_master_rows": "", "yt_total_views": "", "yt_max_video_views": ""})
                print(f"[{i}/{len(canonical_slugs)}] rollup slug={slug} failed: {e}")

        rollups_df = pd.DataFrame(rollups)
        index = index.merge(rollups_df, on="slug", how="left")

    # Write
    OUT_INDEX.parent.mkdir(parents=True, exist_ok=True)
    index.to_csv(OUT_INDEX, index=False)
    print(f"Wrote {len(index)} rows to {OUT_INDEX}")


if __name__ == "__main__":
    main()
