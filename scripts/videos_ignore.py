# scripts/video_ignore.py

from __future__ import annotations

from pathlib import Path
from datetime import date
import pandas as pd

# Project root is one level up from this script
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
VIDEOS_IGNORE_PATH = DATA_DIR / "videos_ignore.csv"


def load_ignore_list() -> pd.DataFrame:
    """
    Load the global ignore list.

    Columns:
      slug         show slug, for example "the_amazing_race"
      video_id
      channel_id   optional
      title        optional
      reason       optional text
      first_marked_at  ISO date string
    """
    if VIDEOS_IGNORE_PATH.exists():
        df = pd.read_csv(VIDEOS_IGNORE_PATH)
        # Ensure required columns exist
        for col in ["slug", "video_id", "channel_id", "title", "reason", "first_marked_at"]:
            if col not in df.columns:
                df[col] = ""
        return df

    return pd.DataFrame(
        columns=["slug", "video_id", "channel_id", "title", "reason", "first_marked_at"]
    )


def append_ignores_for_show(
    slug: str,
    rows_df: pd.DataFrame,
    reason: str = "user_marked_in_ui",
) -> None:
    """
    Append a set of videos to the global ignore list for a given show.

    rows_df should have at least:
      - video_id
      and ideally:
      - channel_id
      - title
    """
    if rows_df.empty:
        return

    ignore_df = load_ignore_list()
    today = date.today().isoformat()

    new_rows = pd.DataFrame(
        {
            "slug": slug,
            "video_id": rows_df["video_id"],
            "channel_id": rows_df.get("channel_id", ""),
            "title": rows_df.get("title", ""),
            "reason": reason,
            "first_marked_at": today,
        }
    )

    combined = pd.concat([ignore_df, new_rows], ignore_index=True)

    # De duplicate on (slug, video_id), keep the first entry
    combined = combined.drop_duplicates(subset=["slug", "video_id"], keep="first").reset_index(
        drop=True
    )

    combined.to_csv(VIDEOS_IGNORE_PATH, index=False)
    print(f"Video ignore list updated at {VIDEOS_IGNORE_PATH}")


def filter_master_for_show(slug: str, master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove any videos from a master DataFrame that are on the ignore list for that slug.
    """
    ignore_df = load_ignore_list()
    if ignore_df.empty:
        return master_df

    bad_ids = (
        ignore_df.loc[ignore_df["slug"] == slug, "video_id"]
        .dropna()
        .astype(str)
        .unique()
    )

    if not len(bad_ids):
        return master_df

    filtered = master_df[~master_df["video_id"].astype(str).isin(bad_ids)].copy()
    print(
        f"Filtered master for slug '{slug}': "
        f"{len(master_df)} rows before, {len(filtered)} rows after ignore filtering"
    )
    return filtered
