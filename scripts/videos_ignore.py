from pathlib import Path
from datetime import date
import pandas as pd

DATA_DIR = Path("data")
IGNORE_PATH = DATA_DIR / "video_ignore_list.csv"


def load_ignore_list() -> pd.DataFrame:
    if IGNORE_PATH.exists():
        return pd.read_csv(IGNORE_PATH)
    return pd.DataFrame(
        columns=["slug", "video_id", "channel_id", "title", "reason", "first_marked_at"]
    )


def append_rejected_for_show(
    slug: str,
    rejected_path: Path,
    reason: str = "not_show_related",
) -> None:
    """
    Append rejected videos for a show into the global ignore list.

    rejected_path: CSV with at least video_id, and ideally title, channel_id
    """
    rejected_df = pd.read_csv(rejected_path)

    if "video_id" not in rejected_df.columns:
        raise ValueError("Rejected CSV must have a 'video_id' column")

    ignore_df = load_ignore_list()

    today = date.today().isoformat()

    # Build new entries
    new_rows = pd.DataFrame(
        {
            "slug": slug,
            "video_id": rejected_df["video_id"],
            "channel_id": rejected_df.get("channel_id", ""),
            "title": rejected_df.get("title", ""),
            "reason": reason,
            "first_marked_at": today,
        }
    )

    combined = pd.concat([ignore_df, new_rows], ignore_index=True)

    # Drop duplicates on (slug, video_id), keep the first time you saw it
    combined = (
        combined.drop_duplicates(subset=["slug", "video_id"], keep="first")
        .reset_index(drop=True)
    )

    combined.to_csv(IGNORE_PATH, index=False)
    print(f"Updated ignore list written to {IGNORE_PATH}")


def filter_snapshot_with_ignore_list(slug: str, snapshot_df: pd.DataFrame) -> pd.DataFrame:
    """Return a snapshot with ignored videos removed for this slug."""
    ignore_df = load_ignore_list()
    if ignore_df.empty:
        return snapshot_df

    ignore_ids = ignore_df.loc[ignore_df["slug"] == slug, "video_id"].dropna().unique()
    filtered = snapshot_df[~snapshot_df["video_id"].isin(ignore_ids)].copy()
    return filtered
