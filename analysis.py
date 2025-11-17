# analysis.py

import pandas as pd
from typing import Dict


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df[df["view_count"].notna()]
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce")
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce")

    df = df.dropna(subset=["view_count"])
    df["view_count"] = df["view_count"].astype(int)

    return df


def summarize_engagement(df: pd.DataFrame) -> Dict[str, int]:
    total_views = int(df["view_count"].sum())
    avg_views = float(df["view_count"].mean()) if not df.empty else 0.0
    video_count = int(len(df))

    return {
        "video_count": video_count,
        "total_views": total_views,
        "avg_views": avg_views,
    }


def channel_counts(df: pd.DataFrame, top_n: int = 20) -> pd.Series:
    return df["channel_title"].value_counts().head(top_n)


def channel_views(df: pd.DataFrame, top_n: int = 20) -> pd.Series:
    return (
        df.groupby("channel_title")["view_count"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
    )


def channel_aggregates(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """
    Return a DataFrame with one row per channel:

    - channel_title
    - channel_id (first seen)
    - video_count
    - total_views
    - channel_url
    """

    # Aggregate by channel_title and channel_id
    grouped = (
        df.groupby(["channel_title", "channel_id"])
        .agg(
            video_count=("video_id", "count"),
            total_views=("view_count", "sum"),
        )
        .reset_index()
    )

    # Sort by total_views descending and keep top N
    grouped = grouped.sort_values("total_views", ascending=False).head(top_n)

    # Build channel URL
    grouped["channel_url"] = grouped["channel_id"].apply(
        lambda cid: f"https://www.youtube.com/channel/{cid}" if cid else ""
    )

    return grouped
