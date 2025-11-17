# analysis.py

import pandas as pd
from typing import Dict


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw YouTube DataFrame:
      - Drop rows with missing view_count
      - Convert numeric columns to integers where possible
    """
    df = df.copy()

    # Filter out rows with missing or non numeric view_count
    df = df[df["view_count"].notna()]

    # Some values come as strings
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce")
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce")

    df = df.dropna(subset=["view_count"])
    df["view_count"] = df["view_count"].astype(int)

    return df


def summarize_engagement(df: pd.DataFrame) -> Dict[str, int]:
    """
    Compute high level engagement stats from a cleaned DataFrame.
    """
    total_views = int(df["view_count"].sum())
    avg_views = float(df["view_count"].mean()) if not df.empty else 0.0
    video_count = int(len(df))

    return {
        "video_count": video_count,
        "total_views": total_views,
        "avg_views": avg_views,
    }


def channel_counts(df: pd.DataFrame, top_n: int = 20) -> pd.Series:
    """
    Count videos per channel. Returns a Series indexed by channel_title.
    """
    return df["channel_title"].value_counts().head(top_n)
