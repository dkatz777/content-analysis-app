import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from googleapiclient.discovery import build


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Default location for CSV snapshots
DATA_DIR = Path("data")


def get_youtube_client(api_key: str | None = None):
    """
    Return an authenticated YouTube API client.

    Reads YOUTUBE_API_KEY from the environment if api_key is not provided.
    """
    key = api_key or os.getenv("YOUTUBE_API_KEY")
    if not key:
        raise ValueError("YouTube API key not found in environment.")

    youtube = build(
        YOUTUBE_API_SERVICE_NAME,
        YOUTUBE_API_VERSION,
        developerKey=key,
    )
    return youtube


def youtube_search(
    query: str,
    max_results: int = 600,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Search YouTube for videos matching `query` and return a DataFrame of results.

    This is the core discovery function.
    It:
      - Uses YouTube's search endpoint with order=viewCount.
      - Paginates until it reaches max_results or runs out of pages.
      - For each page of search results, calls videos.list to fetch rich fields.

    It does not:
      - Write any files.
      - Know anything about masters or snapshots.

    Master and snapshot handling happens outside this function.
    """
    youtube = get_youtube_client(api_key=api_key)

    videos: List[Dict] = []
    video_ids_set = set()

    # Initial search
    search_response = youtube.search().list(
        q=query,
        part="id,snippet",
        maxResults=50,
        type="video",
        order="viewCount",
    ).execute()

    while search_response and len(videos) < max_results:
        items = search_response.get("items", [])
        new_video_ids = [
            item["id"]["videoId"]
            for item in items
            if item.get("id", {}).get("videoId") not in video_ids_set
        ]

        if not new_video_ids:
            # No new IDs on this page, break to avoid loops
            break

        video_ids_set.update(new_video_ids)

        video_response = youtube.videos().list(
            id=",".join(new_video_ids),
            part="id,snippet,statistics,contentDetails,status",
        ).execute()

        for video in video_response.get("items", []):
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})
            status = video.get("status", {})

            videos.append(
                {
                    "title": snippet.get("title", ""),
                    "video_id": video.get("id"),
                    "channel_title": snippet.get("channelTitle", ""),
                    "channel_id": snippet.get("channelId", ""),
                    "publish_time": snippet.get("publishedAt"),
                    "description": snippet.get("description", ""),
                    "tags": ",".join(snippet.get("tags", [])) if snippet.get("tags") else "",
                    "category_id": snippet.get("categoryId"),
                    "view_count": statistics.get("viewCount"),
                    "like_count": statistics.get("likeCount"),
                    "comment_count": statistics.get("commentCount"),
                    "duration": content_details.get("duration"),
                    "definition": content_details.get("definition", "standard"),
                    "privacy_status": status.get("privacyStatus"),
                }
            )

            if len(videos) >= max_results:
                break

        if len(videos) >= max_results:
            break

        # Next page
        next_page_token = search_response.get("nextPageToken")
        if next_page_token and len(videos) < max_results:
            search_response = youtube.search().list(
                q=query,
                part="id,snippet",
                maxResults=50,
                type="video",
                order="viewCount",
                pageToken=next_page_token,
            ).execute()
        else:
            break

    df = pd.DataFrame(videos)
    if df.empty:
        return df

    # Normalize numeric fields
    for col in ["view_count", "like_count", "comment_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.head(max_results)


def fetch_video_details(
    video_ids: List[str],
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch rich details for an arbitrary list of video_ids.

    This is a reusable building block for:
      - Backfilling channel_ids in master files.
      - Periodic stats refresh for all videos in a master, independent of search.

    Returns a DataFrame with the same column structure as youtube_search.
    """
    if not video_ids:
        return pd.DataFrame()

    youtube = get_youtube_client(api_key=api_key)

    all_videos: List[Dict] = []
    unique_ids = list({vid for vid in video_ids if vid})

    # YouTube API allows up to 50 IDs per videos.list call
    for idx in range(0, len(unique_ids), 50):
        batch_ids = unique_ids[idx : idx + 50]

        video_response = youtube.videos().list(
            id=",".join(batch_ids),
            part="id,snippet,statistics,contentDetails,status",
        ).execute()

        for video in video_response.get("items", []):
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})
            status = video.get("status", {})

            all_videos.append(
                {
                    "title": snippet.get("title", ""),
                    "video_id": video.get("id"),
                    "channel_title": snippet.get("channelTitle", ""),
                    "channel_id": snippet.get("channelId", ""),
                    "publish_time": snippet.get("publishedAt"),
                    "description": snippet.get("description", ""),
                    "tags": ",".join(snippet.get("tags", [])) if snippet.get("tags") else "",
                    "category_id": snippet.get("categoryId"),
                    "view_count": statistics.get("viewCount"),
                    "like_count": statistics.get("likeCount"),
                    "comment_count": statistics.get("commentCount"),
                    "duration": content_details.get("duration"),
                    "definition": content_details.get("definition", "standard"),
                    "privacy_status": status.get("privacyStatus"),
                }
            )

    df = pd.DataFrame(all_videos)
    if df.empty:
        return df

    for col in ["view_count", "like_count", "comment_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def build_snapshot_filename(
    slug: str,
    kind: str = "raw",
    ts: Optional[datetime] = None,
) -> str:
    """
    Build a standardized snapshot filename for a given show slug.

    Examples:
      build_snapshot_filename("the_sopranos") ->
        "the_sopranos_20251201_raw.csv"
      build_snapshot_filename("the_office", kind="clean") ->
        "the_office_20251201_clean.csv"
    """
    ts = ts or datetime.utcnow()
    date_part = ts.strftime("%Y%m%d")
    return f"{slug}_{date_part}_{kind}.csv"


def run_search_snapshot(
    slug: str,
    query: str,
    max_results: int = 600,
    api_key: Optional[str] = None,
    kind: str = "raw",
    output_dir: Path | str = DATA_DIR,
) -> Path:
    """
    Run a YouTube search for a given show slug and persist a snapshot CSV.

    This is the entry point for your "snapshot" step in the pipeline.

    It:
      - Calls youtube_search(query).
      - Writes the DataFrame to data/{slug}_{YYYYMMDD}_{kind}.csv by default.
      - Returns the Path to the written file.

    It does not touch any master files. Masters are updated by the merge script.
    """
    df = youtube_search(query=query, max_results=max_results, api_key=api_key)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = build_snapshot_filename(slug=slug, kind=kind)
    output_path = output_dir / filename

    df.to_csv(output_path, index=False)

    return output_path
