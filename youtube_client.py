# youtube_client.py

import os
from typing import List, Dict, Optional

import pandas as pd
from googleapiclient.discovery import build


YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def get_youtube_client(api_key: Optional[str] = None):
    """
    Create a YouTube API client using the provided key or the YOUTUBE_API_KEY env var.
    """
    key = api_key or os.getenv("API_KEY")
    if not key:
        raise ValueError(
            "YouTube API key not found. Set the YOUTUBE_API_KEY environment variable "
            "or pass api_key explicitly."
        )

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
    Sorted by view count on the YouTube side.

    This function does not write any files. It just returns data.
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
        new_video_ids = [
            item["id"]["videoId"]
            for item in search_response.get("items", [])
            if item["id"]["videoId"] not in video_ids_set
        ]

        video_ids_set.update(new_video_ids)

        if new_video_ids:
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
                        "publish_time": snippet.get("publishedAt"),
                        "description": snippet.get("description", ""),
                        "tags": ",".join(snippet.get("tags", [])),
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
    return df.head(max_results)
