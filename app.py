# app.py

import io
import os

import streamlit as st
import pandas as pd

from youtube_client import youtube_search
from analysis import (
    clean_dataframe,
    summarize_engagement,
    channel_counts,
    channel_views,
)
from library import load_library, load_show_df, slugify_show_name


st.set_page_config(page_title="YouTube TV Analysis", layout="wide")

# Make sure the API key from Streamlit secrets is available
if "YOUTUBE_API_KEY" in st.secrets:
    os.environ["YOUTUBE_API_KEY"] = st.secrets["YOUTUBE_API_KEY"]

# Load library of precomputed shows
library = load_library()
library_slugs = {item["slug"] for item in library}


def show_dashboard(df: pd.DataFrame, label: str):
    """
    Shared dashboard view, whether the data came from a saved CSV or a fresh search.
    """

    df = clean_dataframe(df)
    if df.empty:
        st.warning(f"No usable data for {label}.")
        return

    # Summary metrics
    summary = summarize_engagement(df)
    col1, col2, col3 = st.columns(3)
    col1.metric("Videos", summary["video_count"])
    col2.metric("Total views", f'{summary["total_views"]:,}')
    col3.metric("Average views", f'{summary["avg_views"]:,.0f}')

    # Build display table with formatted view counts and clickable titles
    df_display = df.copy()

    # Format view counts with commas
    df_display["view_count_formatted"] = df_display["view_count"].apply(
        lambda x: f"{x:,}"
    )

    # Make title clickable, using video_id to build the URL
    def make_title_link(row):
        url = f"https://www.youtube.com/watch?v={row['video_id']}"
        title = row["title"]
        return f'<a href="{url}" target="_blank">{title}</a>'

    df_display["title_link"] = df_display.apply(make_title_link, axis=1)

    st.subheader(f"Video table for {label}")

    # Show a clean HTML table so links are clickable and counts are formatted
    table_cols = [
        "title_link",
        "channel_title",
        "view_count_formatted",
        "publish_time",
    ]
    table_df = df_display[table_cols].rename(
        columns={
            "title_link": "Title",
            "channel_title": "Channel",
            "view_count_formatted": "Views",
            "publish_time": "Published",
        }
    )

    st.write(
        table_df.to_html(escape=False, index=False),
        unsafe_allow_html=True,
    )

    # Channel bar chart with toggle
    st.subheader("Top channels")

    chart_mode = st.radio(
        "Show channels by",
        ["Number of videos", "Total views"],
        horizontal=True,
    )

    if chart_mode == "Number of videos":
        series = channel_counts(df)
        y_label = "Videos"
    else:
        series = channel_views(df)
        y_label = "Views"

    # Build a DataFrame for the chart
    chart_df = series.to_frame(name=y_label)

    st.bar_chart(chart_df)

    # Allow CSV download of the raw (cleaned) data
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Download CSV",
        data=csv_buffer.getvalue(),
        file_name=f"{label}_youtube_results.csv",
        mime="text/csv",
    )



st.title("YouTube TV Show Analysis")

st.markdown("Select an existing show or run a new search.")


# 1. Library view
st.subheader("Existing shows")

if not library:
    st.info("No shows in the library yet. Use the form below to run a new search.")
else:
    cols = st.columns(3)
    for idx, show in enumerate(library):
        col = cols[idx % 3]
        with col:
            st.markdown(f"**{show['display_name']}**")
            # In the future, you can add st.image(show['poster_url']) here
            if st.button("Open", key=f"open_{show['slug']}"):
                df_saved = load_show_df(show["path"])
                st.session_state["current_show_label"] = show["display_name"]
                st.session_state["current_show_df"] = df_saved


st.markdown("---")

# 2. New search form
st.subheader("Run a new search")

default_query = '"naked and afraid"'
query = st.text_input("Search term", value=default_query)

max_results = st.number_input(
    "Max results",
    min_value=50,
    max_value=600,
    value=600,
    step=50,
)

run_button = st.button("Fetch data")

if run_button and query:
    slug = slugify_show_name(query)

    if slug in library_slugs:
        # Prevent duplicate search and load the existing data instead
        st.warning(
            "This show already exists in the library. Loading the saved version instead."
        )
        existing = next(item for item in library if item["slug"] == slug)
        df_saved = load_show_df(existing["path"])
        st.session_state["current_show_label"] = existing["display_name"]
        st.session_state["current_show_df"] = df_saved
    else:
        with st.spinner("Querying YouTube API..."):
            try:
                df_raw = youtube_search(query=query, max_results=max_results)
            except Exception as e:
                st.error(f"Error while calling YouTube API: {e}")
                df_raw = None

        if df_raw is not None and not df_raw.empty:
            # Store in session for immediate use
            st.session_state["current_show_label"] = query
            st.session_state["current_show_df"] = df_raw

            st.success("Search complete. Data is available in this session.")
            st.info(
                "Note: To make this show part of the permanent library, "
                "you still need to save and commit its CSV into the data/ folder."
            )


# 3. Dashboard for the selected or freshly fetched show
if "current_show_df" in st.session_state:
    st.markdown("---")
    label = st.session_state.get("current_show_label", "Selected show")
    show_dashboard(st.session_state["current_show_df"], label)
