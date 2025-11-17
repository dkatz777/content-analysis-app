# app.py

import io
import os

import streamlit as st
import pandas as pd
import altair as alt  

from youtube_client import youtube_search
from analysis import (
    clean_dataframe,
    summarize_engagement,
    channel_aggregates,  
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

    # Make title clickable using video_id
    def make_title_link(row):
        url = f"https://www.youtube.com/watch?v={row['video_id']}"
        title = row["title"]
        return f'<a href="{url}" target="_blank">{title}</a>'

    df_display["title_link"] = df_display.apply(make_title_link, axis=1)

    st.subheader(f"Video table for {label}")
    
    # Build a display DataFrame for the interactive table
    table_df = df.copy()
    
    # Build a clickable URL column
    table_df["Video URL"] = table_df["video_id"].apply(
        lambda vid: f"https://www.youtube.com/watch?v={vid}"
    )
    
    # Rename columns for display
    table_df = table_df.rename(
        columns={
            "title": "Title",
            "channel_title": "Channel",
            "publish_time": "Published",
            "view_count": "Views",
        }
    )
    
    # Choose and order columns for the table
    display_cols = ["Title", "Channel", "Views", "Published", "Video URL"]
    table_df = table_df[display_cols]
    
    # Use Styler to format Views with commas, but keep it numeric for sorting
    styled = table_df.style.format({"Views": "{:,}"})
    
    st.dataframe(
        styled,
        use_container_width=True,
        height=400,
    )



    # Channel aggregates for chart and links
    st.subheader("Top channels")

    chart_mode = st.radio(
        "Show channels by",
        ["Number of videos", "Total views"],
        horizontal=True,
    )

    chan_df = channel_aggregates(df, top_n=20)

    if chart_mode == "Number of videos":
        value_col = "video_count"
        value_label = "Videos"
    else:
        value_col = "total_views"
        value_label = "Views"

    # Horizontal Altair bar chart, sorted high to low, clickable bars
    chart = (
        alt.Chart(chan_df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "channel_title:N",
                sort=alt.SortField(field=value_col, order="descending"),
                title="Channel",
            ),
            x=alt.X(
                f"{value_col}:Q",
                title=value_label,
            ),
            tooltip=[
                "channel_title",
                "video_count",
                alt.Tooltip("total_views:Q", format=",.0f", title="Total views"),
            ],
            href="channel_url:N",
        )
        .properties(height=400)
    )

    st.altair_chart(chart, use_container_width=True)

    # Optional: table of channels with clickable names
    st.markdown("**Channel links**")
    chan_table = chan_df.copy()
    chan_table["Channel"] = chan_table.apply(
        lambda row: f'<a href="{row["channel_url"]}" target="_blank">{row["channel_title"]}</a>',
        axis=1,
    )
    chan_table["Total views"] = chan_table["total_views"].apply(lambda x: f"{x:,}")
    chan_table["Videos"] = chan_table["video_count"]

    st.write(
        chan_table[["Channel", "Videos", "Total views"]].to_html(
            escape=False, index=False
        ),
        unsafe_allow_html=True,
    )

    # Allow CSV download of the raw (cleaned) data
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Download CSV",
        data=csv_buffer.getvalue(),
        file_name=f"{label}_youtube_results.csv",
        mime="text/csv",
        key=f"download_csv_{label}",
    )



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
