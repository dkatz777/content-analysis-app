# app.py

import io

import streamlit as st
import pandas as pd

from youtube_client import youtube_search
from analysis import clean_dataframe, summarize_engagement, channel_counts


st.set_page_config(page_title="YouTube TV Analysis", layout="wide")

st.title("YouTube TV Show Analysis Prototype")

st.markdown(
    "Enter a TV show search term. The app will pull the top videos by view count "
    "and compute some basic stats."
)

query = st.text_input("Search term", value='"naked and afraid"')
max_results = st.number_input("Max results", min_value=50, max_value=600, value=200, step=50)

run_button = st.button("Run search")

if run_button and query:
    with st.spinner("Querying YouTube API..."):
        try:
            df_raw = youtube_search(query=query, max_results=max_results)
        except Exception as e:
            st.error(f"Error while calling YouTube API: {e}")
            st.stop()

    if df_raw.empty:
        st.warning("No results found.")
        st.stop()

    df = clean_dataframe(df_raw)

    # Summary metrics
    summary = summarize_engagement(df)
    col1, col2, col3 = st.columns(3)
    col1.metric("Videos", summary["video_count"])
    col2.metric("Total views", f'{summary["total_views"]:,}')
    col3.metric("Average views", f'{summary["avg_views"]:,.0f}')

    st.subheader("Video table")
    st.dataframe(df[["title", "channel_title", "view_count", "publish_time", "video_id"]])

    st.subheader("Top channels by video count")
    st.bar_chart(channel_counts(df))

    # CSV download
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Download CSV",
        data=csv_buffer.getvalue(),
        file_name="youtube_results.csv",
        mime="text/csv",
    )
