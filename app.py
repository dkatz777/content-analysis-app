# app.py

import io
import os

import streamlit as st
import pandas as pd
import altair as alt  

#For locally run version, load .env for API keys and such
from dotenv import load_dotenv
load_dotenv() # loads .env into environment variables (local dev)

def get_youtube_api_key() -> str | None:
    """
    Load the YouTube API key from:
      1. Environment / .env (local dev)
      2. Streamlit Cloud secrets (if available)

    Must not crash locally if Streamlit secrets are not configured.
    """

    # 1. Start with env / .env
    api_key = os.getenv("YOUTUBE_API_KEY")

    # 2. Try Streamlit secrets, but guard with try/except so local does not crash
    try:
        # Accessing st.secrets at all can raise StreamlitSecretNotFoundError locally,
        # so keep it inside try.
        secrets_obj = st.secrets
        if "YOUTUBE_API_KEY" in secrets_obj:
            api_key = secrets_obj["YOUTUBE_API_KEY"]
    except Exception:
        # No secrets configured (local dev). Ignore and just use env/.env.
        pass

    return api_key


API_KEY = get_youtube_api_key()

if not API_KEY:
    st.error(
        "YouTube API key not found.\n\n"
        "Locally: create a .env file with `YOUTUBE_API_KEY=...`.\n"
        "On Streamlit Cloud: add YOUTUBE_API_KEY to the app secrets."
    )
    st.stop()

# Make sure downstream code that reads from the environment sees the key
os.environ["YOUTUBE_API_KEY"] = API_KEY



from youtube_client import youtube_search
from analysis import (
    clean_dataframe,
    summarize_engagement,
    channel_aggregates,  
)
from library import load_library, load_show_df, slugify_show_name


st.set_page_config(page_title="YouTube TV Analysis", layout="wide")



# Set view state to allow home and show navigation
if "view" not in st.session_state:
    st.session_state["view"] = "home"

if "current_show_label" not in st.session_state:
    st.session_state["current_show_label"] = None

if "current_show_df" not in st.session_state:
    st.session_state["current_show_df"] = None

# Add helper functions to navigate
def go_home():
    st.session_state["view"] = "home"
    st.rerun()


def open_show(label, df):
    st.session_state["current_show_label"] = label
    st.session_state["current_show_df"] = df
    st.session_state["view"] = "show"
    st.rerun()

# Load library of precomputed shows
library = load_library()
library_slugs = {item["slug"] for item in library}


#Show Title view with Dashboard and home button
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
        width='stretch',
        height=400,
    )



    # Channel aggregates for chart and links
    st.subheader("Top channels")

    chart_mode = st.radio(
        "Show channels by",
        ["Total views", "Number of videos"],
        horizontal=True,
    )

    chan_df = channel_aggregates(df, top_n=20)

    if chart_mode == "Total views":
        value_col = "total_views"
        value_label = "Views"
        
    else:
        value_col = "video_count"
        value_label = "Videos"

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

    st.altair_chart(chart, width='stretch')

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
        file_name=f"{label}",
        mime="text/csv",
    )

def render_home(library):
    st.title("YouTube TV Show Analysis")

    st.markdown("Select an existing show or run a new search.")

    # 1. New search form
    st.subheader("Run a new search")

    default_query = '"Beverly Hills 90210"'
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
        library_slugs = {item["slug"] for item in library}

        if slug in library_slugs:
            st.warning(
                "This show already exists in the library. Loading the saved version instead."
            )
            existing = next(item for item in library if item["slug"] == slug)
            df_saved = load_show_df(existing["path"])
            open_show(existing["display_name"], df_saved)
        else:
            with st.spinner("Querying YouTube API..."):
                try:
                    df_raw = youtube_search(query=query, max_results=max_results)
                except Exception as e:
                    st.error(f"Error while calling YouTube API: {e}")
                    return

            if df_raw is not None and not df_raw.empty:
                st.success("Search complete.")
                # Go straight to the show view with this data
                open_show(query, df_raw)
            else:
                st.warning("No results found for that search.")

    # 2. Library view
    st.subheader("Existing shows")

    if not library:
        st.info("No shows in the library yet. Use the form below to run a new search.")
    else:
        cols = st.columns(3)
        for idx, show in enumerate(library):
            col = cols[idx % 3]
            with col:
                # Show key art if we have it
                if show.get("image_path"):
                    st.image(
                        show["image_path"],
                        width=250,  # tweak if you want larger/smaller
                    )
                
                # Title as the clickable element that opens the show page
                if st.button(
                    show["display_name"],
                    key=f"open_{show['slug']}",
                ):
                    df_saved = load_show_df(show["path"])
                    open_show(show["display_name"], df_saved)

    st.markdown("---")

    


def render_show_page():
    label = st.session_state.get("current_show_label")
    df = st.session_state.get("current_show_df")

    if df is None or label is None:
        st.warning("No show selected. Returning to home.")
        go_home()
        return

    # Back button at the top
    if st.button("← Back to home"):
        go_home()
        return

    # Page header
    st.title(label)

    # Show the dashboard for this show
    show_dashboard(df, label)

# Load library once at top level
library = load_library()

# Simple router
if st.session_state["view"] == "home":
    render_home(library)
elif st.session_state["view"] == "show":
    render_show_page()

