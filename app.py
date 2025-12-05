import io
import os
from pathlib import Path

import streamlit as st
import pandas as pd
import altair as alt  

# For locally run version, load .env for API keys and such
from dotenv import load_dotenv
load_dotenv()  # loads .env into environment variables (local dev)

from youtube_client import run_search_snapshot, build_snapshot_filename
from analysis import (
    clean_dataframe,
    summarize_engagement,
    channel_aggregates,
)
from library import load_library, load_show_df, slugify_show_name
from scripts.merge_shows_csvs import merge_for_show, DATA_DIR
from scripts.videos_ignore import load_ignore_list, append_ignores_for_show, filter_master_for_show
from scripts.channel_annotations import load_channel_annotations, upsert_channel_annotations



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


st.set_page_config(page_title="YouTube TV Analysis", layout="wide")


# Set view state to allow home and show navigation
if "view" not in st.session_state:
    st.session_state["view"] = "home"

if "current_show_label" not in st.session_state:
    st.session_state["current_show_label"] = None

if "current_show_df" not in st.session_state:
    st.session_state["current_show_df"] = None
    
if "current_show_slug" not in st.session_state:
    st.session_state["current_show_slug"] = None



# Add helper functions to navigate
def go_home():
    st.session_state["view"] = "home"
    st.rerun()


def open_show(label: str, slug: str, df: pd.DataFrame) -> None:
    st.session_state["current_show_label"] = label
    st.session_state["current_show_slug"] = slug
    st.session_state["current_show_df"] = df
    st.session_state["view"] = "show"
    st.rerun()


# Load library of precomputed shows (these should map to per show master CSVs)
library = load_library()
library_slugs = {item["slug"] for item in library}



def render_channel_ugc_panel(
    chan_df: pd.DataFrame,
    channel_annotations: pd.DataFrame | None,
) -> None:
    """
    UI panel to mark channels as UGC or not UGC using checkboxes.
    Works on the top N channels passed in chan_df.
    """
    if chan_df.empty:
        return

    with st.expander("Mark channels as UGC"):
        st.markdown(
            "Use the checkboxes to mark which channels are UGC. "
            "These flags are saved globally and used to compute UGC view share."
        )

        # Build annotation map from channel_annotations
        if channel_annotations is None or channel_annotations.empty:
            ann_map = {}
        else:
            ann = (
                channel_annotations[["channel_id", "ugc"]]
                .drop_duplicates(subset=["channel_id"], keep="last")
            )
            ann_map = {
                str(row["channel_id"]): bool(row["ugc"])
                for _, row in ann.iterrows()
            }

        editor_df = chan_df.copy()
        editor_df["channel_id"] = editor_df["channel_id"].astype(str)

        # Add UGC? column pre filled from annotations
        editor_df["UGC?"] = editor_df["channel_id"].map(ann_map).fillna(False)

        # Select and rename columns for editing
        editor_df = editor_df.rename(
            columns={
                "channel_title": "Channel",
                "total_views": "Total views",
                "video_count": "Videos",
                "channel_id": "Channel ID",
            }
        )

        editor_df = editor_df[["UGC?", "Channel", "Videos", "Total views", "Channel ID"]]

        edited_df = st.data_editor(
            editor_df,
            num_rows="fixed",
            hide_index=True,
            key="channel_ugc_editor",
        )

        if st.button("Save UGC flags for these channels", key="save_channel_ugc"):
            try:
                # Map back to canonical names
                result_df = edited_df.rename(
                    columns={
                        "UGC?": "ugc",
                        "Channel": "channel_title",
                        "Channel ID": "channel_id",
                    }
                )

                result_df["channel_id"] = result_df["channel_id"].astype(str)
                result_df["ugc"] = result_df["ugc"].astype(bool)

                rows_for_upsert = result_df[["channel_id", "channel_title", "ugc"]].copy()

                upsert_channel_annotations(rows_for_upsert)
                st.success("Channel UGC flags saved. Reloading view.")
                st.rerun()

            except Exception as e:
                st.error(f"Error saving channel UGC flags: {e}")


# UGC compute helper
def compute_ugc_from_channels(
    chan_df: pd.DataFrame,
    channel_annotations: pd.DataFrame | None,
) -> tuple[float | None, pd.DataFrame | None]:
    """
    Compute UGC view share based on channel aggregates.

    Returns:
      (ugc_pct, debug_df)

    ugc_pct: percentage of total views from channels marked as UGC.
    debug_df: channel level table with total_views and ugc flag.
    """
    if chan_df is None or chan_df.empty:
        return None, None

    if (
        channel_annotations is None
        or channel_annotations.empty
        or "channel_id" not in chan_df.columns
        or "channel_id" not in channel_annotations.columns
        or "ugc" not in channel_annotations.columns
    ):
        return None, None

    # Normalize ids
    merged = chan_df.copy()
    merged["channel_id"] = merged["channel_id"].astype(str)

    ann = (
        channel_annotations[["channel_id", "ugc"]]
        .drop_duplicates(subset=["channel_id"], keep="last")
        .copy()
    )
    ann["channel_id"] = ann["channel_id"].astype(str)

    merged = merged.merge(ann, on="channel_id", how="left")

    if "ugc" not in merged.columns:
        merged["ugc"] = False

    merged["ugc"] = merged["ugc"].fillna(False).astype(bool)

    total_views = merged["total_views"].sum()
    ugc_views = merged.loc[merged["ugc"], "total_views"].sum()

    if total_views <= 0:
        ugc_pct = 0.0
    else:
        ugc_pct = ugc_views / total_views * 100.0

    debug_df = (
        merged[["channel_title", "channel_id", "total_views", "video_count", "ugc"]]
        .sort_values("total_views", ascending=False)
        .reset_index(drop=True)
    )

    return ugc_pct, debug_df



def show_dashboard(
    df: pd.DataFrame,
    label: str,
    slug: str | None = None,
    channel_annotations: pd.DataFrame | None = None,
):
    """
    Shared dashboard view, whether the data came from a saved master CSV
    or from a fresh snapshot.
    """
    df = clean_dataframe(df)
    if df.empty:
        st.warning(f"No usable data for {label}.")
        return

    # Channel aggregates for all channels (used for UGC and for top list)
    chan_df_all = channel_aggregates(df, top_n=None)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    summary = summarize_engagement(df)
    col1.metric("Videos", summary["video_count"])
    col2.metric("Total views", f'{summary["total_views"]:,}')
    col3.metric("Average views", f'{summary["avg_views"]:,.0f}')

    # UGC view share metric based on channel aggregates
    ugc_pct, ugc_debug = compute_ugc_from_channels(chan_df_all, channel_annotations)
    if ugc_pct is not None:
        col4.metric("UGC view share", f"{ugc_pct:,.1f}%")

    # Build a display DataFrame for the interactive table
    st.subheader(f"Video table for {label}")

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
        width="stretch",
        height=400,
    )

    # Channel aggregates for chart and links (top N only)
    st.subheader("Top channels")

    chart_mode = st.radio(
        "Show channels by",
        ["Total views", "Number of videos"],
        horizontal=True,
    )

    # Top 20 channels from the full list
    chan_df = chan_df_all.head(20)

    # UGC panel for channels, uses top 20
    render_channel_ugc_panel(chan_df, channel_annotations)

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
        .properties(height=30 * len(chan_df))
    )

    st.altair_chart(chart, width="stretch")

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

    # Channel level UGC debug, aligned with channel aggregates
    if ugc_debug is not None and not ugc_debug.empty:
        with st.expander("UGC debug (views by channel and UGC flag)"):
            dbg = ugc_debug.copy()
            dbg["total_views"] = dbg["total_views"].apply(lambda x: f"{x:,}")
            st.dataframe(
                dbg.rename(columns={"total_views": "Total views"}),
                width="stretch",
                height=300,
            )

    # Allow CSV download of the raw (cleaned) data
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Download CSV",
        data=csv_buffer.getvalue(),
        file_name=f"{label}.csv",
        mime="text/csv",
    )


def render_ignore_panel(df: pd.DataFrame, slug: str) -> None:
    """
    Allow the user to mark multiple videos as ignored for this show using checkboxes.

    On apply:
      - Append selected videos to videos_ignore.csv
      - Remove ignored videos from the master CSV for this slug
      - Reload the updated master into session_state
    """
    with st.expander("Ignore videos for this show"):
        st.markdown(
            "Select videos to ignore for this show. "
            "Ignored videos will be removed from the master dataset and will be "
            "filtered out of future updates for this show."
        )

        # Load existing ignore list so we can pre-check already ignored videos
        ignore_df = load_ignore_list()
        if ignore_df.empty:
            already_ignored_ids: set[str] = set()
        else:
            already_ignored_ids = set(
                ignore_df.loc[ignore_df["slug"] == slug, "video_id"]
                .dropna()
                .astype(str)
                .unique()
            )

        editor_df = df.copy()
        editor_df["video_id"] = editor_df["video_id"].astype(str)

        # Pre mark rows that are already ignored
        editor_df["Ignore?"] = editor_df["video_id"].isin(already_ignored_ids)

        # Choose columns to show in the editor
        editor_cols = ["Ignore?", "title", "channel_title", "view_count", "video_id"]
        editor_df = editor_df[editor_cols].rename(
            columns={
                "title": "Title",
                "channel_title": "Channel",
                "view_count": "Views",
                "video_id": "Video ID",
            }
        )

        edited_df = st.data_editor(
            editor_df,
            num_rows="fixed",
            hide_index=True,
            key=f"ignore_editor_{slug}",
        )

        if st.button("Apply ignore to selected videos", key=f"apply_ignore_{slug}"):
            try:
                # Map back to canonical column names
                result_df = edited_df.rename(
                    columns={
                        "Ignore?": "ignored",
                        "Title": "title",
                        "Channel": "channel_title",
                        "Views": "view_count",
                        "Video ID": "video_id",
                    }
                )

                # Ensure string for matching
                result_df["video_id"] = result_df["video_id"].astype(str)

                # Take all rows the user checked
                selected = result_df[result_df["ignored"] == True].copy()

                if selected.empty:
                    st.info("No videos selected to ignore.")
                    return

                # Build rows_for_ignore: minimal needed fields
                rows_for_ignore = selected[["video_id", "title"]].copy()

                # Add channel_id if available in original df
                if "channel_id" in df.columns:
                    id_to_channel = (
                        df.assign(video_id=df["video_id"].astype(str))
                        .set_index("video_id")["channel_id"]
                        .to_dict()
                    )
                    rows_for_ignore["channel_id"] = rows_for_ignore["video_id"].map(
                        id_to_channel
                    )
                else:
                    rows_for_ignore["channel_id"] = ""

                # Append to ignore list (dedupe happens inside)
                append_ignores_for_show(slug=slug, rows_df=rows_for_ignore)

                # Now scrub the master CSV for this slug
                master_path = DATA_DIR / f"{slug}_master.csv"
                if master_path.exists():
                    master_df = pd.read_csv(master_path)
                    filtered_master = filter_master_for_show(slug, master_df)
                    filtered_master.to_csv(master_path, index=False)

                    # Update session so the current view is clean
                    st.session_state["current_show_df"] = filtered_master
                    st.success(
                        f"Ignored {len(selected)} videos and updated master for '{slug}'."
                    )
                    st.rerun()
                else:
                    st.warning(
                        f"Master file not found at {master_path}. "
                        "Ignore list was updated but the master CSV was not changed."
                    )

            except Exception as e:
                st.error(f"Error while applying ignore selections: {e}")


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

        if slug in library_slugs:
            st.warning(
                "This show already exists in the library. Loading the saved version instead."
            )
            existing = next(item for item in library if item["slug"] == slug)
            df_saved = load_show_df(existing["path"])
            open_show(existing["display_name"], show["slug"], df_saved)
        else:
            with st.spinner("Querying YouTube API and writing snapshot..."):
                try:
                    # Write a raw snapshot CSV using the new master/snapshot pipeline
                    snapshot_path = run_search_snapshot(
                        slug=slug,
                        query=query,
                        max_results=max_results,
                        api_key=API_KEY,
                        kind="raw",
                        output_dir=Path("data"),
                    )
                    # Load the snapshot into memory for immediate exploration
                    df_raw = pd.read_csv(snapshot_path)
                except Exception as e:
                    st.error(f"Error while calling YouTube API: {e}")
                    return

            if df_raw is not None and not df_raw.empty:
                st.success("Search complete. Snapshot saved.")
                st.info(f"Snapshot file: `{snapshot_path.name}`")
                # Go straight to the show view with this data
                open_show(query, slug, df_raw)
            else:
                st.warning("No results found for that search.")

    # 2. Library view
    st.subheader("Existing shows")

    if not library:
        st.info("No shows in the library yet. Use the form above to run a new search.")
    else:
        cols = st.columns(3)
        for idx, show in enumerate(library):
            col = cols[idx % 3]
            with col:
                # Show key art if we have it
                if show.get("image_path"):
                    st.image(
                        show["image_path"],
                        width=250,
                    )

                # Title as the clickable element that opens the show page
                if st.button(
                    show["display_name"],
                    key=f"open_{show['slug']}",
                ):
                    df_saved = load_show_df(show["path"])
                    open_show(show["display_name"], show["slug"], df_saved)

    st.markdown("---")


def render_show_page():
    label = st.session_state.get("current_show_label")
    slug = st.session_state.get("current_show_slug")
    df = st.session_state.get("current_show_df")

    if df is None or label is None or slug is None:
        st.warning("No show selected. Returning to home.")
        go_home()
        return

    # Always apply ignore filtering to the in memory df for this show
    df = filter_master_for_show(slug, df)
    st.session_state["current_show_df"] = df


    # Back button at the top
    if st.button("← Back to home"):
        go_home()
        return

    # Page header
    st.title(label)

  # Update panel
    with st.expander("Update data for this show"):
        st.markdown(
            "1. Generate a new raw snapshot from YouTube.\n\n"
            "2. Clean the CSV offline to remove non-show videos.\n\n"
            "3. Upload the cleaned snapshot to merge into the master dataset."
        )

        # Step 1: generate raw snapshot
        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("Generate raw snapshot", key="generate_snapshot"):
                try:
                    snapshot_path = run_search_snapshot(
                        slug=slug,
                        query=label,  # or a stored search_query from show_meta later
                        max_results=600,
                        api_key=API_KEY,
                        kind="raw",
                        output_dir=Path("data"),
                    )
                    st.success(f"Raw snapshot saved: {snapshot_path.name}")
                except Exception as e:
                    st.error(f"Error generating snapshot: {e}")

        with col2:
            uploaded = st.file_uploader(
                "Upload cleaned snapshot CSV", type="csv", key="upload_clean_snapshot"
            )

            if uploaded is not None:
                if st.button("Merge uploaded snapshot into master", key="merge_snapshot"):
                    try:
                        # Write uploaded file into data/ using the same naming convention
                        snapshot_filename = build_snapshot_filename(slug=slug, kind="clean")
                        snapshot_path = DATA_DIR / snapshot_filename

                        snapshot_df = pd.read_csv(uploaded)
                        snapshot_df.to_csv(snapshot_path, index=False)

                        # Merge into master
                        merge_for_show(
                            slug=slug,
                            snapshot_filename=snapshot_filename,
                        )

                        # Reload updated master and refresh the dashboard
                        master_path = DATA_DIR / f"{slug}_master.csv"
                        updated_df = pd.read_csv(master_path)
                        st.success("Master updated from cleaned snapshot.")

                        # Update session and rerun into fresh view
                        st.session_state["current_show_df"] = updated_df
                        st.rerun()

                    except Exception as e:
                        st.error(f"Error merging snapshot into master: {e}")


    # Ignore panel for marking videos as non show content
    render_ignore_panel(df, slug)

    # Load channel annotations once
    channel_annotations = load_channel_annotations()

    # Show the dashboard with UGC logic
    show_dashboard(df, label, slug=slug, channel_annotations=channel_annotations)


# Simple router
if st.session_state["view"] == "home":
    render_home(library)
elif st.session_state["view"] == "show":
    render_show_page()
