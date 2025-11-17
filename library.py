import glob
import os
import pandas as pd
from typing import List, Dict


def slugify_show_name(raw_name: str) -> str:
    """
    Normalize a show name into a slug. This is the key we will use
    to map queries to saved CSVs and to prevent duplicates.
    """
    s = raw_name.strip().lower()
    # Remove surrounding quotes if present
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    s = s.replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch == "_")


def load_library() -> List[Dict]:
    """
    Scan the data/ folder for CSVs and build a list of shows.

    Each entry: {"slug": ..., "display_name": ..., "path": ...}
    """
    shows = []
    for path in glob.glob("data/*.csv"):
        filename = os.path.basename(path)
        slug, _ = os.path.splitext(filename)
        display_name = slug.replace("_", " ").title()
        shows.append(
            {
                "slug": slug,
                "display_name": display_name,
                "path": path,
            }
        )
    # Sort by name for nicer UI
    shows.sort(key=lambda x: x["display_name"])
    return shows


def load_show_df(show_path: str) -> pd.DataFrame:
    """
    Load a saved show CSV as a DataFrame.
    """
    return pd.read_csv(show_path)
