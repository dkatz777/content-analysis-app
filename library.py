import glob
import os
from typing import List, Dict

import pandas as pd


def slugify_show_name(raw_name: str) -> str:
    """
    Normalize a show name into a slug. This is the key we will use
    to map queries to saved CSVs and to prevent duplicates.

    Example:
      '"Beverly Hills 90210"' -> "beverly_hills_90210"
    """
    s = raw_name.strip().lower()
    # Remove surrounding quotes if present
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    s = s.replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch == "_")


def find_image_for_slug(slug: str) -> str | None:
    """
    Look for an image in img/ matching the slug, with common extensions.
    Returns the path if found, else None.
    """
    for ext in ("jpg", "jpeg", "png", "webp"):
        candidate = os.path.join("img", f"{slug}.{ext}")
        if os.path.exists(candidate):
            return candidate
    return None


def load_library() -> List[Dict]:
    """
    Scan the data/ folder for per show master CSVs and build a list of shows.

    We now treat ONLY files named:
      data/{slug}_master.csv
    as library entries.

    Each entry:
      {
        "slug": <base slug>,          # e.g. "the_sopranos"
        "display_name": <nice name>,  # e.g. "The Sopranos"
        "path": <path to master csv>, # e.g. "data/the_sopranos_master.csv"
        "image_path": <optional key art path>,
      }
    """
    shows: List[Dict] = []

    pattern = os.path.join("data", "*_master.csv")
    for path in glob.glob(pattern):
        filename = os.path.basename(path)
        root, _ = os.path.splitext(filename)  # e.g. "the_sopranos_master"

        suffix = "_master"
        if not root.endswith(suffix):
            # Defensive coding, but pattern should already enforce this
            continue

        base_slug = root[: -len(suffix)]  # e.g. "the_sopranos"
        display_name = base_slug.replace("_", " ").title()
        image_path = find_image_for_slug(base_slug)

        shows.append(
            {
                "slug": base_slug,
                "display_name": display_name,
                "path": path,
                "image_path": image_path,
            }
        )

    # Sort by name for nicer UI
    shows.sort(key=lambda x: x["display_name"])
    return shows


def load_show_df(show_path: str) -> pd.DataFrame:
    """
    Load a saved show CSV as a DataFrame.
    Currently assumes the path points at a per show master CSV.
    """
    return pd.read_csv(show_path)
