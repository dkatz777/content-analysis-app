import sys
import os
import pandas as pd


def export_channels_for_slug(slug: str) -> None:
    """
    Given a show slug, read data/<slug>.csv and write
    data/<slug>_channels_to_label.csv with distinct channels.
    """
    input_path = os.path.join("data", f"{slug}.csv")
    output_path = os.path.join("data", f"{slug}_channels.csv")

    if not os.path.exists(input_path):
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    df = pd.read_csv(input_path)

    missing_cols = {"channel_id", "channel_title"} - set(df.columns)
    if missing_cols:
        print(f"Input file is missing required columns: {', '.join(missing_cols)}")
        sys.exit(1)

    channels = (
        df[["channel_id", "channel_title"]]
        .drop_duplicates()
        .sort_values("channel_title")
    )

    channels.to_csv(output_path, index=False)
    print(f"Wrote {len(channels)} channels to {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python export_channels.py <show_slug>")
        print("Example: python export_channels.py the_sopranos")
        sys.exit(1)

    slug = sys.argv[1].strip()
    export_channels_for_slug(slug)
