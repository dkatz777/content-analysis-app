from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import subprocess
import sys

DATA_DIR = Path("data")
EXTERNAL_IDS = DATA_DIR / "show_external_ids.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run imdb_ingest.py for all slugs in show_external_ids.csv")
    parser.add_argument("--only_missing", action="store_true", help="Only ingest if <slug>_imdb_episodes_master.csv is missing.")
    args = parser.parse_args()

    if not EXTERNAL_IDS.exists():
        raise SystemExit(f"Missing {EXTERNAL_IDS}")

    df = pd.read_csv(EXTERNAL_IDS, dtype=str).fillna("")
    if "slug" not in df.columns or "imdb_title_id" not in df.columns:
        raise SystemExit("show_external_ids.csv must include columns: slug, imdb_title_id")

    slugs = sorted(set(df["slug"].str.strip().str.lower()))
    slugs = [s for s in slugs if s]

    if not slugs:
        raise SystemExit("No slugs found in show_external_ids.csv")

    total = len(slugs)
    failures = 0

    for i, slug in enumerate(slugs, start=1):
        out = DATA_DIR / f"{slug}_imdb_episodes_master.csv"
        if args.only_missing and out.exists():
            print(f"[{i}/{total}] skip slug={slug} (exists)")
            continue

        print(f"[{i}/{total}] ingest slug={slug}")
        cmd = [sys.executable, "scripts/imdb_ingest.py", "--slug", slug]
        r = subprocess.run(cmd, capture_output=True, text=True)

        if r.returncode != 0:
            failures += 1
            print(f"  FAILED slug={slug}")
            print(r.stdout)
            print(r.stderr)
        else:
            print(r.stdout.strip())

    print(f"Done. failures={failures}/{total}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
