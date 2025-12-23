from __future__ import annotations

from pathlib import Path
import pandas as pd

DATA_DIR = Path("data")
OUT = DATA_DIR / "imdb_show_summary.csv"


def weighted_avg(ratings: pd.Series, votes: pd.Series) -> float | None:
    r = pd.to_numeric(ratings, errors="coerce")
    v = pd.to_numeric(votes, errors="coerce").fillna(0)
    mask = r.notna() & (v > 0)
    if mask.sum() == 0:
        return None
    return float((r[mask] * v[mask]).sum() / v[mask].sum())


def main() -> None:
    rows = []

    for p in sorted(DATA_DIR.glob("*_imdb_episodes_master.csv")):
        slug = p.name.replace("_imdb_episodes_master.csv", "")
        try:
            df = pd.read_csv(p, low_memory=False)
            if df.empty:
                continue

            rating_col = "imdb_rating" if "imdb_rating" in df.columns else None
            votes_col = "imdb_votes" if "imdb_votes" in df.columns else None

            ratings = pd.to_numeric(df[rating_col], errors="coerce") if rating_col else pd.Series([], dtype=float)
            votes = pd.to_numeric(df[votes_col], errors="coerce") if votes_col else pd.Series([], dtype=float)

            avg = float(ratings.mean()) if len(ratings) else None
            wavg = weighted_avg(ratings, votes) if (rating_col and votes_col) else None
            total_votes = int(votes.fillna(0).sum()) if len(votes) else 0
            ep_count = int(df["imdb_episode_id"].nunique()) if "imdb_episode_id" in df.columns else int(len(df))

            best_rating = float(ratings.max()) if len(ratings) else None
            best_ep = ""
            if rating_col and "episode_title" in df.columns and best_rating is not None:
                best_row = df.loc[pd.to_numeric(df[rating_col], errors="coerce").idxmax()]
                best_ep = str(best_row.get("episode_title", ""))

            rows.append({
                "slug": slug,
                "imdb_episode_count": ep_count,
                "imdb_avg_rating": round(avg, 4) if avg is not None else "",
                "imdb_weighted_avg_rating": round(wavg, 4) if wavg is not None else "",
                "imdb_total_votes": total_votes,
                "imdb_best_episode_rating": best_rating if best_rating is not None else "",
                "imdb_best_episode_title": best_ep,
            })

        except Exception as e:
            rows.append({"slug": slug, "imdb_episode_count": "", "imdb_avg_rating": "", "imdb_weighted_avg_rating": "",
                         "imdb_total_votes": "", "imdb_best_episode_rating": "", "imdb_best_episode_title": ""})
            print(f"Failed summary for slug={slug}: {e}")

    out_df = pd.DataFrame(rows).sort_values("slug")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"Wrote {len(out_df)} rows to {OUT}")


if __name__ == "__main__":
    main()
