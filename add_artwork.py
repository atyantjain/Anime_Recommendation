import time
import pandas as pd
import requests
from difflib import SequenceMatcher

FILENAME = "anime_with_mal_artwork.csv"      # single file to modify

JIKAN_SEARCH = "https://api.jikan.moe/v4/anime"
SLEEP = 0.6  # be nice to the API

def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()

def best_match(title: str, items: list):
    best, best_score = None, -1.0
    for it in items:
        cand = it.get("title") or ""
        s = sim(title, cand)
        if s > best_score:
            best, best_score = it, s
    return best, best_score

def main():
    import os
    
    # Check if file exists, if not create it from base CSV
    if not os.path.exists(FILENAME):
        # If the target file doesn't exist, try to load from a base file
        base_files = ["cleaned_anime_data.csv", "anime_data_with_manual_composers.csv", "top_anime_data.csv"]
        df = None
        for base_file in base_files:
            if os.path.exists(base_file):
                df = pd.read_csv(base_file)
                print(f"Creating {FILENAME} from {base_file}")
                break
        if df is None:
            raise FileNotFoundError(f"No base CSV file found to create {FILENAME}")
    else:
        df = pd.read_csv(FILENAME)
        print(f"Loading existing {FILENAME}")

    for col in ["mal_id", "mal_url", "artwork_url", "mal_title_matched", "mal_match_score"]:
        if col not in df.columns:
            df[col] = ""

    session = requests.Session()
    session.headers.update({"User-Agent": "anime-artwork-enricher/1.0"})

    # Filter to only rows that need artwork (empty artwork_url)
    rows_needing_artwork = df[df["artwork_url"].isna() | (df["artwork_url"] == "")].copy()
    total_rows = len(rows_needing_artwork)
    total_in_df = len(df)
    processed = 0
    
    print(f"Total anime in dataset: {total_in_df}")
    print(f"Anime needing artwork: {total_rows}")
    
    if total_rows == 0:
        print("All anime already have artwork URLs!")
        return

    for idx, row in rows_needing_artwork.iterrows():
        title = str(row["title"]).strip()
        processed += 1

        print(f"[{processed}/{total_rows}] Processing: {title}")
        i = idx  # Use the original dataframe index

        try:
            r = session.get(JIKAN_SEARCH, params={"q": title, "limit": 5}, timeout=20)
            if r.status_code == 429:
                print("  Rate limited, waiting...")
                time.sleep(1.2)
                r = session.get(JIKAN_SEARCH, params={"q": title, "limit": 5}, timeout=20)
            r.raise_for_status()

            items = r.json().get("data", [])
            best, score = best_match(title, items)

            if best:
                df.at[i, "mal_id"] = best.get("mal_id", "")
                df.at[i, "mal_url"] = best.get("url", "")
                img = (
                    (best.get("images") or {}).get("jpg", {}).get("large_image_url")
                    or (best.get("images") or {}).get("jpg", {}).get("image_url")
                    or ""
                )
                df.at[i, "artwork_url"] = img
                df.at[i, "mal_title_matched"] = best.get("title", "")
                df.at[i, "mal_match_score"] = round(float(score), 4)
                print(f"  ✓ Found: {best.get('title', '')} (score: {score:.3f})")
            else:
                df.at[i, "mal_match_score"] = 0.0
                print(f"  ✗ No match found")

        except Exception as e:
            df.at[i, "mal_match_score"] = -1
            print(f"  Error: {e}")

        time.sleep(SLEEP)

        # Save progress every 10 entries
        if processed % 10 == 0:
            df.to_csv(f"{FILENAME}.temp", index=False)
            print(f"  Saved progress at {processed}/{total_rows}")

    df.to_csv(FILENAME, index=False)
    print(f"\nCompleted! Updated: {FILENAME}")
    
    # Clean up temp file
    try:
        os.remove(f"{FILENAME}.temp")
    except:
        pass

if __name__ == "__main__":
    main()
