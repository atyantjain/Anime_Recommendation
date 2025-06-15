import pandas as pd
import requests
from bs4 import BeautifulSoup
import wikipedia
import re
import unicodedata
from rapidfuzz import process, fuzz

def normalize_title(title):
    # Lowercase and strip symbols
    title = unicodedata.normalize("NFKD", title).lower()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\b(season|part|movie|final|ova|tv|special|recap|extra|arc|prologue|epilogue|kanketsu|hen|kouhen|zenpen|shou|ban)\b', '', title)
    title = re.sub(r'\b(1st|2nd|3rd|4th|5th|ii|iii|iv|v|vi|vii|viii|ix|x|s\d+)\b', '', title)
    title = re.sub(r'\d+', '', title)
    title = re.sub(r'\s+', ' ', title)
    return title.strip()

def fuzzy_group_titles(df, threshold=90):
    titles = df["normalized_title"].tolist()
    used = set()
    title_to_group = {}

    for base in titles:
        if base in used:
            continue
        matches = process.extract(base, titles, scorer=fuzz.ratio)
        for match_title, score, _ in matches:
            if score >= threshold:
                title_to_group[match_title] = base
                used.add(match_title)

    df["fuzzy_group"] = df["normalized_title"].map(title_to_group)
    return df

def fetch_wikipedia_composer(title):
    try:
        search_titles = [title, normalize_title(title)]
        for search_title in search_titles:
            search_results = wikipedia.search(search_title)
            if not search_results:
                continue
            page = wikipedia.page(search_results[0])
            soup = BeautifulSoup(requests.get(page.url).content, "html.parser")
            infobox = soup.find("table", {"class": "infobox"})
            if infobox:
                for row in infobox.find_all("tr"):
                    header = row.find("th")
                    if header and "music" in header.text.lower():
                        data = row.find("td")
                        return data.get_text(separator=", ").strip() if data else None
        return None
    except:
        return None

def fetch_wikipedia_themes(title):
    try:
        search_titles = [title, normalize_title(title)]
        for search_title in search_titles:
            search_results = wikipedia.search(search_title)
            if not search_results:
                continue
            page = wikipedia.page(search_results[0])
            text = page.summary.lower()
            theme_keywords = [
                "psychological", "time travel", "school", "supernatural", "revenge",
                "war", "coming-of-age", "friendship", "romance", "identity"
            ]
            themes_found = [kw for kw in theme_keywords if kw in text]
            if themes_found:
                return " | ".join(themes_found)
        return None
    except:
        return None

def clean_anime_data(input_csv, output_csv):
    df = pd.read_csv(input_csv)

    print(f"📊 Original dataset size: {len(df)}")

    # Normalize and fuzzy group
    df["normalized_title"] = df["title"].apply(normalize_title)
    df = fuzzy_group_titles(df)

    before_dedup = len(df)
    df = df.sort_values("score", ascending=False)
    df = df.drop_duplicates(subset="fuzzy_group", keep="first")
    after_dedup = len(df)

    print(f"✅ Removed {before_dedup - after_dedup} duplicates (from {before_dedup} to {after_dedup})")

    # Enrich with Wikipedia composer and themes
    for idx, row in df.iterrows():
        print(f"🔍 Processing: {row['title']}")
        if pd.isna(row["composer"]) or not row["composer"]:
            composer = fetch_wikipedia_composer(row["title"])
            if composer:
                df.at[idx, "composer"] = composer
                print(f"   🎵 Composer found: {composer}")
        if pd.isna(row["themes"]) or not row["themes"]:
            themes = fetch_wikipedia_themes(row["title"])
            if themes:
                df.at[idx, "themes"] = themes
                print(f"   🎭 Themes enriched: {themes}")

    df = df.drop(columns=["normalized_title", "fuzzy_group"])
    df.to_csv(output_csv, index=False)
    print(f"\n✅ Cleaned data saved to {output_csv}")

if __name__ == "__main__":
    clean_anime_data("top_anime_data.csv", "cleaned_anime_data.csv")
