import pandas as pd
import requests
from bs4 import BeautifulSoup
import wikipedia
import re
import unicodedata
from rapidfuzz import process, fuzz


def base_title(title: str) -> str:
    """Return *title* normalized and stripped of season/part/final indicators."""
    title = normalize_title(title)
    # remove patterns like "season 2" or "part 1"
    title = re.sub(
        r"\b(?:season|part)\s*(?:\d+|[ivx]+)(?:st|nd|rd|th)?\b", "", title
    )
    title = re.sub(r"\b(?:s|p)\d+\b", "", title)
    # remove any standalone "final" or "finale" words
    title = re.sub(r"\bfinale?\b", "", title)
    return re.sub(r"\s+", " ", title).strip()


def normalize_title(title):
    """Return a simplified version of *title* for fuzzy matching."""
    title = unicodedata.normalize("NFKD", title)
    title = title.lower()
    title = re.sub(r"[\W_]+", " ", title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


def fuzzy_group_titles(df, column="normalized_title", threshold=90):
    titles = df[column].tolist()
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

    df["fuzzy_group"] = df[column].map(title_to_group)
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
                        return (
                            data.get_text(separator=", ").strip()
                            if data
                            else None
                        )
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
                "psychological",
                "time travel",
                "school",
                "supernatural",
                "revenge",
                "war",
                "coming-of-age",
                "friendship",
                "romance",
                "identity",
            ]
            themes_found = [kw for kw in theme_keywords if kw in text]
            if themes_found:
                return " | ".join(themes_found)
        return None
    except:
        return None


def clean_anime_data(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    df = df.drop_duplicates(subset="title", keep="first")

    print(f"📊 Original dataset size: {len(df)}")

    # Normalize titles and derive base titles for deduplication
    df["normalized_title"] = df["title"].apply(normalize_title)
    df["base_title"] = df["title"].apply(base_title)
    df = fuzzy_group_titles(df, column="base_title")

    before_dedup = len(df)
    df = df.sort_values("score", ascending=False)
    df = df.drop_duplicates(subset="base_title", keep="first")
    df["title"] = df["base_title"].str.title()
    after_dedup = len(df)

    print(
        f"✅ Removed {before_dedup - after_dedup} duplicates (from {before_dedup} to {after_dedup})"
    )

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

    df = df.drop(columns=["normalized_title", "base_title", "fuzzy_group"])
    df.to_csv(output_csv, index=False)
    print(f"\n✅ Cleaned data saved to {output_csv}")


if __name__ == "__main__":
    clean_anime_data("top_anime_data.csv", "cleaned_anime_data.csv")
