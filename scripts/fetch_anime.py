import requests
import pandas as pd
import time

def fetch_top_anime(pages=10):
    anime_list = []

    for page in range(1, pages + 1):
        print(f"📥 Fetching page {page}...")
        url = f"https://api.jikan.moe/v4/top/anime?page={page}"
        response = requests.get(url)
        time.sleep(1.2)  # Respect API rate limit

        if response.status_code == 200:
            data = response.json()["data"]
            for anime in data:
                anime_list.append({
                    "title": anime.get("title"),
                    "synopsis": anime.get("synopsis"),
                    "genres": " | ".join(g["name"] for g in anime.get("genres", [])),
                    "themes": " | ".join(t["name"] for t in anime.get("themes", [])),
                    "score": anime.get("score"),
                    "episodes": anime.get("episodes"),
                    "aired": anime.get("aired", {}).get("prop", {}).get("from", {}).get("year"),
                    "studios": " | ".join(s["name"] for s in anime.get("studios", [])),
                    "composer": None,
                    "features": "",
                    "mood": ""
                })
        else:
            print(f"❌ Failed to fetch page {page} (status code {response.status_code})")

    return anime_list

# 🔄 Fetch anime and convert to DataFrame
anime_data = fetch_top_anime(pages=80)  # ~2000 anime (25 per page)
df = pd.DataFrame(anime_data)

# 💾 Save as CSV
output_path = Path(__file__).resolve().parent.parent / "data" / "top_anime_data.csv"
df.to_csv(output_path, index=False, encoding="utf-8")
print(f"✅ Saved to {output_path}")
