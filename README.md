# Anime_Recommendation
Anime recommendation based on more detailed features

## Cleaning data

Run `python clean_data.py` to generate `cleaned_anime_data.csv`. The script
normalizes titles, removes exact duplicate rows by title, groups similar titles
using fuzzy matching and strips season/part information so only one entry per
anime remains. The script also fetches additional details from Wikipedia.
