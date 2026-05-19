# Anime Recommendation System

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=flat-square&logo=python)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Built%20with-Streamlit-red?style=flat-square)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

> An intelligent anime recommendation engine that leverages detailed features to provide personalized recommendations based on user preferences and anime characteristics.

## Overview

This project builds a comprehensive recommendation system for anime using machine learning techniques. The system analyzes detailed anime features including genres, ratings, popularity, and other metrics to suggest anime that match user interests.

**Key Highlights:**
- Analyzes 1000+ anime with rich feature sets
- Intelligent recommendation algorithms
- Beautiful web-based UI with Streamlit
- Fast data processing pipeline

## Features

- **Data Cleaning Pipeline**: Normalizes and deduplicates anime data from multiple sources
- **Feature Engineering**: Extracts and processes detailed anime characteristics  
- **Recommendation Engine**: Uses advanced algorithms to generate personalized suggestions
- **Flexible Filtering**: Supports filtering by genres, ratings, episode count, and more
- **API Integration**: Connects with Jikan API to fetch real-time anime data
- **Artwork Enrichment**: Automatically fetches high-quality poster artwork

## Quick Start

Get up and running in 3 steps:

```bash
# 1. Clone and setup
git clone https://github.com/atyantjain/Anime_Recommendation.git
cd Anime_Recommendation
python -m venv myanime && source myanime/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the app
streamlit run app.py
```

The app will be available at `http://localhost:8501`

## Data Preparation

### Cleaning Data

Run the data cleaning script to preprocess the raw anime data:

```bash
python scripts/clean_data.py
```

**What it does:**
- Normalizes anime titles for consistency
- Removes exact duplicate rows based on title
- Groups similar titles to avoid near-duplicates
- Saves cleaned output to `data/cleaned_anime_data.csv`

### Fetching Fresh Data

Update your dataset with the latest anime:

```bash
python scripts/fetch_anime.py        # Fetch top anime from Jikan API
python scripts/add_artwork.py        # Enrich with poster artwork
```

## Usage

### Launch the Web App

```bash
streamlit run app.py
```

Browse to `http://localhost:8501` and start exploring anime recommendations!

### Available Scripts

| Script | Purpose |
|--------|---------|
| `scripts/clean_data.py` | Clean and deduplicate anime dataset |
| `scripts/fetch_anime.py` | Fetch top anime data from Jikan API |
| `scripts/add_artwork.py` | Enrich CSV with poster artwork URLs |

## Project Structure

```
Anime_Recommendation/
├── README.md                           # Project documentation
├── app.py                              # Main Streamlit application
├── requirements.txt                    # Python dependencies
├── .gitignore                          # Git ignore rules
├── data/                               # Datasets and preprocessed data
│   ├── anime_complete.csv                 # Complete anime dataset
│   ├── anime_data_with_manual_composers.csv
│   ├── anime_with_mal_artwork.csv         # Dataset with artwork URLs
│   ├── cleaned_anime_data.csv             # Cleaned/deduplicated data
│   └── top_anime_data.csv                 # Top anime from Jikan API
├── scripts/                            # Utility scripts
│   ├── add_artwork.py                     # Fetch and add artwork
│   ├── clean_data.py                      # Data cleaning pipeline
│   └── fetch_anime.py                     # Fetch from Jikan API
└── assets/                             # Images and UI assets
    ├── dan.png
    ├── dandadan.gif
    └── title.jpg
```

## Technologies

- **Python** (90.9%) - Core language for data processing and ML
- **C++** (4.6%) - Performance-critical operations
- **Cython** (3.8%) - Python/C integration for optimization
- **[Streamlit](https://streamlit.io/)** - Beautiful web UI framework
- **[scikit-learn](https://scikit-learn.org/)** - ML algorithms
- **[pandas](https://pandas.pydata.org/)** - Data manipulation
- **[numpy](https://numpy.org/)** - Numerical computing
- **[Jikan API](https://jikan.moe/)** - Anime data source

## Contributing

We love contributions! Here's how to get involved:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Ideas for Contributions
- Improve recommendation algorithms
- Bug fixes and optimizations
- Documentation improvements
- New features and enhancements

## License

This project is open source and available under the [MIT License](LICENSE). Feel free to use it in your projects!

## Contact & Support

- Have questions? [Open an issue](https://github.com/atyantjain/Anime_Recommendation/issues)
- Have suggestions? [Start a discussion](https://github.com/atyantjain/Anime_Recommendation/discussions)
- Found it useful? Please star the repository!

---

<div align="center">

**Made by [Atyant Jain](https://github.com/atyantjain)**

Last Updated: May 2026 | v1.0.0

</div>
