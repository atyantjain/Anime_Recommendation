# Anime Recommendation System

An intelligent anime recommendation engine that leverages detailed features to provide personalized recommendations based on user preferences and anime characteristics.

## Overview

This project builds a recommendation system for anime using machine learning techniques. The system analyzes detailed anime features including genres, ratings, popularity, and other metrics to suggest anime that match user interests.

## Features

- **Data Cleaning Pipeline**: Normalizes and deduplicates anime data from multiple sources
- **Feature Engineering**: Extracts and processes detailed anime characteristics
- **Recommendation Engine**: Uses advanced algorithms to generate personalized suggestions
- **Flexible Filtering**: Supports filtering by genres, ratings, episode count, and more

## Table of Contents

- [Installation](#installation)
- [Data Preparation](#data-preparation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Technologies](#technologies)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Prerequisites

- Python 3.8 or higher
- pip or conda package manager

### Setup

1. Clone the repository:
```bash
git clone https://github.com/atyantjain/Anime_Recommendation.git
cd Anime_Recommendation
```

2. Create a virtual environment (recommended):
```bash
python -m venv myanime
source myanime/bin/activate  # On Windows: myanime\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Data Preparation

### Cleaning Data

Run the data cleaning script to preprocess the raw anime data:

```bash
python scripts/clean_data.py
```

This script performs the following operations:
- Normalizes anime titles for consistency
- Removes exact duplicate rows based on title
- Groups similar titles to avoid duplicates with slight variations
- Saves cleaned output to `data/cleaned_anime_data.csv`

### Input Data

Place your raw anime data in the `data/` directory. The expected format should include columns like:
- `title`: Anime title
- `genre`: Anime genres
- `rating`: User rating
- `episodes`: Number of episodes
- Additional features for recommendation

## Usage

### Run the Streamlit app

```bash
streamlit run app.py
```

### Optional helper scripts

- `python scripts/fetch_anime.py` — fetch top anime data from the Jikan API into `data/top_anime_data.csv`
- `python scripts/add_artwork.py` — enrich your anime CSV with artwork URL data from MAL via Jikan

## Project Structure

```
Anime_Recommendation/
├── README.md
├── app.py
├── requirements.txt
├── .gitignore
├── data/
│   ├── anime_complete.csv
│   ├── anime_data_with_manual_composers.csv
│   ├── anime_with_mal_artwork.csv
│   ├── cleaned_anime_data.csv
│   └── top_anime_data.csv
├── scripts/
│   ├── add_artwork.py
│   ├── clean_data.py
│   └── fetch_anime.py
├── assets/
│   ├── dan.png
│   ├── dandadan.gif
│   └── title.jpg
```

## Technologies

- **Python** (90.9%) - Core language for data processing and ML
- **C++** (4.6%) - Performance-critical operations
- **Cython** (3.8%) - Python/C integration for optimization
- **scikit-learn** - Machine learning algorithms
- **pandas** - Data manipulation and analysis
- **numpy** - Numerical computing

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit your changes (`git commit -am 'Add improvement'`)
4. Push to the branch (`git push origin feature/improvement`)
5. Open a Pull Request

## License

This project is open source and available under the [MIT License](LICENSE).

## Contact

For questions or suggestions, please open an issue in the [GitHub repository](https://github.com/atyantjain/Anime_Recommendation).

---

**Last Updated**: May 2026
