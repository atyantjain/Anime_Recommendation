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
python clean_data.py
```

This script performs the following operations:
- Normalizes anime titles for consistency
- Removes exact duplicate rows based on title
- Groups similar titles to avoid duplicates with slight variations
- Generates a cleaned dataset: `cleaned_anime_data.csv`

### Input Data

Place your raw anime data in the `data/` directory. The expected format should include columns like:
- `title`: Anime title
- `genre`: Anime genres
- `rating`: User rating
- `episodes`: Number of episodes
- Additional features for recommendation

## Usage

### Getting Recommendations

(Add specific usage instructions for your recommendation engine)

```bash
python recommend.py --user_id <id> --num_recommendations <number>
```

## Project Structure

```
Anime_Recommendation/
├── README.md                 # This file
├── clean_data.py            # Data cleaning script
├── recommend.py             # Main recommendation engine
├── requirements.txt         # Project dependencies
├── data/                    # Data directory
│   └── anime_data.csv      # Raw anime data
├── output/                  # Generated outputs
│   └── cleaned_anime_data.csv
└── src/                     # Source code modules
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
