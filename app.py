import re
import numpy as np
import pandas as pd
import streamlit as st
from PIL import Image
import requests
from io import BytesIO
import base64

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

@st.cache_data
def get_base64_image(image_path):
    """Convert image to base64 string"""
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode()
    except:
        return None


# ----------------------------
# Helpers
# ----------------------------
def _clean_text(x: str) -> str:
    if pd.isna(x):
        return ""
    x = str(x)
    x = x.lower()
    x = re.sub(r"<[^>]+>", " ", x)              # remove HTML
    x = re.sub(r"[^a-z0-9\s]+", " ", x)         # keep alnum
    x = re.sub(r"\s+", " ", x).strip()          # squeeze spaces
    return x


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return df[col] if exists else empty strings."""
    return df[col].fillna("").astype(str) if col in df.columns else pd.Series([""] * len(df))


def extract_dominant_colors(image_url, num_colors=3):
    """Extract dominant colors from an image URL, filtering out dark colors."""
    try:
        # Download image
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Open image and convert to RGB
        image = Image.open(BytesIO(response.content))
        image = image.convert('RGB')
        
        # Resize image for faster processing
        image = image.resize((150, 150))
        
        # Convert to numpy array
        data = np.array(image)
        data = data.reshape((-1, 3))
        
        # Simple clustering approach - get most frequent colors
        from collections import Counter
        
        # Reduce color space to make clustering more effective
        reduced_data = data // 32 * 32  # Reduce to 8 levels per channel
        
        # Count color frequencies
        color_counts = Counter(map(tuple, reduced_data))
        
        # Filter out very dark colors and get lighter, more vibrant colors
        filtered_colors = []
        for color, count in color_counts.most_common(num_colors * 3):  # Get more to filter from
            # Calculate brightness (luminance)
            brightness = (color[0] * 0.299 + color[1] * 0.587 + color[2] * 0.114)
            
            # Skip very dark colors (brightness < 80) and very light colors (brightness > 240)
            if brightness > 80 and brightness < 240:
                # Enhance the color slightly for better gradients
                enhanced_color = tuple(min(255, int(c * 1.2)) for c in color)
                filtered_colors.append(enhanced_color)
                
                if len(filtered_colors) >= num_colors:
                    break
        
        # If we don't have enough colors, add some defaults
        if len(filtered_colors) < 2:
            filtered_colors.extend([(103, 126, 234), (118, 75, 162), (240, 147, 251)])
        
        # Convert to hex colors
        hex_colors = []
        for color in filtered_colors[:num_colors]:
            hex_color = "#{:02x}{:02x}{:02x}".format(int(color[0]), int(color[1]), int(color[2]))
            hex_colors.append(hex_color)
        
        return hex_colors
    except Exception as e:
        # Return default bright colors if extraction fails
        return ["#667eea", "#764ba2", "#f093fb"]


def create_gradient_background(colors):
    """Create CSS gradient background from color list with light overlay for readability."""
    if len(colors) >= 2:
        gradient = f"linear-gradient(rgba(255, 255, 255, 0.4), rgba(255, 255, 255, 0.4)), linear-gradient(135deg, {colors[0]}, {colors[1]}"
        if len(colors) >= 3:
            gradient += f", {colors[2]}"
        gradient += ")"
        return gradient
    else:
        return f"linear-gradient(rgba(255, 255, 255, 0.4), rgba(255, 255, 255, 0.4)), linear-gradient(135deg, {colors[0]}, {colors[0]})"


def build_feature_text(df: pd.DataFrame, feature_weights: dict = None) -> pd.Series:
    """
    Combine useful columns into one text field with custom weights.
    Add/remove columns here as per your dataset.
    """
    title = _safe_col(df, "title")
    synopsis = _safe_col(df, "synopsis")
    genres = _safe_col(df, "genres")
    themes = _safe_col(df, "themes")
    studio = _safe_col(df, "studio")
    composer = _safe_col(df, "composer")
    mood = _safe_col(df, "mood")

    # Default weights - only used if no feature_weights provided
    if feature_weights is None:
        feature_weights = {
            "genres": 3,
            "themes": 2,
            "composer": 3,
            "mood": 3,
            "studio": 1,
            "synopsis": 2
        }

    # Weighting trick: repeat fields based on their weights
    # Title is always included at base level (1x)
    combined = (
        (title + " ") * 1 +
        (genres + " ") * feature_weights["genres"] +
        (themes + " ") * feature_weights["themes"] +
        (composer + " ") * feature_weights["composer"] +
        (mood + " ") * feature_weights["mood"] +
        (studio + " ") * feature_weights["studio"] +
        (synopsis + " ") * feature_weights["synopsis"]
    )

    return combined.map(_clean_text)


@st.cache_data(show_spinner=False)
def load_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    if "title" not in df.columns:
        raise ValueError("Your CSV must contain a 'title' column.")

    # Drop exact duplicate titles (keep first). If you already collapsed seasons, great.
    df = df.dropna(subset=["title"]).copy()
    df["title"] = df["title"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["title"], keep="first").reset_index(drop=True)

    return df


@st.cache_resource(show_spinner=False)
def fit_vectorizer_and_matrix(feature_text: pd.Series, weights_hash: str):
    """
    Fits TF-IDF on the combined text and returns:
    - vectorizer
    - tfidf_matrix (sparse)
    The weights_hash parameter ensures cache invalidation when weights change.
    """
    vectorizer = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.90,
        sublinear_tf=True
    )
    tfidf_matrix = vectorizer.fit_transform(feature_text)
    return vectorizer, tfidf_matrix


def recommend_similar(
    df: pd.DataFrame,
    title_query: str,
    tfidf_matrix,
    top_k: int = 10,
    min_score: float | None = None,
    same_genre_only: bool = False
) -> pd.DataFrame:
    """
    Returns a DataFrame of top_k nearest anime to title_query based on cosine similarity.
    Optional filters:
    - min_score: if 'score' column exists, filter by >= min_score
    - same_genre_only: if 'genres' exists, restrict recs to overlapping genres
    """
    # Map title -> index
    title_to_idx = {t: i for i, t in enumerate(df["title"].tolist())}
    if title_query not in title_to_idx:
        raise ValueError("Selected title not found in data.")

    idx = title_to_idx[title_query]

    # Cosine similarities (1 x N)
    sims = cosine_similarity(tfidf_matrix[idx], tfidf_matrix).ravel()
    sims[idx] = -1  # exclude itself

    # Candidate indices sorted by similarity
    ranked_idx = np.argsort(-sims)

    # Build candidate DF
    cand = df.iloc[ranked_idx].copy()
    cand["similarity"] = sims[ranked_idx]

    # Optional filter: min_score (if present)
    if min_score is not None and "score" in cand.columns:
        cand = cand[pd.to_numeric(cand["score"], errors="coerce").fillna(-1) >= float(min_score)]

    # Optional filter: same_genre_only (if present)
    if same_genre_only and "genres" in df.columns:
        base_genres = set(str(df.loc[idx, "genres"]).lower().split(","))
        base_genres = {g.strip() for g in base_genres if g.strip()}

        def overlaps(genres_str: str) -> bool:
            gs = set(str(genres_str).lower().split(","))
            gs = {g.strip() for g in gs if g.strip()}
            return len(base_genres.intersection(gs)) > 0

        cand = cand[cand["genres"].apply(overlaps)]

    # Take top_k
    cand = cand.head(top_k).reset_index(drop=True)
    return cand


# ----------------------------
# Streamlit UI
# ----------------------------
st.set_page_config(page_title="Anime Recommender (Content-Based)", layout="wide")

# Get base64 encoded image for title background
title_bg = get_base64_image("dan.png")

if title_bg:
    st.markdown(f"""
    <style>
    .page-title {{
        background-image: url('data:image/gif;base64,{title_bg}');
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
        padding: 0;
        border-radius: 12px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
        margin-bottom: 24px;
        text-align: center;
        display: flex;
        align-items: center;
        justify-content: center;
        min-height: 200px;
        backdrop-filter: blur(2px);
        -webkit-backdrop-filter: blur(2px);
        position: relative;
        overflow: hidden;
        animation: slowMotion 120s ease-in-out infinite;
    }}
    .page-title::before {{
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-image: url('data:image/gif;base64,{title_bg}');
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
        filter: blur(1px);
        z-index: -1;
        animation: slowZoom 120s ease-in-out infinite;
    }}
    @keyframes slowMotion {{
        0%, 100% {{ transform: scale(1); }}
        50% {{ transform: scale(1.02); }}
    }}
    @keyframes slowZoom {{
        0%, 100% {{ transform: scale(1); background-position: center; }}
        25% {{ transform: scale(1.05); background-position: center top; }}
        50% {{ transform: scale(1.08); background-position: center; }}
        75% {{ transform: scale(1.05); background-position: center bottom; }}
    }}
    .page-title h1 {{
        margin: 0;
        font-size: 8rem;
        font-weight: 900;
        font-family: 'Zenkaku Gothic New', 'Hiragino Kaku Gothic Pro', 'Yu Gothic', 'Meiryo', sans-serif;
        color: white;
        letter-spacing: 0.15em;
        width: 100%;
        text-transform: uppercase;
        z-index: 2;
        position: relative;
    }}
    
    /* Decorative Elements */
    .title-decorations {{
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        pointer-events: none;
        z-index: 1;
    }}
    
    .deco-element {{
        position: absolute;
        color: white;
        font-size: 2rem;
        text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.5);
        opacity: 0.8;
    }}
    
    .deco-1 {{
        top: 15%;
        left: 10%;
        animation: float1 6s ease-in-out infinite;
    }}
    
    .deco-2 {{
        top: 20%;
        right: 15%;
        animation: float2 8s ease-in-out infinite;
    }}
    
    .deco-3 {{
        bottom: 25%;
        left: 15%;
        animation: float3 7s ease-in-out infinite;
    }}
    
    .deco-4 {{
        bottom: 20%;
        right: 10%;
        animation: float4 5s ease-in-out infinite;
    }}
    
    .deco-5 {{
        top: 50%;
        left: 5%;
        animation: pulse 4s ease-in-out infinite;
    }}
    
    .deco-6 {{
        top: 50%;
        right: 5%;
        animation: pulse 4s ease-in-out infinite 2s;
    }}
    
    .deco-7 {{
        top: 10%;
        left: 50%;
        animation: float1 9s ease-in-out infinite;
    }}
    
    .deco-8 {{
        bottom: 10%;
        left: 50%;
        animation: float2 6s ease-in-out infinite;
    }}
    
    .deco-9 {{
        top: 30%;
        left: 3%;
        animation: pulse 5s ease-in-out infinite 1s;
    }}
    
    .deco-10 {{
        top: 70%;
        right: 3%;
        animation: float4 7s ease-in-out infinite;
    }}
    
    .deco-11 {{
        bottom: 40%;
        right: 50%;
        animation: float3 8s ease-in-out infinite;
    }}
    
    .deco-12 {{
        top: 40%;
        right: 30%;
        animation: pulse 6s ease-in-out infinite 3s;
    }}
    
    @keyframes float1 {{
        0%, 100% {{ transform: translateY(0px) rotate(0deg); }}
        50% {{ transform: translateY(-15px) rotate(180deg); }}
    }}
    
    @keyframes float2 {{
        0%, 100% {{ transform: translateY(0px) scale(1); }}
        50% {{ transform: translateY(-10px) scale(1.1); }}
    }}
    
    @keyframes float3 {{
        0%, 100% {{ transform: translateX(0px) rotate(0deg); }}
        50% {{ transform: translateX(10px) rotate(-180deg); }}
    }}
    
    @keyframes float4 {{
        0%, 100% {{ transform: translateY(0px) translateX(0px); }}
        25% {{ transform: translateY(-8px) translateX(-5px); }}
        75% {{ transform: translateY(8px) translateX(5px); }}
    }}
    
    @keyframes pulse {{
        0%, 100% {{ transform: scale(1) rotate(0deg); opacity: 0.8; }}
        50% {{ transform: scale(1.2) rotate(180deg); opacity: 1; }}
    }}
    
    @keyframes gradientShift {{
        0% {{ background-position: 0% 50%; }}
        50% {{ background-position: 100% 50%; }}
        100% {{ background-position: 0% 50%; }}
    }}
    </style>
    <div class="page-title">
        <div class="title-decorations">
            <div class="deco-element deco-1">変</div>
            <div class="deco-element deco-2">態</div>
            <div class="deco-element deco-3">ア</div>
            <div class="deco-element deco-4">ニ</div>
            <div class="deco-element deco-5">メ</div>
            <div class="deco-element deco-6">推</div>
            <div class="deco-element deco-7">漫</div>
            <div class="deco-element deco-8">画</div>
            <div class="deco-element deco-9">オ</div>
            <div class="deco-element deco-10">タ</div>
            <div class="deco-element deco-11">ク</div>
            <div class="deco-element deco-12">愛</div>
        </div>
        <h1>My Next Anime</h1>
    </div>
    """, unsafe_allow_html=True)
else:
    # Fallback if image doesn't load
    st.markdown("""
    <style>
    .page-title {
        background: white;
        padding: 16px 24px;
        border-radius: 12px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
        margin-bottom: 24px;
        text-align: center;
    }
    .page-title h1 {
        margin: 0;
        color: #333;
    }
    </style>
    <div class="page-title">
        <h1>My Next Anime</h1>
    </div>
    """, unsafe_allow_html=True)
st.caption("Your next anime recommendation based on what you like!")

with st.sidebar:
    st.header("Data")
    csv_path = st.text_input("Path to clean CSV", value="/Users/atyantjain/Desktop/Personal Projects/Anime_Recommendation/anime_with_mal_artwork.csv")
    st.caption("Put your CSV in the same folder as app.py (or provide a full path).")

    st.header("Recommendation Settings")
    top_k = st.slider("How many recommendations?", 5, 30, 12)

    use_min_score = st.checkbox("Filter by minimum MAL score (if available)", value=False)
    min_score = st.slider("Minimum score", 0.0, 10.0, 7.0, 0.1) if use_min_score else None

    same_genre_only = st.checkbox("Only recommend anime with overlapping genres (if available)", value=False)

try:
    df = load_data(csv_path)

    # Feature weight selection - horizontal layout under title
    st.markdown("**Boost recommendation accuracy by selecting features that matter to you:**")
    
    col_w0, col_w1, col_w2, col_w3, col_w4, col_w5, col_w6 = st.columns(7)
    
    with col_w0:
        use_default = st.checkbox("Default", value=True, help="Use our optimized preset weights")
    with col_w1:
        boost_genres = st.checkbox("Genres", value=False, help="3x emphasis on genres")
    with col_w2:
        boost_themes = st.checkbox("Themes", value=False, help="3x emphasis on themes")
    with col_w3:
        boost_composer = st.checkbox("Music", value=False, help="3x emphasis on music")
    with col_w4:
        boost_mood = st.checkbox("Mood", value=False, help="3x emphasis on mood")
    with col_w5:
        boost_studio = st.checkbox("Studio", value=False, help="3x emphasis on studio")
    with col_w6:
        boost_synopsis = st.checkbox("Story", value=False, help="3x emphasis on story")
    
    # Calculate dynamic weights based on selections
    if use_default:
        # Our optimized preset weights
        feature_weights = {
            "genres": 3,
            "themes": 2, 
            "composer": 3,
            "mood": 3,
            "studio": 1,
            "synopsis": 2
        }
    else:
        # Individual feature boosts - selected features get 3x, others get 1x
        any_selected = boost_genres or boost_themes or boost_composer or boost_mood or boost_studio or boost_synopsis
        
        if any_selected:
            feature_weights = {
                "genres": 3 if boost_genres else 1,
                "themes": 3 if boost_themes else 1,
                "composer": 3 if boost_composer else 1,
                "mood": 3 if boost_mood else 1,
                "studio": 3 if boost_studio else 1,
                "synopsis": 3 if boost_synopsis else 1
            }
        else:
            # If no individual features selected, use equal weights
            feature_weights = {
                "genres": 1,
                "themes": 1,
                "composer": 1,
                "mood": 1,
                "studio": 1,
                "synopsis": 1
            }
    
    # Show current weight mode
    if use_default:
        st.caption("Using optimized preset weights (Genres=3, Music=3, Mood=3, Themes=2, Story=2, Studio=1)")
    else:
        selected_features = []
        if boost_genres: selected_features.append("Genres")
        if boost_themes: selected_features.append("Themes")
        if boost_composer: selected_features.append("Music")
        if boost_mood: selected_features.append("Mood")
        if boost_studio: selected_features.append("Studio")
        if boost_synopsis: selected_features.append("Story")
        
        if selected_features:
            st.caption(f"3x Boosting: {', '.join(selected_features)} (others = 1x)")
        else:
            st.caption("Using equal weights for all features")
    
    # Generate feature text with custom weights
    feature_text = build_feature_text(df, feature_weights)
    
    # Create a hash of the weights to ensure cache invalidation when weights change
    import hashlib
    weights_str = str(sorted(feature_weights.items()))
    weights_hash = hashlib.md5(weights_str.encode()).hexdigest()
    
    _, tfidf_matrix = fit_vectorizer_and_matrix(feature_text, weights_hash)

    #st.success(f"Loaded {len(df):,} anime from {csv_path}")

    col1, col2 = st.columns([1, 1], gap="large")

    with col1:
        st.subheader("Pick an anime you like")
        titles = df["title"].sort_values().tolist()
        selected_title = st.selectbox("Anime title", options=titles, index=0)
        # Store selected title in session state for detailed analysis page
        st.session_state['selected_title'] = selected_title
        
        # Add CSS for continuous animated gradient background with fixed colors
        st.markdown(f"""
        <style>
        .stApp {{
            background: #fefefe;
            background-attachment: fixed;
        }}
        
        /* Continuous flowing animation */
        @keyframes continuousFlow {{
            0% {{ 
                background-position: 0% 0%; 
            }}
            100% {{ 
                background-position: 100% 100%; 
            }}
        }}
        
        /* Make content more readable with white shadow boxes */
        .main .block-container {{
            background: white;
            border-radius: 15px;
            padding: 2rem;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.15);
        }}
        
        /* Style sidebar to match */
        .css-1d391kg {{
            background: white;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
        }}
        
        /* Main title gets its own box */
        .stTitle h1 {{
            background: white;
            padding: 12px 16px;
            border-radius: 10px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
            margin: 8px 0;
        }}
        
        /* Section containers for grouped content */
        .stContainer > div {{
            background: white;
            padding: 16px;
            border-radius: 10px;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.1);
            margin: 12px 0;
        }}
        
        /* Override image styling to not have separate box when in cards */
        .recommendation-card .stImage {{
            background: transparent;
            padding: 0;
            box-shadow: none;
            margin: 0;
        }}
        
        /* Override individual text element styling inside cards */
        .recommendation-card h2, .recommendation-card h3, .recommendation-card h4, 
        .recommendation-card h5, .recommendation-card h6, .recommendation-card .stMarkdown, 
        .recommendation-card .stText, .recommendation-card .stCaption {{
            background: transparent;
            padding: 4px 0;
            margin: 4px 0;
            box-shadow: none;
        }}
        
        /* Dataframe styling */
        .stDataFrame {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            padding: 8px;
            margin: 12px 0;
        }}
        
        /* Remove individual text element boxes - let them inherit from containers */
        h2, h3, h4, h5, h6, .stMarkdown, .stText, .stCaption {{
            background: transparent;
            padding: 4px 0;
            margin: 4px 0;
        }}
        
        /* Form elements in subtle boxes */
        .stSelectbox, .stSlider, .stCheckbox {{
            background: rgba(248, 249, 250, 0.8);
            padding: 8px;
            border-radius: 6px;
            margin: 8px 0;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }}
        
        /* Image containers */
        .stImage {{
            background: white;
            padding: 12px;
            border-radius: 8px;
            box-shadow: 0 3px 12px rgba(0, 0, 0, 0.1);
            margin: 8px 0;
        }}
        
        /* Image elements themselves */
        .stImage img {{
            border-radius: 4px;
        }}
        </style>
        """, unsafe_allow_html=True)

        # Show details if present
        # Display artwork and synopsis side by side
        if "synopsis" in df.columns:
            syn = df.loc[df["title"] == selected_title, "synopsis"].iloc[0]
            if isinstance(syn, str) and syn.strip():
                # Create columns for artwork and synopsis
                art_col, syn_col = st.columns([1, 2], gap="medium")
                
                with art_col:
                    selected_row = df[df['title'] == selected_title].iloc[0]
                    if "artwork_url" in df.columns and str(selected_row.get("artwork_url", "")).strip():
                        try:
                            st.image(selected_row["artwork_url"], caption=selected_title, width=200)
                        except:
                            st.write("🖼️ No artwork available")
                    else:
                        st.write("🖼️ No artwork available")
                    
                    # Display meta bits under the artwork
                    meta_bits = []
                    for c in ["genres", "themes", "studio", "composer", "mood", "episodes", "score"]:
                        if c in df.columns:
                            v = df.loc[df["title"] == selected_title, c].iloc[0]
                            if not pd.isna(v) and str(v).strip():
                                meta_bits.append((c, v))
                    
                    if meta_bits:
                        st.markdown("**Details**")
                        # Add CSS to reduce spacing for subsequent elements
                        st.markdown("""
                        <style>
                        .stMarkdown p, .element-container p {
                            margin-bottom: 0.0rem !important;
                            margin-top: 0.1rem !important;
                        }
                        </style>
                        """, unsafe_allow_html=True)
                        
                        for k, v in meta_bits:
                            st.write(f"**{k}**: {v}")
                
                with syn_col:
                    st.markdown("**Synopsis**")
                    st.write(syn)
            else:
                # If no synopsis, just show artwork normally
                selected_row = df[df['title'] == selected_title].iloc[0]
                if "artwork_url" in df.columns and str(selected_row.get("artwork_url", "")).strip():
                    try:
                        st.image(selected_row["artwork_url"], caption=selected_title, width=200)
                    except:
                        st.write("🖼️ No artwork available")
                else:
                    st.write("🖼️ No artwork available")
        else:
            # If no synopsis column, just show artwork normally
            selected_row = df[df['title'] == selected_title].iloc[0]
            if "artwork_url" in df.columns and str(selected_row.get("artwork_url", "")).strip():
                try:
                    st.image(selected_row["artwork_url"], caption=selected_title, width=200)
                except:
                    st.write("🖼️ No artwork available")
            else:
                st.write("🖼️ No artwork available")

    with col2:
        st.subheader("The Next to watch")
        if selected_title:
            recs = recommend_similar(
                df=df,
                title_query=selected_title,
                tfidf_matrix=tfidf_matrix,
                top_k=top_k,
                min_score=min_score,
                same_genre_only=same_genre_only
            )
            st.markdown(f"### Recommendations based on **{selected_title}**")

            # Quick cards in scrollable container
            container = st.container(height=1500)
            
            with container:
                # Add CSS styling for artwork shadows and disable horizontal scroll
                st.markdown("""
                <style>
                .stImage img {
                    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.3) !important;
                    border-radius: 8px !important;
                }
                div[data-testid="stVerticalBlock"] > div[style*="overflow"] {
                    overflow-x: hidden !important;
                    overflow-y: auto !important;
                }
                /* Remove all possible borders from container */
                div[data-testid="stVerticalBlock"] > div,
                div[data-testid="stVerticalBlock"] > div > div,
                div[style*="height"],
                div[style*="overflow"],
                .element-container div,
                .stContainer div,
                [data-testid="stVerticalBlock"] div {
                    border: none !important;
                    outline: none !important;
                    box-shadow: none !important;
                    border-top: none !important;
                    border-bottom: none !important;
                    border-left: none !important;
                    border-right: none !important;
                }
                </style>
                """, unsafe_allow_html=True)
                
                for rank, (_, row) in enumerate(recs.iterrows(), 1):
                    col_img, col_info = st.columns([1, 2], gap="medium")
                    
                    with col_img:
                        if "artwork_url" in recs.columns and str(row.get("artwork_url", "")).strip():
                            try:
                                st.image(row["artwork_url"], caption=f"{rank}. {row['title']}", use_container_width=True)
                            except:
                                st.write("🖼️ No image")
                        else:
                            st.write("🖼️ No image")
                    
                    with col_info:
                        st.markdown(f"**{rank}. {row['title']}**")
                        st.write(f"Similarity: `{row['similarity']:.3f}`")
                        
                        if "genres" in recs.columns and str(row.get("genres", "")).strip():
                            st.write(f"**Genres:** {row['genres']}")
                        if "themes" in recs.columns and str(row.get("themes", "")).strip():
                            st.write(f"**Themes:** {row['themes']}")
                        if "composer" in recs.columns and str(row.get("composer", "")).strip():
                            st.write(f"**Composer:** {row['composer']}")
                        if "mood" in recs.columns and str(row.get("mood", "")).strip():
                            st.write(f"**Mood:** {row['mood']}")
                        if "studio" in recs.columns and str(row.get("studio", "")).strip():
                            st.write(f"**Studio:** {row['studio']}")
                        if "score" in recs.columns and pd.notna(row.get("score")):
                            st.write(f"**Score:** {row['score']}")
                        if "episodes" in recs.columns and pd.notna(row.get("episodes")):
                            st.write(f"**Episodes:** {row['episodes']}")
                        if "synopsis" in recs.columns and str(row.get("synopsis", "")).strip():
                            st.write(f"**Synopsis:** {row['synopsis'][:200]}{'...' if len(str(row['synopsis'])) > 200 else ''}")
                    
                    st.markdown("---")  # Add separator between cards

                # # Data table at bottom
                # st.markdown("---")
                # st.markdown("### 📊 Detailed view")
                # show_cols = ["title", "similarity"]
                # for c in ["score", "genres", "themes", "studio", "composer", "mood", "episodes"]:
                #     if c in recs.columns:
                #         show_cols.append(c)

                # st.dataframe(
                #     recs[show_cols],
                #     use_container_width=True,
                #     hide_index=True
                # )

except Exception as e:
    st.error("Could not load or process data.")
    st.exception(e)
