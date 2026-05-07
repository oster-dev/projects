from pathlib import Path
import requests
import json
from datetime import datetime

"""
Jikan API Feature Pipeline

Created by: 
https://github.com/oster-dev/

Description: 
Fetches top anime data from the Jikan API, validates records,
builds derived features, and stores the output as JSON.
"""


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# API Ingestion Layer
def fetch_top_anime():
    url = "https://api.jikan.moe/v4/top/anime"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# Validation Layer
def validate_response(payload):
    if not isinstance(payload, dict):
        raise ValueError("API response is not a dictionary")

    if "data" not in payload:
        raise ValueError("Missing 'data' field in API response")

    if not isinstance(payload["data"], list):
        raise ValueError("'data' is not a list")

    return payload["data"]


def validate_anime(anime):
    if not isinstance(anime, dict):
        return None

    mal_id = anime.get("mal_id")
    title = anime.get("title")
    score = anime.get("score")
    popularity = anime.get("popularity")
    episodes = anime.get("episodes")
    genres = anime.get("genres", [])

    if not isinstance(mal_id, int):
        return None

    if not isinstance(title, str) or not title.strip():
        return None

    if score is not None and not isinstance(score, (int, float)):
        score = None

    if popularity is not None and not isinstance(popularity, int):
        popularity = None

    if episodes is not None and not isinstance(episodes, int):
        episodes = None

    if not isinstance(genres, list):
        genres = []

    return {
        "anime_id": mal_id,
        "title": title,
        "score": score,
        "popularity": popularity,
        "episodes": episodes,
        "genres": genres,
    }


payload = fetch_top_anime()
anime_list = validate_response(payload)

validated_anime = []

for anime in anime_list:
    cleaned = validate_anime(anime)
    if cleaned is not None:
        validated_anime.append(cleaned)

print(f"Valid anime records: {len(validated_anime)}")
if validated_anime:
    print(validated_anime[0])


# Feature Layer with None Protection
def build_feature_record(anime):
    anime_id = anime["anime_id"]
    title = anime["title"]
    score = anime["score"]
    popularity = anime["popularity"]
    episodes = anime["episodes"]
    genres = anime["genres"]

    if popularity is None:
        popularity_bucket = "unknown"
    elif popularity <= 100:
        popularity_bucket = "very_popular"
    elif popularity <= 500:
        popularity_bucket = "popular"
    elif popularity <= 2000:
        popularity_bucket = "mid_popular"
    else:
        popularity_bucket = "niche"

    if genres is None:
        genre_count = 0
        has_multiple_genres = False
    else:
        genre_count = len(genres)
        has_multiple_genres = len(genres) > 1

    feature_record = {
        "anime_id": anime_id,
        "title": title,
        "is_high_score": score is not None and score >= 8,
        "popularity_bucket": popularity_bucket,
        "is_long_running": episodes is not None and episodes > 24,
        "genre_count": genre_count,
        "has_multiple_genres": has_multiple_genres,
    }

    return feature_record


# Feature Record Layer
feature_records = []

for anime in validated_anime:
    feature_record = build_feature_record(anime)
    feature_records.append(feature_record)

print(f"Feature records: {len(feature_records)}")
print(feature_records[:3])


# JSON Output Layer
output_path = Path(__file__).resolve().parent / "output" / f"feature_records_{timestamp}.json"

with open(output_path, "w", encoding="utf-8") as file:
    json.dump(feature_records, file, indent=4, ensure_ascii=False)

print(f"File saved to: {output_path}")