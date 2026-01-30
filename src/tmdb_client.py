import os
import time
import requests

BASE_URL = "https://api.themoviedb.org/3"

def get_tmdb_json(url, api_key, params=None, verbose=True):
    if params is None:
        params = {}
    params["api_key"] = api_key

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        try:
            msg = e.response.json()
        except Exception:
            msg = e.response.text if e.response is not None else str(e)

        if verbose:
            print(f"[HTTP {status}] Failed: {url} | {msg}")
        return None

    except Exception as e:
        if verbose:
            print(f"[ERROR] Failed: {url} | {e}")
        return None

def fetch_movie_with_credits(movie_id, api_key, sleep_time=0.25):
    details_url = f"{BASE_URL}/movie/{movie_id}"
    credits_url = f"{BASE_URL}/movie/{movie_id}/credits"

    details = get_tmdb_json(details_url, api_key)
    if not details:
        print(f"ID {movie_id}: Movie not found. Skipping")
        return None

    credits = get_tmdb_json(credits_url, api_key)
    time.sleep(sleep_time)

    return {
        "movie": details,
        "credits": credits,
        "fetched_at": int(time.time())
    }
