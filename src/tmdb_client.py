from __future__ import annotations

import time
import requests
from typing import Any, Optional, Dict

from src.logging_utils import get_logger

logger = get_logger()

BASE_URL = "https://api.themoviedb.org/3"


def get_tmdb_json(
    url: str,
    api_key: str,
    params: Optional[dict] = None,
    timeout: int = 30
) -> Optional[dict]:
    """
    GET a TMDB endpoint and return JSON dict, or None if request fails.
    """
    p = dict(params or {})
    p["api_key"] = api_key

    try:
        r = requests.get(url, params=p, timeout=timeout)
        if r.status_code == 404:
            logger.warning("TMDB 404 Not Found: url=%s params=%s", url, {k: v for k, v in p.items() if k != "api_key"})
            return None

        r.raise_for_status()
        return r.json()

    except requests.HTTPError:
        status = getattr(r, "status_code", None)
        try:
            body = r.json()
        except Exception:
            body = r.text if r is not None else None

        logger.error("TMDB HTTPError status=%s url=%s response=%s", status, url, body)
        return None

    except requests.Timeout:
        logger.error("TMDB Timeout: url=%s timeout=%ss", url, timeout)
        return None

    except requests.RequestException as e:
        logger.error("TMDB RequestException: url=%s error=%s", url, str(e))
        return None

    except Exception:
        logger.exception("TMDB Unexpected error: url=%s", url)
        return None


def fetch_movie_with_credits(
    movie_id: int,
    api_key: str,
    sleep_time: float = 0.25
) -> Optional[Dict[str, Any]]:
    """
    Fetch TMDB movie details + credits bundle.
    Returns None if details not found or request fails.
    """
    details_url = f"{BASE_URL}/movie/{movie_id}"
    credits_url = f"{BASE_URL}/movie/{movie_id}/credits"

    try:
        details = get_tmdb_json(details_url, api_key)
        if not details:
            logger.warning("Movie not found or failed fetch: movie_id=%s", movie_id)
            return None

        credits = get_tmdb_json(credits_url, api_key)

        # pacing for rate limits (even if credits failed)
        if sleep_time:
            time.sleep(sleep_time)

        bundle = {
            "movie": details,
            "credits": credits,
            "fetched_at": int(time.time())
        }

        logger.info(
            "Fetched movie bundle: movie_id=%s credits_ok=%s",
            movie_id, credits is not None
        )
        return bundle

    except Exception:
        logger.exception("fetch_movie_with_credits failed: movie_id=%s", movie_id)
        return None