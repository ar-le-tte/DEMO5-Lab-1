import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.tmdb_client import fetch_movie_with_credits
from src.logging_utils import get_logger

logger = get_logger()


def download_movies_parallel(
    movie_ids,
    out_dir,
    api_key,
    max_workers=8,
    sleep_between_calls=0.0,
    verbose=True,
):
    """
    Download TMDB movie bundles in parallel and save as one JSON per movie_id.

    Returns: list of tuples (movie_id, status)
    status in: saved, skipped_exists, not_found, error
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    def fetch_and_save(mid: int):
        try:
            out_path = out_dir / f"movie_id={mid}.json"

            if out_path.exists():
                return (mid, "skipped_exists")

            if sleep_between_calls:
                time.sleep(sleep_between_calls)

            bundle = fetch_movie_with_credits(mid, api_key)
            if bundle is None:
                return (mid, "not_found")

            tmp_path = out_dir / f".tmp_movie_id={mid}.json"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(bundle, f, ensure_ascii=False)

            tmp_path.replace(out_path)
            return (mid, "saved")

        except Exception:
            # logs stack trace
            logger.exception("Failed to fetch/save movie_id=%s", mid)
            return (mid, "error")

    logger.info(
        "Starting TMDB download: n_ids=%s max_workers=%s out_dir=%s",
        len(movie_ids), max_workers, str(out_dir)
    )

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(fetch_and_save, int(mid)): int(mid) for mid in movie_ids}

        for fut in as_completed(future_map):
            mid = future_map[fut]
            try:
                res = fut.result()
            except Exception:
                logger.exception("Unhandled future error for movie_id=%s", mid)
                res = (mid, "error")

            results.append(res)

            if verbose:
                logger.info("movie_id=%s status=%s", res[0], res[1])

    logger.info("Download finished. statuses=%s", dict(_count_statuses(results)))
    return results


def _count_statuses(results):
    counts = {}
    for _, status in results:
        counts[status] = counts.get(status, 0) + 1
    return counts.items()