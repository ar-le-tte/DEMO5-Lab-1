import os
import json
from dotenv import load_dotenv
from tmdb_client import fetch_movie_with_credits

MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818,
    24428, 168259, 99861, 284054, 12445, 181808, 330457,
    351286, 109445, 321612, 260513
]

def main():
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise RuntimeError("TMDB_API_KEY not found. Put it in a .env file.")

    out_dir = "data/bronze/tmdb_raw_json"
    os.makedirs(out_dir, exist_ok=True)

    for mid in MOVIE_IDS:
        if mid == 0:
            print("Skipping movie_id=0 (invalid placeholder)")
            continue

        out_path = os.path.join(out_dir, f"movie_id={mid}.json")

        if os.path.exists(out_path):
            print(f"Exists, skipping: {mid}")
            continue

        print(f"Fetching: {mid}")
        bundle = fetch_movie_with_credits(mid, api_key)

        if bundle is None:
            print(f"Not found / failed: {mid}")
            continue

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(bundle, f, ensure_ascii=False)

    print(f"\nDone. Raw JSON saved to: {out_dir}")

if __name__ == "__main__":
    main()
