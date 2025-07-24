#!/usr/bin/env python3
import os, json, boto3, signal, sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

ACCESS_KEY = os.getenv("E2E_ACCESS_KEY", "")
SECRET_KEY = os.getenv("E2E_SECRET_KEY", "")
ENDPOINT_URL  = os.getenv("E2E_ENDPOINT_URL", "")
REGION        = os.getenv("E2E_REGION", "ap-south-1")
BUCKET_NAME   = os.getenv("E2E_BUCKET", "carpmai")
S3_FOLDER     = os.getenv("E2E_PREFIX", "e2e/")
LOCAL_IMAGE_FOLDER = os.getenv("LOCAL_IMAGE_FOLDER", "../3DRealCar_Segment_Dataset/images/train")
MAKE_PUBLIC   = os.getenv("MAKE_PUBLIC", "true").lower() == "true"
JSON_PATH     = os.getenv("JSON_PATH", "urls.json")

# Save to disk after every N successful uploads (default=1: every image)
COMMIT_EVERY  = int(os.getenv("COMMIT_EVERY", "1"))

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".tif", ".tiff"}

s3 = boto3.client(
    "s3", region_name=REGION, endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
)

def object_url(key: str) -> str:
    if MAKE_PUBLIC:
        return f"{ENDPOINT_URL}/{BUCKET_NAME}/{key}"
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=3600
    )

def load_json(path: str) -> list[dict]:
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    return data
            except json.JSONDecodeError:
                pass
    return []

def save_json(path: str, data: list[dict]):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def existing_filenames(json_list: list[dict]) -> set[str]:
    return {d.get("filename", "") for d in json_list if "filename" in d}

def upload_incremental():
    folder = Path(LOCAL_IMAGE_FOLDER)
    if not folder.exists():
        raise FileNotFoundError(folder)

    data = load_json(JSON_PATH)
    done = existing_filenames(data)
    counter = 0

    def flush_and_log():
        save_json(JSON_PATH, data)
        print(f"💾 Persisted {len(data)} records to {JSON_PATH}")

    # Ensure we persist on termination (Ctrl+C, kill, etc.)
    def handle_signal(signum, frame):
        print(f"\n📥 Caught signal {signum}. Flushing JSON and exiting…")
        flush_and_log()
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    for path in folder.iterdir():
        if not (path.is_file() and path.suffix.lower() in IMAGE_EXTS):
            continue
        if path.name in done:
            print(f"⏭️  Skipping {path.name} (already in JSON)")
            continue
        try:
            key = f"{S3_FOLDER}{path.name}"
            extra = {"ACL": "public-read"} if MAKE_PUBLIC else {}
            s3.upload_file(str(path), BUCKET_NAME, key, ExtraArgs=extra)
            url = object_url(key)
            data.append({ "image": url})
            counter += 1
            print(f"✅ {path.name} -> {url}")

            # **Persist incrementally**
            if counter % COMMIT_EVERY == 0:
                flush_and_log()

        except Exception as e:
            print(f"❌ Failed {path.name}: {e}")

    # Final flush
    flush_and_log()

def list_to_json():
    rows = load_json(JSON_PATH)
    have = {d.get("filename") for d in rows if "filename" in d}

    continuation = None
    while True:
        kw = {"Bucket": BUCKET_NAME, "Prefix": S3_FOLDER}
        if continuation:
            kw["ContinuationToken"] = continuation
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            name = Path(key).name
            if Path(key).suffix.lower() in IMAGE_EXTS and name not in have:
                rows.append({"image": object_url(key)})
                # Persist every addition (super safe)
                save_json(JSON_PATH, rows)
        if resp.get("IsTruncated"):
            continuation = resp["NextContinuationToken"]
        else:
            break

    save_json(JSON_PATH, rows)

if __name__ == "__main__":
    mode = os.getenv("MODE", "upload")
    if mode == "upload":
        upload_incremental()
    else:
        list_to_json()
