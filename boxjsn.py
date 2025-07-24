import os
import json
import boto3
from uuid import uuid4
from PIL import Image

# === CONFIG ===
ACCESS_KEY = ""
SECRET_KEY = ""
ENDPOINT_URL = ""
REGION = "ap-south-1"
BUCKET_NAME = "carpmai"
S3_FOLDER = "e2e/"
LOCAL_IMAGE_FOLDER = "images/"
LABEL_FOLDER = "labels/"
MAKE_PUBLIC = True

CLASSES = [
    "Bonnet",
    "Bumper",
    "Front Left Door",
    "Front Left Window",
    "Front Right Door",
    "Front Right Window",
    "Front Windshield",
    "Left Fender",
    "Left Quarter Panel",
    "Rear Bumper",
    "Rear Left Door",
    "Rear Left Window",
    "Rear Right Door",
    "Rear Right Window",
    "Rear Windshield",
    "Right Fender",
    "Right Quarter Panel",
    "SpareWheel",
    "Tail Gate"
]


# === S3 client ===
s3 = boto3.client(
    "s3",
    region_name=REGION,
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def upload_to_s3(filepath, filename):
    key = S3_FOLDER + filename
    extra_args = {"ACL": "public-read"} if MAKE_PUBLIC else {}
    s3.upload_file(filepath, BUCKET_NAME, key, ExtraArgs=extra_args)
    return f"{ENDPOINT_URL}/{BUCKET_NAME}/{key}" if MAKE_PUBLIC else s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': key},
        ExpiresIn=3600
    )

def parse_yolo_bbox_file(filepath, width, height):
    annotations = []
    with open(filepath, 'r') as f:
        for line in f:
            parts = list(map(float, line.strip().split()))
            if len(parts) != 5:
                continue  # must be class_id, x_center, y_center, width, height
            class_id, x_center, y_center, w, h = parts

            # Convert YOLO normalized bbox to percentages
            x = round((x_center - w / 2) * 100, 2)
            y = round((y_center - h / 2) * 100, 2)
            w = round(w * 100, 2)
            h = round(h * 100, 2)

            annotations.append({
                "id": str(uuid4()),
                "type": "rectanglelabels",
                "from_name": "label",
                "to_name": "image",
                "original_width": width,
                "original_height": height,
                "image_rotation": 0,
                "value": {
                    "rectanglelabels": [CLASSES[int(class_id)]],
                    "x": x,
                    "y": y,
                    "width": w,
                    "height": h,
                    "rotation": 0
                }
            })
    return annotations

def build_annotation(image_url, boxes, width, height):
    return {
        "data": {
            "image": image_url
        },
        "annotations": [{
            "model_version": "v1",
            "score": 0.5,
            "result": boxes
        }]
    }

# === MAIN ===
annotations = []

for filename in os.listdir(LOCAL_IMAGE_FOLDER):
    if filename.lower().endswith((".jpg", ".jpeg", ".png")):
        image_path = os.path.join(LOCAL_IMAGE_FOLDER, filename)
        label_path = os.path.join(LABEL_FOLDER, os.path.splitext(filename)[0] + ".txt")

        if not os.path.exists(label_path):
            print(f"⚠️  Skipping {filename}: label file not found.")
            continue

        with Image.open(image_path) as img:
            width, height = img.size

        image_url = upload_to_s3(image_path, filename)
        boxes = parse_yolo_bbox_file(label_path, width, height)
        if not boxes:
            print(f"⚠️  Skipping {filename}: no valid bounding boxes.")
            continue

        annotation = build_annotation(image_url, boxes, width, height)
        annotations.append(annotation)
        print(f"✅ Uploaded and annotated: {filename}")

# === SAVE ===
with open("bbox_annotations.json", "w") as f:
    json.dump(annotations, f, indent=2)

print("\n✅ Saved all bounding box annotations to bbox_annotations.json")
