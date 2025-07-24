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
LABEL_FOLDER = "labels/"  # same name as image but with .txt
MAKE_PUBLIC = True

CLASSES = [
    "Bonnet",
    "Bumper",
    "Bumper Corner Left",
    "Bumper Corner Right",
    "Foot Rest",
    "Front Left Door",
    "Front Left Door Window",
    "Front Left Wheel",
    "Front Right Door",
    "Front Right Door Window",
    "Front Right Wheel",
    "Front Windshield",
    "Grill",
    "Indicator Light Left",
    "Indicator Light Right",
    "Left Fender",
    "Left Fog Lamp",
    "Left Headlamp",
    "Left ORVM",
    "Left Pillar A",
    "Left Pillar B",
    "Left Pillar C",
    "Left Pillar D",
    "Left Quarter Glass",
    "Left Quarter Panel",
    "Lower Bumper",
    "Rear Bumper",
    "Rear Left Door",
    "Rear Left Door Window",
    "Rear Left Wheel",
    "Rear Light Left",
    "Rear Light Right",
    "Rear Reflector Left",
    "Rear Reflector Right",
    "Rear Right Door",
    "Rear Right Door Window",
    "Rear Right Wheel",
    "Rear Windshield",
    "Rear Wiper",
    "Right Fender",
    "Right Fog Lamp",
    "Right Headlamp",
    "Right ORVM",
    "Right Pillar A",
    "Right Pillar B",
    "Right Pillar C",
    "Right Pillar D",
    "Right Quarter Glass",
    "Right Quarter Panel",
    "Roof",
    "Spare Wheel",
    "SpareWheel",
    "Spoiler",
    "Tail Gate",
    "Upper Grill",
    "Wiper"
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

def parse_yolo_polygon_file(filepath, width, height):
    annotations = []
    with open(filepath, 'r') as f:
        for line in f:
            parts = list(map(float, line.strip().split()))
            if len(parts) < 3 or len(parts) % 2 == 0:
                continue  # must have class_id and even number of coordinates
            class_id = int(parts[0])
            coords = parts[1:]
            points = [[round(x * 100, 2), round(y * 100, 2)] for x, y in zip(coords[::2], coords[1::2])]
            annotations.append({
                "id": str(uuid4()),
                "type": "polygonlabels",
                "from_name": "label",
                "to_name": "image",
                "original_width": width,
                "original_height": height,
                "image_rotation": 0,
                "value": {
                    "polygonlabels": [CLASSES[class_id]],
                    "points": points,
                    "rotation": 0
                }
            })
    return annotations

def build_annotation(image_url, polygons, width, height):
    return {
        "data": {
            "image": image_url
        },
        "annotations": [{
            "model_version": "v1",
            "score": 0.5,
            "result": polygons
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
        polygons = parse_yolo_polygon_file(label_path, width, height)
        if not polygons:
            print(f"⚠️  Skipping {filename}: no valid polygons.")
            continue

        annotation = build_annotation(image_url, polygons, width, height)
        annotations.append(annotation)
        print(f"✅ Uploaded and annotated: {filename}")

# === SAVE ===
with open("polygon_annotations.json", "w") as f:
    json.dump(annotations, f, indent=2)

print("\n✅ Saved all polygon annotations to polygon_annotations.json")
