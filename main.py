import boto3
import os
from PIL import Image
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# AWS S3 configuration
bucket_name = 'aigisbot-media'
region_name = 'us-east-1'
profile_name = 'new-lea'

# Initialize S3 client
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3', region_name=region_name)


def optimize_image(obj):
    key = obj['Key']

    if key.lower().endswith('.webp'):
        print(f"Skipping {key}, already a WebP image.")
        return

    # Download the image from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    img_data = response['Body'].read()

    # Optimize and convert to WebP
    img = Image.open(BytesIO(img_data))
    output = BytesIO()
    img.save(output, format='WEBP', quality=85)  # Adjust quality as needed
    output.seek(0)

    # Upload the optimized image back to S3
    s3_client.upload_fileobj(
        output, bucket_name, key[:-4] + ".webp", ExtraArgs={'ContentType': 'image/webp'})
    print(f"Optimized and uploaded {key} as WebP.")


def process_images():
    # List all objects in the S3 bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)

    # Create a thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for page in pages:
            for obj in page.get('Contents', []):
                futures.append(executor.submit(optimize_image, obj))

        # Wait for all threads to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")


if __name__ == '__main__':
    process_images()
