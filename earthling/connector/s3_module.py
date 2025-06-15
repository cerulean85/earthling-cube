import os, time, boto3, yaml, requests


def generate_s3_file_key(file_path):
    file_extension = os.path.splitext(file_path)[1]
    now = time.localtime()
    timestamp = f"{now.tm_year}{now.tm_mon:02d}{now.tm_mday:02d}{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
    s3_file_key = f"{timestamp}{file_extension}"
    return s3_file_key


def get_bucket_name():
    with open("earth-compose.yaml") as f:
        compose = yaml.safe_load(f)
        bucket_name = compose["s3"]["bucket_name"]
    return bucket_name


def upload_file_to_s3(file_path):
    s3_file_key = generate_s3_file_key(file_path)

    s3 = boto3.client("s3")
    bucket_name = get_bucket_name()
    s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=s3_file_key)

    try:
        region = s3.meta.region_name or "us-east-1"  # Default region setting
        public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_file_key}"
        response = s3.head_object(Bucket=bucket_name, Key=s3_file_key)
        file_size = response["ContentLength"]
        return public_url, file_size
    except Exception as e:
        print(f"❌ Failed to search data: {e}")
        return "", 0

def upload_from_buffer_to_s3(buffer, file_path):
    s3_file_key = generate_s3_file_key(file_path)

    s3 = boto3.client("s3")
    bucket_name = get_bucket_name()
    s3.put_object(
        Bucket=bucket_name,         # 🔁 버킷 이름
        Key=s3_file_key,   # 🔁 저장할 S3 경로
        Body=buffer.getvalue()
    )

    try:
        region = s3.meta.region_name or "us-east-1"  # Default region setting
        public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_file_key}"
        response = s3.head_object(Bucket=bucket_name, Key=s3_file_key)
        file_size = response["ContentLength"]
        return public_url, file_size
    except Exception as e:
        print(f"❌ Failed to search data: {e}")
        return "", 0


def read_file_from_s3(url):    
    response = requests.get(url)

    content = ""
    if response.status_code == 200:
        content = response.text
    else:
        print(f"Failed to fetch file. Status code: {response.status_code}")
    return content
