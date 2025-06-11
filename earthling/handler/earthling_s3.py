import os, sys, time
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connector.s3_module import upload_file_to_s3 as s3_upload

def generate_s3_file_key(file_path):
  file_extension = os.path.splitext(file_path)[1]  # 파일 확장명 추출 .txt, .csv 등
  now = time.localtime()
  timestamp = f"{now.tm_year}{now.tm_mon:02d}{now.tm_mday:02d}{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
  s3_file_key = f"{timestamp}{file_extension}"
  return s3_file_key

def upload_file_to_s3(file_path):
  s3_file_key = generate_s3_file_key(file_path)
  public_url, file_size = s3_upload(file_path, s3_file_key)
  return public_url, file_size
  