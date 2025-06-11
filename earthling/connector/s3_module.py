import boto3, yaml

def upload_file_to_s3(file_path, s3_file_key):
  s3 = boto3.client('s3')
  with open(f'earth-compose.yml') as f:
    compose = yaml.load(f, Loader=yaml.FullLoader)
    compose = compose['s3']
    bucket_name = compose['bucket_name']        
  s3.upload_file(Filename=file_path, Bucket=bucket_name, Key=s3_file_key)
  
  try:
    region = s3.meta.region_name or 'us-east-1'  # 기본 리전 설정
    public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_file_key}"
    response = s3.head_object(Bucket=bucket_name, Key=s3_file_key)
    file_size = response['ContentLength']    
    return public_url, file_size
  except Exception as e:
    print(f"❌ 객체 정보 조회 실패: {e}")
    return '', 0