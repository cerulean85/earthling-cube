import boto3

# S3 클라이언트 생성 (region 명시적 지정)
s3 = boto3.client('s3', region_name='ap-northeast-2')  # 서울 리전

# 파일 업로드
bucket_name = 'kkennib-s3'
file_key = 'file-in-s343.txt'
local_file_path = 'C:\\Users\\ZHKim\\Desktop\\자본주의에 대하여.txt'

res = s3.upload_file(
    Filename=local_file_path,     # 로컬 파일 경로
    Bucket=bucket_name,           # 업로드할 S3 버킷 이름
    Key=file_key                  # S3 내 저장될 경로
)

print("✅ 업로드 완료")

# 방법 1: Presigned URL 생성 (임시 접근 링크, 기본 1시간 유효)
try:
    presigned_url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': file_key},
        ExpiresIn=3600  # 1시간 (초 단위)
    )
    print(f"🔗 Presigned URL (1시간 유효): {presigned_url}")
except Exception as e:
    print(f"❌ Presigned URL 생성 실패: {e}")

# 방법 2: Public URL 생성 (버킷이 public인 경우에만 작동)
region = 'ap-northeast-2'  # 서울 리전 직접 지정
public_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{file_key}"
print(f"🌐 Public URL (버킷이 public인 경우): {public_url}")

# 방법 3: S3 객체 정보 확인
try:
    response = s3.head_object(Bucket=bucket_name, Key=file_key)
    print(f"📊 파일 크기: {response['ContentLength']} bytes")
    print(f"📅 마지막 수정: {response['LastModified']}")
    print(f"🏷️  ETag: {response['ETag']}")
except Exception as e:
    print(f"❌ 객체 정보 조회 실패: {e}")