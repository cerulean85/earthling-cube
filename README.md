# 🌍 Earthling-Cube

**gRPC 기반 분산 웹 크롤링 & NLP 분석 파이프라인 시스템**

웹 검색 데이터를 자동으로 수집하고, 형태소 분석 · 클리닝 · 빈도 분석 · TF-IDF · 공출현(Co-occurrence) 분석까지 이어지는 **엔드투엔드 NLP 데이터 파이프라인**을 분산 아키텍처로 구현한 프로젝트입니다.

---

## 📐 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL (DB)                          │
│              (태스크 상태 관리 & 분석 결과 메타데이터)               │
└──────────────────────────┬──────────────────────────────────────┘
                           │ poll (5초 주기)
                           ▼
              ┌────────────────────────┐
              │   Manager (cube-manager)│
              │  - 태스크 발견 & 할당    │
              │  - gRPC 서버/클라이언트  │
              └────────────┬───────────┘
                           │ gRPC: NotifyTask / GetIdleCount
                           ▼
              ┌────────────────────────┐
              │  Assistant (cube-assistant)│
              │  - 태스크 수신 & 워커 분배 │
              │  - gRPC 서버             │
              │  - 이벤트 루프 (3초 폴링)  │
              └────────────┬───────────┘
                           │ multiprocessing.Queue
                           ▼
              ┌────────────────────────┐
              │      WorkerPool        │
              │  ┌──────┐ ┌──────┐    │
              │  │Worker│ │Worker│ …  │
              │  │:1000 │ │:2000 │    │
              │  └──────┘ └──────┘    │
              └────────────┬───────────┘
                           │ action(task)
                           ▼
    ┌──────────────────────────────────────────────┐
    │           Application Layer                   │
    │  search → clean → frequency / tfidf / concor  │
    └──────────────────────────┬───────────────────┘
                               │ 결과 저장
                               ▼
                    ┌─────────────────┐
                    │   AWS S3 Bucket  │
                    │ (크롤링/분석 결과) │
                    └─────────────────┘
```

---

## 🔧 기술 스택

| 영역 | 기술 |
|------|------|
| **언어** | Python 3 |
| **분산 통신** | gRPC (protobuf) |
| **병렬 처리** | multiprocessing (Process, Value, Queue) |
| **데이터베이스** | PostgreSQL + SQLAlchemy ORM |
| **클라우드 스토리지** | AWS S3 (boto3) |
| **NLP 형태소 분석** | KoNLPy (Okt), MeCab |
| **텍스트 분석** | scikit-learn (TF-IDF), pandas, numpy, scipy |
| **웹 크롤링** | Selenium, undetected-chromedriver, BeautifulSoup4, requests |
| **NLP 설정 관리** | AWS DynamoDB (동적 불용어/복합명사 관리) |
| **설정 관리** | YAML (earth-compose.yaml) |

---

## 📁 프로젝트 구조

```
earthling-cube/
│
├── cube-manager.py              # Manager 노드 진입점
├── cube-assistant.py            # Assistant 노드 진입점 (태스크 라우팅)
├── earth-compose.yaml           # 클러스터 구성 설정 (노드 주소, 포트, 크롤링 설정)
├── requirements.txt             # Python 의존성 패키지
│
├── earthling/                   # 🔵 코어 프레임워크 (분산 통신 엔진)
│   ├── query.py                 #    ORM 모델 & 태스크 쿼리 계층
│   ├── connector/               #    데이터 접근 계층
│   │   ├── DBPoolConnector.py   #      DB 커넥션 풀 (SQLAlchemy)
│   │   └── s3_module.py         #      AWS S3 파일 I/O
│   ├── proto/                   #    gRPC 프로토콜 계층
│   │   ├── EarthlingProtocol.proto  # protobuf 서비스 정의
│   │   ├── Earthling.py         #      gRPC 베이스 서비서
│   │   ├── AssistantEarthling.py#      Assistant gRPC 구현체
│   │   ├── ManagerEarthling.py  #      Manager gRPC 구현체
│   │   └── WorkerEarthling.py   #      Worker gRPC 구현체
│   └── service/                 #    서비스 오케스트레이션 계층
│       ├── Com.py               #      공통 통신 베이스 클래스
│       ├── ComManager.py        #      Manager 핵심 로직
│       ├── ComAssistant.py      #      Assistant 핵심 로직 ⭐
│       ├── ComWorker.py         #      워커 프로세스 풀 관리
│       ├── Monitor.py           #      설정 & 상태 모니터링
│       └── Logging.py           #      로깅 유틸리티 (싱글턴)
│
├── application/                 # 🟠 비즈니스 로직 (NLP 파이프라인)
│   ├── common.py                #    공통 유틸리티 & S3 업로드 헬퍼
│   ├── settings.py              #    상수 정의
│   ├── search/                  #    🔍 웹 크롤링 모듈
│   │   ├── SearchApplication.py #      크롤링 오케스트레이터
│   │   ├── util.py              #      HTTP 세션 & 설정 유틸
│   │   ├── naver/               #      네이버 크롤러 (Web, News, Blog)
│   │   └── google/              #      구글 크롤러 (Portal)
│   ├── clean/                   #    🧹 텍스트 클리닝 & 형태소 분석
│   │   ├── CleanApplication.py  #      형태소 분석 + 다층 불용어 필터링
│   │   ├── config_adapter.py    #      DynamoDB 설정 어댑터
│   │   └── dynamodb_config_manager.py  # NLP 설정 CRUD (DynamoDB)
│   ├── frequency/               #    📊 단어 빈도 분석
│   │   └── FrequencyApplication.py
│   ├── tfidf/                   #    📈 TF-IDF 분석
│   │   └── TfidfApplication.py
│   └── concor/                  #    🔗 공출현(Co-occurrence) 분석
│       └── ConcorApplication.py
│
├── config_data/                 # NLP 설정 파일 (로컬 폴백용)
│   ├── stopwords.json           #    불용어 사전
│   ├── compound_nouns.json      #    복합명사 사전
│   └── patterns.json            #    형태소 패턴 규칙
│
└── temp/                        # 워커 상태 모니터링 파일
    └── worker_*.json
```

---

## 🔄 데이터 파이프라인 흐름

전체 파이프라인은 **5단계**로 구성되며, 각 단계가 완료되면 자동으로 다음 단계의 태스크를 트리거합니다.

```
① Search (검색/크롤링)
   │  네이버(Web/News/Blog) · 구글 검색 결과 수집
   │  → 크롤링 결과를 S3에 저장
   ▼
② Clean (클리닝/형태소 분석)
   │  KoNLPy Okt로 형태소 분석
   │  다층 불용어 필터링 (도메인별, 빈도별, n-gram, 문맥, 형태소 패턴)
   │  → 정제된 형태소 데이터를 S3에 저장
   ▼
   ├──→ ③ Frequency (빈도 분석)
   │       단어+품사별 출현 빈도 집계 → 상위 100개 추출
   │
   ├──→ ④ TF-IDF (가중치 분석)
   │       문서별 TF-IDF 벡터 산출 → 문서당 상위 10개 키워드 추출
   │
   └──→ ⑤ Concordance (공출현 분석)
           문장 내 단어 동시출현 매트릭스 생성 → 상위 100개 단어쌍 추출
```

### 태스크 상태 관리

각 태스크는 DB에서 다음 상태를 순서대로 거칩니다:

```
preparing → pending → progress → completed
```

- **preparing**: 태스크 생성됨 (대기 전)
- **pending**: 실행 대기 중 → Manager가 발견하여 Assistant에 할당
- **progress**: Worker가 처리 중
- **completed**: 처리 완료 → 결과 S3 URL이 DB에 기록됨

---

## 🏛️ 핵심 디자인 패턴

### 1. Manager-Assistant-Worker 3계층 분산 패턴
- **Manager**: DB를 폴링하여 태스크를 발견하고, 가장 유휴 워커가 많은 Assistant에 할당
- **Assistant**: gRPC로 태스크를 수신하고, 멀티프로세싱 큐를 통해 Worker에 분배
- **Worker**: 실제 비즈니스 로직(크롤링, NLP 분석)을 독립 프로세스에서 실행

### 2. Decorator Pattern (gRPC 통신 분리)
```
ComAssistant → AssistantEarthlingDecorator → AssistantEarthling
(오케스트레이션)   (통신 인프라 래핑)            (gRPC 서비서 구현)
```
핵심 로직과 gRPC 통신 인프라를 분리하여 관심사를 격리합니다.

### 3. Singleton Pattern (공유 자원 관리)
- `WorkerPool.getInstance()` — 워커 풀 단일 인스턴스
- `DBPoolConnector.getInstance()` — DB 커넥션 풀 단일 인스턴스
- `log` — 로깅 인스턴스

### 4. Strategy Pattern (액션 콜백 주입)
```python
# cube-assistant.py에서 태스크 타입에 따라 적절한 Application을 라우팅
def action(task):
    if queue_name == 'search':  SearchApplication.run(task)
    if queue_name == 'clean':   CleanApplication.run(task)
    if queue_name == 'frequency': FrequencyApplication.run(task)
    ...
```

### 5. 프로세스 간 통신 (IPC)
- `multiprocessing.Queue` — 태스크 전달 파이프라인
- `multiprocessing.Value` — 워커 상태(idle_count, is_working) 공유

---

## ⚙️ 설정 (earth-compose.yaml)

```yaml
# 데이터베이스 접속 정보
db:
  kkennibdb: 'postgresql://...'

# AWS S3 설정
s3:
  bucket_name: 'kkennib-s3'
  region: 'ap-northeast-2'

# gRPC 노드 구성
rpc:
  manager:                    # Manager 노드 주소
    address: '127.0.0.1'
    port: 50153
  assistant:                  # Assistant 노드 목록
    - address: '127.0.0.1'
      port: 50156
      workers: [1000, 2000, 3000, 4000, 5000]  # 워커 포트 번호

# 크롤링 설정 (사이트별 딜레이, 최대 수집 수 등)
naver:
  web: { delay_time: 10, max_count: 1000 }
  news: { delay_time: 10, max_count: 1000 }
  blog: { delay_time: 10, max_count: 1000 }
google:
  portal: { delay_time: 30, max_count: 2000 }
```

---

## 🚀 실행 방법

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 설정 파일 편집

`earth-compose.yaml`에서 DB 접속 정보, S3 버킷, 노드 주소/포트를 환경에 맞게 수정합니다.

### 3. Manager 노드 실행

```bash
python cube-manager.py
```

### 4. Assistant 노드 실행 (별도 터미널)

```bash
python cube-assistant.py
```

> Manager와 Assistant는 같은 머신에서 실행하거나, 다른 머신에 분산 배치할 수 있습니다.
> `earth-compose.yaml`의 주소/포트 설정으로 멀티 노드 구성이 가능합니다.

---

## 📊 gRPC 프로토콜

`EarthlingProtocol.proto`에 정의된 서비스:

| RPC | 방향 | 설명 |
|-----|------|------|
| `Echo` | Manager ↔ Assistant | 연결 상태 확인 (Health Check) |
| `GetIdleCount` | Manager → Assistant | 유휴 워커 수 조회 (부하 분산) |
| `NotifyTask` | Manager → Assistant | 태스크 할당 (taskNo, queueName, message) |

---

## 🧠 NLP 처리 상세

### 형태소 분석 (Clean)
- **KoNLPy Okt** 기반 한국어 형태소 분석
- **다층 불용어 필터링 시스템**:
  - 도메인 기반 불용어
  - 빈도 기반 필터링
  - n-gram 패턴 필터링
  - 문맥 기반 필터링
  - 형태소 패턴 규칙 적용
  - 복합명사 처리
- **DynamoDB 기반 동적 설정 관리** (5분 TTL 캐시)

### 분석 모듈
- **Frequency**: `Counter` 기반 단어 빈도 집계 → 상위 100개
- **TF-IDF**: `scikit-learn TfidfVectorizer` → 문서별 상위 10개 키워드
- **Concordance**: 문장 내 단어쌍 동시출현 매트릭스 → `scipy.sparse` → 상위 100개 쌍
