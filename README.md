# 🌊 WattaBuzz

**Apache Airflow 기반 YouTube 핫 콘텐츠 자동 탐지 및 모니터링 시스템**

YouTube에서 'WSWF', 'Kyoka', 'Kaea' 키워드 관련 핫한 영상과 댓글을 자동으로 수집하고 분석하는 데이터 파이프라인입니다.

## ✨ 주요 특징

- 🚀 **Apache Airflow 파이프라인**: 체계적인 워크플로우 관리 및 모니터링
- 🔥 **핫 콘텐츠 탐지**: 최근 댓글 활동 기반 실시간 인기도 측정
- 📊 **Streamlit 대시보드**: 직관적인 데이터 시각화 및 탐색
- 🎯 **타겟 키워드**: 미리 정의된 키워드 자동 모니터링
- 💾 **영구 저장**: PostgreSQL/SQLite 데이터베이스 연동
- 🔗 **원클릭 이동**: 핫한 영상/댓글로 바로 이동 가능

## 🏗️ 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Apache Airflow │ -> │   YouTube API   │ -> │   PostgreSQL    │
│  (스케줄링/실행)   │    │   (데이터 수집)    │    │   (데이터 저장)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         └─────────────────┐                           │
                          │                           │
                          v                           v
                ┌─────────────────┐          ┌─────────────────┐
                │   Streamlit     │ <------- │      DAG        │
                │   (대시보드)      │          │   (워크플로우)    │
                └─────────────────┘          └─────────────────┘
```

## 📁 프로젝트 구조

```
wattabuzz/
├── 📂 dags/                    # Airflow DAG 정의
│   ├── hotspotter_dag.py      # WattaBuzz 데이터 수집 파이프라인
│   └── test_dag.py            # 테스트용 DAG
├── 📂 core/                   # 핵심 비즈니스 로직
│   ├── analysis/              # 데이터 분석 엔진
│   │   └── sentiment_analyzer.py
│   ├── collectors/            # 데이터 수집기
│   │   └── youtube_collector.py
│   ├── database/              # 데이터베이스 관리
│   │   └── database_manager.py
│   └── services/              # 자동 수집 서비스
│       └── auto_collector.py
├── 📂 config/                 # 설정 관리
│   ├── keywords.py            # 타겟 키워드 정의
│   └── settings.py            # 환경 설정
├── 📂 airflow/                # Airflow 설정
│   └── airflow.cfg            # Airflow 구성 파일
├── 📱 app.py                  # Streamlit 웹 대시보드
├── 📋 requirements.txt        # Python 종속성
├── 📖 setup_airflow.md        # Airflow 설정 가이드
├── 🧪 test_youtube_api.py     # YouTube API 테스트
├── 📄 wattabuzz.md            # 프로젝트 상세 문서
├── 📋 pyproject.toml          # Python 프로젝트 설정
└── 📝 .editorconfig           # 에디터 설정
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone <repository-url>
cd wattabuzz

# 가상환경 생성 및 활성화
python -m venv dev
source dev/bin/activate  # Linux/Mac
# 또는 dev\Scripts\activate  # Windows

# Airflow 종속성 설치
pip install -r requirements-airflow.txt
```

### 2. YouTube API 키 설정

```bash
# .env 파일 생성
echo "YOUTUBE_API_KEY=your_api_key_here" > .env
```

### 3. Airflow 초기화

```bash
# Airflow 홈 디렉토리 설정
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow

# 데이터베이스 초기화
airflow db init

# 관리자 계정 생성
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@wattabuzz.com
```

### 4. 시스템 실행

**터미널 1 - Airflow 웹서버:**
```bash
source dev/bin/activate
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow
airflow webserver --port 8080
```

**터미널 2 - Airflow 스케줄러:**
```bash
source dev/bin/activate
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow
airflow scheduler
```

**터미널 3 - Streamlit 대시보드:**
```bash
source dev/bin/activate
streamlit run app.py
```

### 5. 접속 및 활성화

1. **Airflow 웹 UI**: http://localhost:8080 (관리자 계정으로 로그인)
2. **DAG 활성화**: `hotspotter_collection` 토글 ON
3. **Streamlit 대시보드**: http://localhost:8501

## 🎯 핵심 기능

### 🔥 핫 콘텐츠 탐지 알고리즘

#### 영상 핫점수 (100점 만점)
- **최근 댓글 활동** (60%): 최근 7일/1일 내 댓글 수
- **전체 조회수** (25%): 로그 스케일 점수
- **좋아요 비율** (10%): 조회수 대비 좋아요
- **영상 신선도** (5%): 업로드 시점 가중치

#### 댓글 핫점수 (100점 만점)
- **좋아요 수** (40%): 댓글 인기도
- **답글 활동** (40%): 대화 활성화 정도
- **최근성** (20%): 댓글 작성 시점 (1시간 내 특별 보너스)

### 🔄 자동화 워크플로우

1. **데이터 수집** (병렬): WSWF, Kyoka, Kaea 키워드별 독립 수집
2. **데이터 저장**: 기존 데이터 교체 후 새 데이터 저장
3. **정리 작업**: 7일 이전 오래된 데이터 자동 삭제
4. **리포트 생성**: 수집 결과 통계 및 요약

## 📊 모니터링 대시보드

### Airflow 웹 UI
- 📈 **실행 상태**: DAG 및 Task별 성공/실패 현황
- 📝 **상세 로그**: 각 수집 과정의 디버깅 정보
- ⏰ **실행 히스토리**: 과거 수집 기록 및 성능 추적
- 🔧 **수동 실행**: 필요 시 즉시 수집 트리거

### Streamlit 대시보드
- 🎬 **핫한 영상**: 핫점수 순위별 영상 목록 (썸네일, 통계, 링크)
- 💬 **핫한 댓글**: 실시간 화제 댓글 및 답글 활동
- 📊 **키워드별 통계**: 각 키워드의 수집 현황 및 성과
- 🔄 **실시간 업데이트**: 30초 자동 새로고침 옵션

## ⚙️ 설정 및 커스터마이징

### 키워드 변경
`config/keywords.py`에서 모니터링할 키워드 수정:
```python
TARGET_KEYWORDS = [
    "새로운키워드1",
    "새로운키워드2", 
    "새로운키워드3"
]
```

### 수집 주기 변경
`dags/hotspotter_dag.py`에서 스케줄 간격 조정:
```python
# 30분마다 실행
schedule_interval=timedelta(minutes=30)

# 매일 오전 9시 실행  
schedule_interval='0 9 * * *'
```

### 데이터 보관 기간 설정
`config/keywords.py`에서 보관 기간 수정:
```python
COLLECTION_SETTINGS = {
    "data_retention_days": 7,  
    # ...
}
```

## 🗄️ 데이터베이스

### 테이블 구조
- **hot_videos**: 핫한 영상 정보 (제목, 채널, 통계, 핫점수)
- **hot_comments**: 핫한 댓글 정보 (내용, 작성자, 좋아요, 핫점수)

### 지원 데이터베이스
- **PostgreSQL**: 프로덕션 환경


## 📈 로드맵

### ✅ Phase 1: 완료
- [x] Streamlit 기반 MVP 구축
- [x] YouTube 데이터 수집기
- [x] 핫 콘텐츠 탐지 알고리즘
- [x] Airflow 파이프라인 마이그레이션

### 🔄 Phase 2: 확장
- [ ] Twitter API 연동
- [ ] Reddit API 연동  
- [ ] Slack/이메일 알림 시스템
- [ ] 고급 시각화 차트

### 🚀 Phase 3: 고도화
- [ ] 머신러닝 기반 핫점수 개선
- [ ] 실시간 스트리밍 처리
- [ ] 모바일 앱 개발
- [ ] API 서비스 상용화


## 📞 문의 및 지원

- 이슈 리포팅: GitHub Issues
- 기능 요청: GitHub Discussions
- 문서 개선: Pull Request 환영

---

**🌊 WattaBuzz - 덕질을 조금 더 편하게** 🌊 