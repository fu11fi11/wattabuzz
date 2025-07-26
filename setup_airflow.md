# 🚀 HotSpotter Airflow 설정 가이드

Apache Airflow를 사용하여 HotSpotter 자동 수집 파이프라인을 설정하는 방법입니다.

## 📋 사전 준비

1. **Python 가상환경 활성화**
```bash
source dev/bin/activate
```

2. **Airflow 설치**
```bash
pip install -r requirements-airflow.txt
```

## 🛠️ Airflow 초기 설정

### 1. 환경 변수 설정
```bash
# Airflow 홈 디렉토리 설정
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow

# Airflow 초기화
airflow db init
```

### 2. 관리자 계정 생성
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123
```

### 3. DAG 폴더 설정
```bash
# airflow.cfg 파일에서 dags_folder 경로 확인/수정
# dags_folder = /Users/duck/wattabuzz/dags
```

## 🔧 DAG 배포

### 1. DAG 파일 경로 수정
`dags/hotspotter_dag.py` 파일에서 프로젝트 경로를 실제 경로로 수정:

```python
# 실제 프로젝트 경로로 수정
sys.path.append('/Users/duck/wattabuzz')  # 현재 경로에 맞게 수정
```

### 2. 환경 변수 파일 복사
```bash
# .env 파일이 Airflow worker에서도 접근 가능하도록 설정
cp .env /Users/duck/wattabuzz/.env
```

## 🚀 Airflow 실행

### 1. Airflow 웹서버 시작 (터미널 1)
```bash
source dev/bin/activate
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow
airflow webserver --port 8080
```

### 2. Airflow 스케줄러 시작 (터미널 2)
```bash
source dev/bin/activate
export AIRFLOW_HOME=/Users/duck/wattabuzz/airflow
airflow scheduler
```

### 3. Streamlit 대시보드 실행 (터미널 3)
```bash
source dev/bin/activate
streamlit run app.py
```

## 📊 사용 방법

### 1. Airflow 웹 UI 접속
- URL: http://localhost:8080
- 계정: admin / admin123

### 2. DAG 활성화
1. `hotspotter_collection` DAG 찾기
2. 토글 버튼을 클릭하여 DAG 활성화
3. 1시간마다 자동 실행 확인

### 3. Streamlit 대시보드 확인
- URL: http://localhost:8501
- 실시간으로 수집된 핫한 콘텐츠 확인

## 🔍 모니터링

### Airflow 웹 UI에서 확인 가능한 정보:
- **DAG 실행 상태**: 성공/실패/진행중
- **Task 별 실행 시간**: 각 키워드별 수집 시간
- **로그**: 상세한 실행 로그
- **실행 히스토리**: 과거 실행 기록

### Streamlit 대시보드에서 확인 가능한 정보:
- **실시간 데이터**: 최신 핫한 영상/댓글
- **키워드별 통계**: 수집된 데이터 현황
- **핫점수 순위**: 가장 핫한 콘텐츠 순위

## 🛠️ 문제 해결

### 1. DAG가 보이지 않는 경우
```bash
# DAG 파일 문법 확인
python dags/hotspotter_dag.py

# Airflow DAG 새로고침
airflow dags reserialize
```

### 2. 수집 실패 시
- Airflow 웹 UI에서 해당 Task 로그 확인
- YouTube API 키 유효성 확인
- 데이터베이스 연결 상태 확인

### 3. 스케줄 변경
`dags/hotspotter_dag.py`에서 `schedule_interval` 수정:
```python
# 30분마다 실행
schedule_interval=timedelta(minutes=30)

# 매일 오전 9시 실행
schedule_interval='0 9 * * *'
```

## 🎯 운영 팁

1. **리소스 관리**: 동시 실행되는 Task 수 조정
2. **알림 설정**: 실패 시 이메일/슬랙 알림 설정
3. **로그 모니터링**: 정기적인 로그 확인으로 문제 예방
4. **백업**: 중요한 설정과 데이터 정기 백업

## 📈 확장 계획

- Twitter, Reddit 등 추가 플랫폼 연동
- 실시간 알림 시스템 구축
- 더 많은 키워드 추가
- 머신러닝 기반 핫점수 개선 