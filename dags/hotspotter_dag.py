"""
HotSpotter 자동 수집 Airflow DAG
YouTube 핫한 콘텐츠를 주기적으로 수집하는 워크플로우
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import sys
import os

# 프로젝트 루트를 Python path에 추가 (동적 경로)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # dags 폴더의 상위 디렉토리
sys.path.append(project_root)

print(f"DAG 로딩: 프로젝트 루트 = {project_root}")  # 디버깅용

try:
    from core.collectors.youtube_collector import YouTubeCollector
    from core.database.database_manager import DatabaseManager
    from config.keywords import TARGET_KEYWORDS, COLLECTION_SETTINGS
    print("✅ 모든 모듈 import 성공")  # 디버깅용
except ImportError as e:
    print(f"❌ Import 에러: {e}")  # 디버깅용
    raise

# DAG 기본 설정
default_args = {
    'owner': 'hotspotter',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,  # 과거 실행 건너뛰기
}

# DAG 정의
dag = DAG(
    'hotspotter_collection',
    default_args=default_args,
    description='YouTube 핫한 콘텐츠 자동 수집 파이프라인',
    schedule_interval=timedelta(hours=1),  # 1시간마다 실행
    max_active_runs=1,  # 동시 실행 방지
    tags=['youtube', 'hotspotter', 'data-collection'],
)

def collect_keyword_data(keyword: str, **context):
    """특정 키워드의 핫한 콘텐츠 수집"""
    print(f"🔍 키워드 '{keyword}' 데이터 수집 시작...")
    
    try:
        # YouTube 수집기 초기화
        collector = YouTubeCollector()
        db_manager = DatabaseManager()
        
        # 키워드별 설정 가져오기
        from config.keywords import KEYWORD_SPECIFIC_SETTINGS
        keyword_settings = KEYWORD_SPECIFIC_SETTINGS.get(keyword, {})
        max_videos = keyword_settings.get("max_videos", COLLECTION_SETTINGS["max_videos_per_keyword"])
        
        # 핫한 콘텐츠 수집
        hot_content = collector.find_hot_content(keyword, max_videos)
        
        # 기존 데이터 삭제 (최신 데이터로 교체)
        db_manager.delete_keyword_data(keyword)
        
        # 새로운 데이터 저장
        success = db_manager.save_hot_content_results(keyword, hot_content)
        
        if success:
            video_count = len(hot_content.get('hot_videos', []))
            comment_count = len(hot_content.get('hot_comments', []))
            print(f"✅ '{keyword}' 수집 완료: {video_count}개 영상, {comment_count}개 댓글")
            
            # XCom에 결과 저장 (다음 태스크에서 사용 가능)
            return {
                'keyword': keyword,
                'video_count': video_count,
                'comment_count': comment_count,
                'status': 'success'
            }
        else:
            raise Exception(f"데이터베이스 저장 실패: {keyword}")
            
    except Exception as e:
        print(f"❌ '{keyword}' 수집 중 오류: {str(e)}")
        raise

def cleanup_old_data(**context):
    """오래된 데이터 정리"""
    print("🧹 오래된 데이터 정리 시작...")
    
    try:
        db_manager = DatabaseManager()
        
        # 7일 이전 데이터 삭제
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=COLLECTION_SETTINGS["data_retention_days"])
        
        success = db_manager.cleanup_old_data(cutoff_date)
        
        if success:
            print(f"✅ {COLLECTION_SETTINGS['data_retention_days']}일 이전 데이터 정리 완료")
            return {'status': 'success', 'cutoff_date': cutoff_date.isoformat()}
        else:
            raise Exception("데이터 정리 실패")
            
    except Exception as e:
        print(f"❌ 데이터 정리 중 오류: {str(e)}")
        raise

def generate_collection_report(**context):
    """수집 결과 리포트 생성"""
    print("📊 수집 결과 리포트 생성...")
    
    try:
        db_manager = DatabaseManager()
        
        # 전체 통계
        total_videos = db_manager.get_total_stored_videos()
        total_comments = db_manager.get_total_stored_comments()
        last_collection = db_manager.get_last_collection_time()
        
        # 키워드별 통계
        keywords_stats = db_manager.get_keywords_stats()
        
        print(f"📈 수집 리포트:")
        print(f"  전체 영상: {total_videos}개")
        print(f"  전체 댓글: {total_comments}개")
        print(f"  마지막 수집: {last_collection}")
        
        for stat in keywords_stats:
            print(f"  📊 {stat['keyword']}: {stat['video_count']}개 영상, "
                  f"평균 핫점수 {stat['avg_hot_score']:.1f}")
        
        return {
            'total_videos': total_videos,
            'total_comments': total_comments,
            'keywords_stats': keywords_stats
        }
        
    except Exception as e:
        print(f"❌ 리포트 생성 중 오류: {str(e)}")
        raise

# Task 정의
start_task = DummyOperator(
    task_id='start_collection',
    dag=dag,
)

# 각 키워드별 수집 태스크 (병렬 실행)
collection_tasks = []
for keyword in TARGET_KEYWORDS:
    # task_id에서 공백과 특수문자를 언더스코어로 변경 (안전장치)
    safe_keyword = keyword.lower().replace(' ', '_').replace('-', '_')
    # 영숫자와 언더스코어만 남기기
    safe_keyword = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_keyword)
    
    task = PythonOperator(
        task_id=f'collect_{safe_keyword}',
        python_callable=collect_keyword_data,
        op_kwargs={'keyword': keyword},  # 원본 키워드는 그대로 전달
        dag=dag,
    )
    collection_tasks.append(task)

# 데이터 정리 태스크
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
)

# 리포트 생성 태스크
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_collection_report,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end_collection',
    dag=dag,
)

# Task 의존성 설정
start_task >> collection_tasks >> cleanup_task >> report_task >> end_task 