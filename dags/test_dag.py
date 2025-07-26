"""
간단한 테스트 DAG - DAG 로딩 테스트용
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# 기본 설정
default_args = {
    'owner': 'test',
    'start_date': datetime(2025, 7, 25),
    'retries': 0,
}

# DAG 정의
dag = DAG(
    'test_simple_dag',
    default_args=default_args,
    description='Simple test DAG',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['test'],
)

# 간단한 태스크
test_task = DummyOperator(
    task_id='test_task',
    dag=dag,
)

print("✅ 테스트 DAG 로딩 성공!") 