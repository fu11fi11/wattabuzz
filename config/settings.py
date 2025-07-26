import os
from typing import Dict, Any
from dotenv import load_dotenv

def load_settings() -> Dict[str, Any]:
    """애플리케이션 설정 로드
    
    Returns:
        설정 딕셔너리
    """
    # .env 파일 로드
    load_dotenv()
    
    settings = {
        # API 키들
        'youtube_api_key': os.getenv('YOUTUBE_API_KEY'),
        'twitter_bearer_token': os.getenv('TWITTER_BEARER_TOKEN'),
        
        # 데이터베이스 설정
        'database_path': os.getenv('DATABASE_PATH', 'data/wattabuzz.db'),
        
        # 모델 설정
        'sentiment_model': os.getenv('SENTIMENT_MODEL', 'matthewburke/korean-sentiment-analysis-dataset'),
        'model_cache_dir': os.getenv('MODEL_CACHE_DIR', 'cache/models'),
        
        # 수집 설정
        'default_max_results': int(os.getenv('DEFAULT_MAX_RESULTS', '100')),
        'collection_interval': int(os.getenv('COLLECTION_INTERVAL', '300')),  # 5분
        
        # 앱 설정
        'app_title': os.getenv('APP_TITLE', '🌊 WattaBuzz'),
        'debug': os.getenv('DEBUG', 'True').lower() == 'true',
        
        # 기본 키워드
        'default_keywords': [
            '삼성 갤럭시 S24',
            '아이폰 15',
            '테슬라 모델 Y',
            'BTS',
            '엔비디아'
        ]
    }
    
    return settings

def get_api_status() -> Dict[str, bool]:
    """API 키 상태 확인
    
    Returns:
        API 키 유효성 상태
    """
    settings = load_settings()
    
    return {
        'youtube_api': bool(settings['youtube_api_key']),
        'twitter_api': bool(settings['twitter_bearer_token']),
    }

# 전역 설정 객체
SETTINGS = load_settings() 