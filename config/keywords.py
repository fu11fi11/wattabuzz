# 키워드 관리 설정
"""
HotSpotter에서 자동으로 수집할 키워드들과 관련 설정
"""

# 자동 수집할 키워드 목록
TARGET_KEYWORDS = [
    "WSWF",
    "Kyoka", 
    "Kaea Pearce"
]

# 수집 설정
COLLECTION_SETTINGS = {
    "max_videos_per_keyword": 10,  # 키워드당 최대 영상 수 
    "collection_interval_hours": 3,  # 수집 주기 (시간)
    "hot_videos_limit": 20,  # 저장할 핫한 영상 수
    "hot_comments_limit": 10,  # 저장할 핫한 댓글 수
    "data_retention_days": 7,  # 데이터 보관 기간 (일)
}

# 키워드별 추가 설정
KEYWORD_SPECIFIC_SETTINGS = {
    "WSWF": {
        "max_videos": 30,  # API 할당량 고려
    },
    "Kyoka": {
        "max_videos": 30,  # API 할당량 고려
    },
} 