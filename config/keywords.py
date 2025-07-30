# 키워드 관리 설정
"""
HotSpotter에서 자동으로 수집할 키워드들과 관련 설정
"""

# 자동 수집할 키워드 목록
TARGET_KEYWORDS = [
    "WSWF",
    "Kyoka", 
    "Kaea Pearce",
    "Ibuki",
    "Rie Hata",
]

# 수집 설정(기본)
COLLECTION_SETTINGS = {
    "max_videos": 9,  # 키워드당 최대 영상 수 
    "collection_interval_hours": 6,  # 수집 주기 (시간)
    "hot_videos_limit": 5,  # 저장할 핫한 영상 수
    "hot_comments_limit": 10,  # 저장할 핫한 댓글 수
    "data_retention_days": 7,  # 데이터 보관 기간 (일)
}

# 키워드별 추가 설정
KEYWORD_SPECIFIC_SETTINGS = {
    "Ibuki": {
        "max_videos": 15,
        "hot_videos_limit": 8,
        "hot_comments_limit": 15,
    },
    "Kyoka": {
        "max_videos": 30,
        "hot_videos_limit": 20,
        "hot_comments_limit": 20,
    },
} 