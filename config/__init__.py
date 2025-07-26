# Config 패키지
"""
설정 관리 모듈
- settings: 애플리케이션 설정
- keywords: 타겟 키워드 설정
"""

from .settings import load_settings
from .keywords import TARGET_KEYWORDS, COLLECTION_SETTINGS, KEYWORD_SPECIFIC_SETTINGS

__all__ = ['load_settings', 'TARGET_KEYWORDS', 'COLLECTION_SETTINGS', 'KEYWORD_SPECIFIC_SETTINGS'] 