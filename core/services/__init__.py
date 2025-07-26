# Services 패키지
"""
백그라운드 서비스 모듈
- auto_collector: 자동 데이터 수집 서비스
"""

from .auto_collector import AutoCollector

__all__ = ['AutoCollector'] 