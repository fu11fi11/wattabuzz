# /Users/duck/wattabuzz/__init__.py
import sys
import os

# 프로젝트 루트를 Python path에 추가
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"✅ 프로젝트 루트 경로 추가: {project_root}") 

# 프로젝트 정보
__version__ = "1.0.0"
__author__ = "Gimme Job"
__description__ = "YouTube 핫한 콘텐츠 모니터링 시스템"
