# 자동 데이터 수집 서비스
"""
백그라운드에서 주기적으로 미리 정의된 키워드들의 핫한 콘텐츠를 수집하여 데이터베이스에 저장
"""

import schedule
import time
import threading
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from core.collectors.youtube_collector import YouTubeCollector
from core.database.database_manager import DatabaseManager
from config.keywords import TARGET_KEYWORDS, COLLECTION_SETTINGS, KEYWORD_SPECIFIC_SETTINGS

class AutoCollector:
    def __init__(self):
        self.youtube_collector = YouTubeCollector()
        self.db_manager = DatabaseManager()
        self.is_running = False
        self.collection_thread = None
        
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def start_auto_collection(self):
        """자동 수집 시작"""
        if self.is_running:
            self.logger.warning("Auto collection is already running")
            return
            
        self.logger.info("Starting auto collection service...")
        
        # 스케줄 설정
        interval_hours = COLLECTION_SETTINGS["collection_interval_hours"]
        schedule.every(interval_hours).hours.do(self._collect_all_keywords)
        
        # 시작 시 즉시 한 번 수집
        self._collect_all_keywords()
        
        # 백그라운드 스레드에서 스케줄러 실행
        self.is_running = True
        self.collection_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.collection_thread.start()
        
        self.logger.info(f"Auto collection started with {interval_hours}h interval")
        
    def stop_auto_collection(self):
        """자동 수집 중지"""
        self.is_running = False
        schedule.clear()
        self.logger.info("Auto collection stopped")
        
    def _run_scheduler(self):
        """스케줄러 실행 (백그라운드 스레드)"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 체크
            
    def _collect_all_keywords(self):
        """모든 타겟 키워드에 대해 데이터 수집"""
        self.logger.info("Starting collection for all keywords...")
        
        for keyword in TARGET_KEYWORDS:
            try:
                self._collect_keyword_data(keyword)
            except Exception as e:
                self.logger.error(f"Error collecting data for keyword '{keyword}': {str(e)}")
                
        # 오래된 데이터 정리
        self._cleanup_old_data()
        
        self.logger.info("Collection completed for all keywords")
        
    def _collect_keyword_data(self, keyword: str):
        """특정 키워드에 대한 데이터 수집"""
        self.logger.info(f"Collecting data for keyword: {keyword}")
        
        # 키워드별 설정 가져오기
        keyword_settings = KEYWORD_SPECIFIC_SETTINGS.get(keyword, {})
        max_videos = keyword_settings.get("max_videos", COLLECTION_SETTINGS["max_videos_per_keyword"])
        
        # 핫한 콘텐츠 수집
        hot_content = self.youtube_collector.find_hot_content(keyword, max_videos)
        
        # 데이터베이스에 저장
        self._save_hot_content_to_db(keyword, hot_content)
        
        self.logger.info(f"Saved {len(hot_content.get('hot_videos', []))} videos and "
                        f"{len(hot_content.get('hot_comments', []))} comments for '{keyword}'")
        
    def _save_hot_content_to_db(self, keyword: str, hot_content: Dict[str, Any]):
        """핫한 콘텐츠를 데이터베이스에 저장"""
        try:
            # 기존 데이터 삭제 (최신 데이터로 교체)
            self._delete_old_keyword_data(keyword)
            
            # 새로운 데이터 저장
            self.db_manager.save_hot_content_results(keyword, hot_content)
            
        except Exception as e:
            self.logger.error(f"Error saving hot content for '{keyword}': {str(e)}")
            
    def _delete_old_keyword_data(self, keyword: str):
        """특정 키워드의 기존 데이터 삭제"""
        try:
            self.db_manager.delete_keyword_data(keyword)
        except Exception as e:
            self.logger.error(f"Error deleting old data for '{keyword}': {str(e)}")
            
    def _cleanup_old_data(self):
        """오래된 데이터 정리"""
        try:
            retention_days = COLLECTION_SETTINGS["data_retention_days"]
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # retention_days 이전 데이터 삭제
            self.db_manager.cleanup_old_data(cutoff_date)
            
            self.logger.info(f"Cleaned up data older than {retention_days} days")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {str(e)}")
            
    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환"""
        return {
            "is_running": self.is_running,
            "target_keywords": TARGET_KEYWORDS,
            "collection_interval_hours": COLLECTION_SETTINGS["collection_interval_hours"],
            "last_collection_time": self.db_manager.get_last_collection_time(),
            "total_stored_videos": self.db_manager.get_total_stored_videos(),
            "total_stored_comments": self.db_manager.get_total_stored_comments()
        } 