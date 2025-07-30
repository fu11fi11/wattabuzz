import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class DatabaseManager:
    
    def __init__(self, database_url: str = None):
        """데이터베이스 매니저 초기화
        
        Args:
            database_url: 데이터베이스 연결 URL
        """
        
        if database_url is None:
            # 환경변수에서 읽기, PostgreSQL 필수
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL 환경변수가 필요합니다. PostgreSQL 연결 문자열을 설정해주세요.")
        
        self.database_url = database_url
        
        try:
            self.engine = create_engine(database_url, pool_pre_ping=True)
            print("PostgreSQL 연결 시도 중...")
            
            # 데이터베이스 초기화
            self._init_database()
            
        except Exception as e:
            print(f"데이터베이스 연결 실패: {e}")
            raise Exception(f"PostgreSQL 연결에 실패했습니다. DATABASE_URL을 확인해주세요: {e}")
    
    def _init_database(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    self._create_postgresql_tables(conn)
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                print("PostgreSQL 데이터베이스 초기화 완료")
                
        except SQLAlchemyError as e:
            print(f"데이터베이스 초기화 실패: {e}")
            raise
    
    def _create_postgresql_tables(self, conn):
        """PostgreSQL 테이블 생성"""
        # 원본 소셜미디어 게시글 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS social_media_posts (
                id SERIAL PRIMARY KEY,
                platform VARCHAR(50) NOT NULL,
                keyword VARCHAR(200) NOT NULL,
                content TEXT NOT NULL,
                author VARCHAR(100),
                timestamp TIMESTAMP WITH TIME ZONE,
                likes INTEGER DEFAULT 0,
                engagement_metrics JSONB,
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        '''))
        
        # 감성분석 결과 테이블 (현재 사용하지 않음)
        # conn.execute(text('''
        #     CREATE TABLE IF NOT EXISTS sentiment_analysis (
        #         id SERIAL PRIMARY KEY,
        #         post_id INTEGER REFERENCES social_media_posts(id),
        #         sentiment VARCHAR(20) NOT NULL,
        #         confidence REAL NOT NULL,
        #         scores JSONB,
        #         analyzed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        #     )
        # '''))
        
        # 키워드 분석 요약 테이블 (현재 사용하지 않음)
        # conn.execute(text('''
        #     CREATE TABLE IF NOT EXISTS keyword_analysis (
        #         id SERIAL PRIMARY KEY,
        #         keyword VARCHAR(200) NOT NULL,
        #         total_posts INTEGER DEFAULT 0,
        #         positive_count INTEGER DEFAULT 0,
        #         negative_count INTEGER DEFAULT 0,
        #         neutral_count INTEGER DEFAULT 0,
        #         avg_confidence REAL DEFAULT 0,
        #         analysis_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        #     )
        # '''))
        
        # 핫한 콘텐츠 영상 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS hot_videos (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(200) NOT NULL,
                video_id VARCHAR(50) NOT NULL,
                title VARCHAR(500) NOT NULL,
                channel VARCHAR(200),
                published_at TIMESTAMP WITH TIME ZONE,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                hot_score REAL DEFAULT 0,
                thumbnail VARCHAR(500),
                url VARCHAR(500),
                description TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(keyword, video_id)
            )
        '''))
        
        # 핫한 콘텐츠 댓글 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS hot_comments (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(200) NOT NULL,
                video_id VARCHAR(50) NOT NULL,
                comment_id VARCHAR(50) NOT NULL,
                author VARCHAR(100),
                content TEXT NOT NULL,
                like_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                published_at TIMESTAMP WITH TIME ZONE,
                hot_score REAL DEFAULT 0,
                video_title VARCHAR(500),
                video_url VARCHAR(500),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(keyword, comment_id)
            )
        '''))
        
        # 기존 테이블에 UNIQUE 제약조건 추가 (중복 방지)
        try:
            # hot_videos 테이블에 UNIQUE 제약조건 추가
            conn.execute(text('''
                ALTER TABLE hot_videos 
                ADD CONSTRAINT unique_video_per_keyword 
                UNIQUE (keyword, video_id)
            '''))
        except Exception:
            # 이미 제약조건이 있거나 오류가 발생한 경우 무시
            pass
            
        try:
            # hot_comments 테이블에 UNIQUE 제약조건 추가
            conn.execute(text('''
                ALTER TABLE hot_comments 
                ADD CONSTRAINT unique_comment_per_keyword 
                UNIQUE (keyword, comment_id)
            '''))
        except Exception:
            # 이미 제약조건이 있거나 오류가 발생한 경우 무시
            pass
        
        # 인덱스 생성 
        # conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_keyword ON social_media_posts(keyword)'))
        # conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_platform ON social_media_posts(platform)'))
        # conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sentiment_post ON sentiment_analysis(post_id)'))
        # conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_videos_keyword ON hot_videos(keyword)'))
        # conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_comments_keyword ON hot_comments(keyword)'))
    

    
    # def save_analysis_results(self, keyword: str, analyzed_data: List[Dict[str, Any]]) -> bool:
    #     """분석 결과를 데이터베이스에 저장 (현재 사용하지 않음)"""
    #     # 감성분석 관련 메서드 - 현재 사용하지 않음
    #     pass
    
    def save_hot_content_results(self, keyword: str, hot_content: Dict[str, Any]) -> bool:
        """핫한 콘텐츠 결과를 데이터베이스에 저장"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    # 영상 데이터 저장 (중복 시 더 높은 hot_score로 업데이트)
                    for video in hot_content.get('hot_videos', []):
                        video_query = text("""
                            INSERT INTO hot_videos (
                                keyword, video_id, title, channel, published_at,
                                view_count, like_count, comment_count, hot_score,
                                thumbnail, url, description, created_at
                            ) VALUES (
                                :keyword, :video_id, :title, :channel, :published_at,
                                :view_count, :like_count, :comment_count, :hot_score,
                                :thumbnail, :url, :description, :created_at
                            )
                            ON CONFLICT (keyword, video_id) 
                            DO UPDATE SET 
                                hot_score = CASE 
                                    WHEN EXCLUDED.hot_score > hot_videos.hot_score 
                                    THEN EXCLUDED.hot_score 
                                    ELSE hot_videos.hot_score 
                                END,
                                view_count = EXCLUDED.view_count,
                                like_count = EXCLUDED.like_count,
                                comment_count = EXCLUDED.comment_count,
                                created_at = EXCLUDED.created_at
                        """)
                        
                        conn.execute(video_query, {
                            'keyword': keyword,
                            'video_id': video['video_id'],
                            'title': video['title'],
                            'channel': video['channel'],
                            'published_at': video['published_at'],
                            'view_count': video['view_count'],
                            'like_count': video['like_count'],
                            'comment_count': video['comment_count'],
                            'hot_score': video['hot_score'],
                            'thumbnail': video['thumbnail'],
                            'url': video['url'],
                            'description': video['description'],
                            'created_at': datetime.now().isoformat()
                        })
                    
                    # 댓글 데이터 저장 (중복 시 더 높은 hot_score로 업데이트)
                    for comment in hot_content.get('hot_comments', []):
                        comment_query = text("""
                            INSERT INTO hot_comments (
                                keyword, video_id, comment_id, author, content,
                                like_count, reply_count, published_at, hot_score,
                                video_title, video_url, created_at
                            ) VALUES (
                                :keyword, :video_id, :comment_id, :author, :content,
                                :like_count, :reply_count, :published_at, :hot_score,
                                :video_title, :video_url, :created_at
                            )
                            ON CONFLICT (keyword, comment_id) 
                            DO UPDATE SET 
                                hot_score = CASE 
                                    WHEN EXCLUDED.hot_score > hot_comments.hot_score 
                                    THEN EXCLUDED.hot_score 
                                    ELSE hot_comments.hot_score 
                                END,
                                like_count = EXCLUDED.like_count,
                                reply_count = EXCLUDED.reply_count,
                                created_at = EXCLUDED.created_at
                        """)
                        
                        conn.execute(comment_query, {
                            'keyword': keyword,
                            'video_id': comment['video_id'],
                            'comment_id': comment['comment_id'],
                            'author': comment['author'],
                            'content': comment['content'],
                            'like_count': comment['like_count'],
                            'reply_count': comment['reply_count'],
                            'published_at': comment['published_at'],
                            'hot_score': comment['hot_score'],
                            'video_title': comment['video_title'],
                            'video_url': comment['video_url'],
                            'created_at': datetime.now().isoformat()
                        })
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Error saving hot content results: {e}")
            return False
    
    def delete_keyword_data(self, keyword: str) -> bool:
        """특정 키워드의 데이터 삭제"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    # 영상 데이터 삭제
                    conn.execute(text("DELETE FROM hot_videos WHERE keyword = :keyword"), 
                               {'keyword': keyword})
                    
                    # 댓글 데이터 삭제
                    conn.execute(text("DELETE FROM hot_comments WHERE keyword = :keyword"), 
                               {'keyword': keyword})
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Error deleting keyword data: {e}")
            return False
    
    def cleanup_old_data(self, cutoff_date: datetime) -> bool:
        """오래된 데이터 정리"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    cutoff_str = cutoff_date.isoformat()
                    
                    # 오래된 영상 데이터 삭제
                    conn.execute(text("DELETE FROM hot_videos WHERE created_at < :cutoff"), 
                               {'cutoff': cutoff_str})
                    
                    # 오래된 댓글 데이터 삭제
                    conn.execute(text("DELETE FROM hot_comments WHERE created_at < :cutoff"), 
                               {'cutoff': cutoff_str})
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Error cleaning up old data: {e}")
            return False
    
    def get_stored_hot_content(self, keyword: str = None) -> Dict[str, Any]:
        """저장된 핫한 콘텐츠 조회 (중복 제거)"""
        try:
            with self.engine.connect() as conn:
                # 영상 데이터 조회 (video_id 기준으로 중복 제거, 최고 hot_score 우선)
                if keyword:
                    video_query = """
                        SELECT DISTINCT ON (video_id) *
                        FROM hot_videos 
                        WHERE keyword = :keyword
                        ORDER BY video_id, hot_score DESC
                    """
                    params = {'keyword': keyword}
                else:
                    video_query = """
                        SELECT DISTINCT ON (video_id) *
                        FROM hot_videos 
                        ORDER BY video_id, hot_score DESC
                    """
                    params = {}
                
                videos_result = conn.execute(text(video_query), params)
                videos_raw = [dict(row._mapping) for row in videos_result]
                
                # hot_score 순으로 재정렬
                videos = sorted(videos_raw, key=lambda x: x['hot_score'], reverse=True)
                
                # 댓글 데이터 조회 (comment_id 기준으로 중복 제거, 최고 hot_score 우선)
                if keyword:
                    comment_query = """
                        SELECT DISTINCT ON (comment_id) *
                        FROM hot_comments 
                        WHERE keyword = :keyword
                        ORDER BY comment_id, hot_score DESC
                    """
                else:
                    comment_query = """
                        SELECT DISTINCT ON (comment_id) *
                        FROM hot_comments 
                        ORDER BY comment_id, hot_score DESC
                    """
                
                comments_result = conn.execute(text(comment_query), params)
                comments_raw = [dict(row._mapping) for row in comments_result]
                
                # hot_score 순으로 재정렬
                comments = sorted(comments_raw, key=lambda x: x['hot_score'], reverse=True)
                
                return {
                    'hot_videos': videos,
                    'hot_comments': comments,
                    'keyword': keyword,
                    'total_videos': len(videos),
                    'total_comments': len(comments)
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting stored hot content: {e}")
            return {'hot_videos': [], 'hot_comments': [], 'keyword': keyword}
    
    def get_last_collection_time(self) -> str:
        """마지막 수집 시간 조회"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT MAX(created_at) as last_time 
                    FROM hot_videos
                """))
                
                row = result.fetchone()
                if row and row[0]:
                    # datetime 객체를 문자열로 변환
                    last_time = row[0]
                    if isinstance(last_time, datetime):
                        return last_time.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        return str(last_time)
                return "수집 기록 없음"
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting last collection time: {e}")
            return "조회 실패"
    
    def get_total_stored_videos(self) -> int:
        """저장된 총 영상 수"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM hot_videos"))
                return result.fetchone()[0]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting total stored videos: {e}")
            return 0
    
    def get_total_stored_comments(self) -> int:
        """저장된 총 댓글 수"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM hot_comments"))
                return result.fetchone()[0]
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting total stored comments: {e}")
            return 0

    def get_keywords_stats(self) -> List[Dict[str, Any]]:
        """키워드별 통계 조회"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        keyword,
                        COUNT(*) as video_count,
                        AVG(hot_score) as avg_hot_score,
                        MAX(created_at) as last_update
                    FROM hot_videos
                    GROUP BY keyword
                    ORDER BY avg_hot_score DESC
                """))
                
                # datetime 필드를 문자열로 변환
                stats = []
                for row in result:
                    row_dict = dict(row._mapping)
                    if row_dict.get('last_update') and isinstance(row_dict['last_update'], datetime):
                        row_dict['last_update'] = row_dict['last_update'].strftime("%Y-%m-%d %H:%M:%S")
                    stats.append(row_dict)
                
                return stats
                
        except SQLAlchemyError as e:
            logger.error(f"Error getting keywords stats: {e}")
            return []
    
    # def get_keyword_history(self, keyword: str, limit: int = 10) -> pd.DataFrame:
    #     """키워드 분석 히스토리 조회 (현재 사용하지 않음)"""
    #     # 감성분석 관련 메서드 - 현재 사용하지 않음
    #     return pd.DataFrame()
    
    def get_recent_posts(self, keyword: str = None, limit: int = 50) -> pd.DataFrame:
        """최근 게시글 조회
        
        Args:
            keyword: 키워드 필터 (None이면 전체)
            limit: 최대 조회 개수
            
        Returns:
            게시글 데이터프레임
        """
        try:
            with self.engine.connect() as conn:
                if keyword:
                    query = '''
                        SELECT p.*
                        FROM social_media_posts p
                        WHERE p.keyword = %s
                        ORDER BY p.created_at DESC 
                        LIMIT %s
                    '''
                    params = (keyword, limit)
                else:
                    query = '''
                        SELECT p.*
                        FROM social_media_posts p
                        ORDER BY p.created_at DESC 
                        LIMIT %s
                    '''
                    params = (limit,)
                
                df = pd.read_sql_query(query, conn, params=params)
                return df
                
        except Exception as e:
            print(f"게시글 조회 실패: {e}")
            return pd.DataFrame()
    
    # def get_sentiment_trends(self, keyword: str, days: int = 7) -> pd.DataFrame:
    #     """감성 트렌드 조회 (현재 사용하지 않음)"""
    #     # 감성분석 관련 메서드 - 현재 사용하지 않음
    #     return pd.DataFrame()
    
    # def get_top_keywords(self, limit: int = 10) -> pd.DataFrame:
    #     """인기 키워드 조회 (현재 사용하지 않음)"""
    #     # 감성분석 관련 메서드 - 현재 사용하지 않음
    #     return pd.DataFrame()
    
    def clean_old_data(self, days: int = 30) -> bool:
        """오래된 데이터 정리
        
        Args:
            days: 보관할 일수
            
        Returns:
            정리 성공 여부
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    # 오래된 데이터 삭제 (SQLAlchemy text 사용)
                    conn.execute(text(f'''
                        DELETE FROM social_media_posts 
                        WHERE created_at < NOW() - INTERVAL '{days} days'
                    '''))
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                print(f"{days}일 이전 데이터 정리 완료")
                return True
                
        except Exception as e:
            print(f"데이터 정리 실패: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """데이터베이스 통계 조회
        
        Returns:
            데이터베이스 통계 정보
        """
        try:
            with self.engine.connect() as conn:
                # 테이블별 레코드 수
                result = conn.execute(text('SELECT COUNT(*) FROM social_media_posts'))
                posts_count = result.scalar()
                
                # 감성분석 관련 통계는 현재 사용하지 않음
                analysis_count = 0
                keywords_count = 0
                
                # 최근 분석일
                result = conn.execute(text('SELECT MAX(created_at) FROM social_media_posts'))
                last_post = result.scalar()
                
                return {
                    'total_posts': posts_count,
                    'total_analysis': analysis_count,
                    'unique_keywords': keywords_count,
                    'last_post_date': last_post,
                    'database_size': 0  # PostgreSQL에서는 파일 크기 대신 다른 메트릭 사용
                }
                
        except Exception as e:
            print(f"통계 조회 실패: {e}")
            return {}

 