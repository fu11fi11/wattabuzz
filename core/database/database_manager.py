import json
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sqlite3
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """데이터베이스 관리자
    
    나중에 FastAPI로 마이그레이션할 때도 그대로 사용 가능한 독립적인 모듈
    PostgreSQL을 사용하여 확장성 있는 데이터 저장소 구축
    """
    
    def __init__(self, database_url: str = None):
        """데이터베이스 매니저 초기화
        
        Args:
            database_url: 데이터베이스 연결 URL
        """
        from dotenv import load_dotenv
        load_dotenv()
        
        if database_url is None:
            # 환경변수에서 읽기, 없으면 SQLite 사용
            database_url = os.getenv('DATABASE_URL', 'sqlite:///data/wattabuzz.db')
        
        self.database_url = database_url
        self.is_postgresql = database_url.startswith('postgresql')
        
        try:
            if self.is_postgresql:
                self.engine = create_engine(database_url, pool_pre_ping=True)
                print("🐘 PostgreSQL 연결 시도 중...")
            else:
                # SQLite용 디렉토리 생성
                if database_url.startswith('sqlite:///'):
                    db_path = database_url.replace('sqlite:///', '')
                    os.makedirs(os.path.dirname(db_path), exist_ok=True)
                self.engine = create_engine(database_url)
                print("🗃️ SQLite 데이터베이스 사용")
            
            # 데이터베이스 초기화
            self._init_database()
            
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            # SQLite로 fallback
            fallback_url = 'sqlite:///data/wattabuzz.db'
            print(f"🔄 SQLite로 fallback: {fallback_url}")
            
            os.makedirs('data', exist_ok=True)
            self.database_url = fallback_url
            self.is_postgresql = False
            self.engine = create_engine(fallback_url)
            self._init_database()
    
    def _init_database(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    if self.is_postgresql:
                        # PostgreSQL 스키마
                        self._create_postgresql_tables(conn)
                    else:
                        # SQLite 스키마
                        self._create_sqlite_tables(conn)
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                print(f"✅ {'PostgreSQL' if self.is_postgresql else 'SQLite'} 데이터베이스 초기화 완료")
                
        except SQLAlchemyError as e:
            print(f"❌ 데이터베이스 초기화 실패: {e}")
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
        
        # 감성분석 결과 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS sentiment_analysis (
                id SERIAL PRIMARY KEY,
                post_id INTEGER REFERENCES social_media_posts(id),
                sentiment VARCHAR(20) NOT NULL,
                confidence REAL NOT NULL,
                scores JSONB,
                analyzed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        '''))
        
        # 키워드 분석 요약 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS keyword_analysis (
                id SERIAL PRIMARY KEY,
                keyword VARCHAR(200) NOT NULL,
                total_posts INTEGER DEFAULT 0,
                positive_count INTEGER DEFAULT 0,
                negative_count INTEGER DEFAULT 0,
                neutral_count INTEGER DEFAULT 0,
                avg_confidence REAL DEFAULT 0,
                analysis_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        '''))
        
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        '''))
        
        # 인덱스 생성
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_keyword ON social_media_posts(keyword)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_platform ON social_media_posts(platform)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sentiment_post ON sentiment_analysis(post_id)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_videos_keyword ON hot_videos(keyword)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_comments_keyword ON hot_comments(keyword)'))
    
    def _create_sqlite_tables(self, conn):
        """SQLite 테이블 생성"""
        # 원본 소셜미디어 게시글 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS social_media_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                keyword TEXT NOT NULL,
                content TEXT NOT NULL,
                author TEXT,
                timestamp TEXT,
                likes INTEGER DEFAULT 0,
                engagement_metrics TEXT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        
        # 감성분석 결과 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS sentiment_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER,
                sentiment TEXT NOT NULL,
                confidence REAL NOT NULL,
                scores TEXT,
                analyzed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (post_id) REFERENCES social_media_posts (id)
            )
        '''))
        
        # 키워드 분석 요약 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS keyword_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                total_posts INTEGER DEFAULT 0,
                positive_count INTEGER DEFAULT 0,
                negative_count INTEGER DEFAULT 0,
                neutral_count INTEGER DEFAULT 0,
                avg_confidence REAL DEFAULT 0,
                analysis_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        
        # 핫한 콘텐츠 영상 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS hot_videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                video_id TEXT NOT NULL,
                title TEXT NOT NULL,
                channel TEXT,
                published_at TEXT,
                view_count INTEGER DEFAULT 0,
                like_count INTEGER DEFAULT 0,
                comment_count INTEGER DEFAULT 0,
                hot_score REAL DEFAULT 0,
                thumbnail TEXT,
                url TEXT,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        
        # 핫한 콘텐츠 댓글 테이블
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS hot_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                video_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                author TEXT,
                content TEXT NOT NULL,
                like_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                published_at TEXT,
                hot_score REAL DEFAULT 0,
                video_title TEXT,
                video_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        '''))
        
        # 인덱스 생성
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_keyword ON social_media_posts(keyword)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_posts_platform ON social_media_posts(platform)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_sentiment_post ON sentiment_analysis(post_id)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_videos_keyword ON hot_videos(keyword)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_hot_comments_keyword ON hot_comments(keyword)'))
    
    def save_analysis_results(self, keyword: str, analyzed_data: List[Dict[str, Any]]) -> bool:
        """분석 결과를 데이터베이스에 저장
        
        Args:
            keyword: 분석한 키워드
            analyzed_data: 분석된 데이터 리스트
            
        Returns:
            저장 성공 여부
        """
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    cursor = conn.cursor()
                    
                    post_ids = []
                    sentiment_counts = {'positive': 0, 'negative': 0, 'neutral': 0}
                    total_confidence = 0
                    
                    for item in analyzed_data:
                        # 1. 원본 게시글 저장
                        cursor.execute('''
                            INSERT INTO social_media_posts 
                            (platform, keyword, content, author, timestamp, likes, engagement_metrics, metadata)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            item.get('platform', 'unknown'),
                            keyword,
                            item.get('content', ''),
                            item.get('author', 'anonymous'),
                            item.get('timestamp', datetime.now().isoformat()),
                            item.get('likes', 0),
                            json.dumps(item.get('engagement_metrics', {})),
                            json.dumps(item.get('metadata', {}))
                        ))
                        
                        post_id = cursor.lastrowid
                        post_ids.append(post_id)
                        
                        # 2. 감성분석 결과 저장
                        sentiment = item.get('sentiment', 'neutral')
                        confidence = item.get('confidence', 0.5)
                        scores = item.get('scores', {})
                        
                        cursor.execute('''
                            INSERT INTO sentiment_analysis 
                            (post_id, sentiment, confidence, scores)
                            VALUES (?, ?, ?, ?)
                        ''', (
                            post_id,
                            sentiment,
                            confidence,
                            json.dumps(scores)
                        ))
                        
                        # 통계 업데이트
                        sentiment_counts[sentiment] += 1
                        total_confidence += confidence
                    
                    # 3. 키워드 분석 요약 저장
                    avg_confidence = total_confidence / len(analyzed_data) if analyzed_data else 0
                    
                    cursor.execute('''
                        INSERT INTO keyword_analysis 
                        (keyword, total_posts, positive_count, negative_count, neutral_count, avg_confidence)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        keyword,
                        len(analyzed_data),
                        sentiment_counts['positive'],
                        sentiment_counts['negative'],
                        sentiment_counts['neutral'],
                        avg_confidence
                    ))
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                print(f"✅ 데이터베이스에 {len(analyzed_data)}개 레코드 저장 완료")
                return True
                
        except Exception as e:
            print(f"❌ 데이터베이스 저장 실패: {e}")
            return False
    
    def save_hot_content_results(self, keyword: str, hot_content: Dict[str, Any]) -> bool:
        """핫한 콘텐츠 결과를 데이터베이스에 저장"""
        try:
            with self.engine.connect() as conn:
                with conn.begin():  # 트랜잭션 시작
                    # 영상 데이터 저장
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
                    
                    # 댓글 데이터 저장
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
        """저장된 핫한 콘텐츠 조회"""
        try:
            with self.engine.connect() as conn:
                # 영상 데이터 조회
                video_query = "SELECT * FROM hot_videos"
                params = {}
                
                if keyword:
                    video_query += " WHERE keyword = :keyword"
                    params['keyword'] = keyword
                
                video_query += " ORDER BY hot_score DESC"
                
                videos_result = conn.execute(text(video_query), params)
                videos = [dict(row._mapping) for row in videos_result]
                
                # 댓글 데이터 조회  
                comment_query = "SELECT * FROM hot_comments"
                
                if keyword:
                    comment_query += " WHERE keyword = :keyword"
                
                comment_query += " ORDER BY hot_score DESC"
                
                comments_result = conn.execute(text(comment_query), params)
                comments = [dict(row._mapping) for row in comments_result]
                
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
    
    def get_keyword_history(self, keyword: str, limit: int = 10) -> pd.DataFrame:
        """키워드 분석 히스토리 조회
        
        Args:
            keyword: 조회할 키워드
            limit: 최대 조회 개수
            
        Returns:
            분석 히스토리 데이터프레임
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT * FROM keyword_analysis 
                    WHERE keyword = ? 
                    ORDER BY analysis_date DESC 
                    LIMIT ?
                '''
                df = pd.read_sql_query(query, conn, params=(keyword, limit))
                return df
                
        except Exception as e:
            print(f"❌ 히스토리 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_recent_posts(self, keyword: str = None, limit: int = 50) -> pd.DataFrame:
        """최근 게시글 조회
        
        Args:
            keyword: 키워드 필터 (None이면 전체)
            limit: 최대 조회 개수
            
        Returns:
            게시글 데이터프레임
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                if keyword:
                    query = '''
                        SELECT p.*, s.sentiment, s.confidence 
                        FROM social_media_posts p
                        LEFT JOIN sentiment_analysis s ON p.id = s.post_id
                        WHERE p.keyword = ?
                        ORDER BY p.created_at DESC 
                        LIMIT ?
                    '''
                    params = (keyword, limit)
                else:
                    query = '''
                        SELECT p.*, s.sentiment, s.confidence 
                        FROM social_media_posts p
                        LEFT JOIN sentiment_analysis s ON p.id = s.post_id
                        ORDER BY p.created_at DESC 
                        LIMIT ?
                    '''
                    params = (limit,)
                
                df = pd.read_sql_query(query, conn, params=params)
                return df
                
        except Exception as e:
            print(f"❌ 게시글 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_sentiment_trends(self, keyword: str, days: int = 7) -> pd.DataFrame:
        """감성 트렌드 조회
        
        Args:
            keyword: 키워드
            days: 조회할 일수
            
        Returns:
            트렌드 데이터프레임
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT 
                        DATE(p.created_at) as date,
                        s.sentiment,
                        COUNT(*) as count,
                        AVG(s.confidence) as avg_confidence
                    FROM social_media_posts p
                    JOIN sentiment_analysis s ON p.id = s.post_id
                    WHERE p.keyword = ? AND DATE(p.created_at) >= DATE('now', '-{} days')
                    GROUP BY DATE(p.created_at), s.sentiment
                    ORDER BY date DESC
                '''.format(days)
                
                df = pd.read_sql_query(query, conn, params=(keyword,))
                return df
                
        except Exception as e:
            print(f"❌ 트렌드 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_top_keywords(self, limit: int = 10) -> pd.DataFrame:
        """인기 키워드 조회
        
        Args:
            limit: 최대 조회 개수
            
        Returns:
            인기 키워드 데이터프레임
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = '''
                    SELECT 
                        keyword,
                        SUM(total_posts) as total_posts,
                        AVG(avg_confidence) as avg_confidence,
                        MAX(analysis_date) as last_analysis
                    FROM keyword_analysis
                    GROUP BY keyword
                    ORDER BY total_posts DESC
                    LIMIT ?
                '''
                
                df = pd.read_sql_query(query, conn, params=(limit,))
                return df
                
        except Exception as e:
            print(f"❌ 인기 키워드 조회 실패: {e}")
            return pd.DataFrame()
    
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
                    conn.execute(text('''
                        DELETE FROM sentiment_analysis 
                        WHERE post_id IN (
                            SELECT id FROM social_media_posts 
                            WHERE DATE(created_at) < DATE('now', '-{} days')
                        )
                    '''.format(days)))
                    
                    conn.execute(text('''
                        DELETE FROM social_media_posts 
                        WHERE DATE(created_at) < DATE('now', '-{} days')
                    '''.format(days)))
                    
                    conn.execute(text('''
                        DELETE FROM keyword_analysis 
                        WHERE DATE(analysis_date) < DATE('now', '-{} days')
                    '''.format(days)))
                    # 트랜잭션은 with 블록 종료 시 자동 커밋됨
                
                print(f"✅ {days}일 이전 데이터 정리 완료")
                return True
                
        except Exception as e:
            print(f"❌ 데이터 정리 실패: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """데이터베이스 통계 조회
        
        Returns:
            데이터베이스 통계 정보
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 테이블별 레코드 수
                cursor.execute('SELECT COUNT(*) FROM social_media_posts')
                posts_count = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM sentiment_analysis')
                analysis_count = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(DISTINCT keyword) FROM keyword_analysis')
                keywords_count = cursor.fetchone()[0]
                
                # 최근 분석일
                cursor.execute('SELECT MAX(created_at) FROM social_media_posts')
                last_post = cursor.fetchone()[0]
                
                return {
                    'total_posts': posts_count,
                    'total_analysis': analysis_count,
                    'unique_keywords': keywords_count,
                    'last_post_date': last_post,
                    'database_size': os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                }
                
        except Exception as e:
            print(f"❌ 통계 조회 실패: {e}")
            return {}

# 사용 예시 (테스트용)
if __name__ == "__main__":
    db = DatabaseManager()
    
    # 테스트 데이터
    test_data = [
        {
            'platform': 'YouTube',
            'content': '삼성 갤럭시 정말 좋네요!',
            'author': 'TestUser1',
            'sentiment': 'positive',
            'confidence': 0.9,
            'scores': {'positive': 0.9, 'negative': 0.05, 'neutral': 0.05}
        }
    ]
    
    # 저장 테스트
    success = db.save_analysis_results('삼성 갤럭시', test_data)
    print(f"저장 성공: {success}")
    
    # 조회 테스트
    stats = db.get_database_stats()
    print(f"데이터베이스 통계: {stats}") 