"""YouTube content collector for trending analysis and hot content detection."""


import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeCollector:    
    def __init__(self, api_key: str = None):
        """YouTube API 클라이언트 초기화
        Args:
            api_key: YouTube Data API v3 키. None이면 환경변수에서 읽음
        """
        self.api_key = api_key or os.getenv('YOUTUBE_API_KEY')
        
        if not self.api_key:
            self.youtube = None
            print("⚠️ YouTube API 키를 사용할 수 없습니다. 데이터 수집 실패")
        else:
            try:
                self.youtube = build('youtube', 'v3', developerKey=self.api_key)
            except Exception as e:
                print(f"YouTube API 초기화 실패: {e}")
                self.youtube = None
    
    def find_hot_content(
        self, keyword: str, max_videos: int = 50
    ) -> Dict[str, Any]:
        """키워드로 핫한 YouTube 영상과 댓글을 찾기
        
        Args:
            keyword: 검색할 키워드
            max_videos: 최대 분석할 영상 수
            
        Returns:
            핫한 영상들과 핫한 댓글들의 딕셔너리
        """
        if not self.youtube:
            print(f"⚠️ YouTube API를 사용할 수 없어 '{keyword}' 키워드의 최신 데이터를 수집하지 못했습니다.")
            print("📚 기존 데이터베이스의 데이터를 사용합니다.")
            return {
                'keyword': keyword,
                'hot_videos': [],
                'hot_comments': [],
                'total_videos_analyzed': 0,
                'total_comments_analyzed': 0,
                'status': 'api_unavailable',
                'message': 'YouTube API 사용 불가능으로 인해 최신 데이터 수집 실패'
            }
        
        try:
            # 1. 키워드로 비디오 검색 (최신순 + 인기순)
            hot_videos = []
            hot_comments = []
            
            # 최신 인기 영상 검색
            search_response = self.youtube.search().list(  # pylint: disable=no-member
                q=keyword,
                part='id,snippet',
                maxResults=max_videos,
                order='viewCount',
                type='video',
                publishedAfter=(datetime.utcnow() - timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%SZ')
            ).execute()
            
            video_ids = [item['id']['videoId'] for item in search_response['items']]
            
            # 2. 비디오 상세 정보 가져오기 (조회수, 좋아요 등)
            if video_ids:
                videos_response = self.youtube.videos().list(  # pylint: disable=no-member
                    part='statistics,snippet,contentDetails',
                    id=','.join(video_ids)
                ).execute()
                
                for video in videos_response['items']:
                    video_id = video['id']
                    snippet = video['snippet']
                    stats = video['statistics']
                    
                    # 핫한 영상 점수 계산을 위한 데이터
                    view_count = int(stats.get('viewCount', 0))
                    like_count = int(stats.get('likeCount', 0))
                    comment_count = int(stats.get('commentCount', 0))
                    
                    # 최근 댓글 활동 분석을 위해 댓글들을 미리 수집
                    recent_comments_data = self._analyze_recent_comments(video_id)
                    
                    # 핫한 점수 계산 (최근 댓글 활동 중심)
                    hot_score = self._calculate_video_hot_score(
                        view_count, like_count, comment_count, 
                        snippet['publishedAt'], recent_comments_data
                    )
                    if hot_score < 45:
                        continue
                    
                    video_data = {
                        'video_id': video_id,
                        'title': snippet['title'],
                        'channel': snippet['channelTitle'],
                        'published_at': snippet['publishedAt'],
                        'view_count': view_count,
                        'like_count': like_count,
                        'comment_count': comment_count,
                        'recent_comments_count': recent_comments_data.get('recent_comments_count', 0),
                        'very_recent_comments_count': recent_comments_data.get('very_recent_comments_count', 0),
                        'hot_score': hot_score,
                        'thumbnail': snippet['thumbnails'].get('medium', {}).get('url', ''),
                        'url': f"https://www.youtube.com/watch?v={video_id}",
                        'description': snippet.get('description', '')[:200] + '...'
                    }
                    
                    hot_videos.append(video_data)
                    
                    # 3. 각 영상의 핫한 댓글 수집 (이미 recent_comments_data에서 분석됨)
                    try:
                        # recent_comments_data에서 핫한 댓글들을 추출
                        video_hot_comments = recent_comments_data.get('hot_comments', [])
                        
                        for comment_data in video_hot_comments:
                            # 영상 정보 추가 (video_id 포함!)
                            comment_data.update({
                                'video_id': video_id,  # 누락된 video_id 추가
                                'video_title': snippet['title'],
                                'video_url': f"https://www.youtube.com/watch?v={video_id}"
                            })
                            hot_comments.append(comment_data)
                                
                    except HttpError as e:
                        if e.resp.status == 403:
                            continue  # 댓글 비활성화된 영상
                        else:
                            print(f"댓글 수집 오류 (비디오 {video_id}): {e}")
                            continue
            
            # 4. 핫한 순서로 정렬
            hot_videos.sort(key=lambda x: x['hot_score'], reverse=True)
            hot_comments.sort(key=lambda x: x['hot_score'], reverse=True)
            
            result = {
                'keyword': keyword,
                'hot_videos': hot_videos,
                'hot_comments': hot_comments,
                'total_videos_analyzed': len(hot_videos),
                'total_comments_analyzed': len(hot_comments)
            }
            
            print(f"🔥 핫한 영상 {len(result['hot_videos'])}개, 핫한 댓글 {len(result['hot_comments'])}개 발견!")
            return result
            
        except HttpError as e:
            print(f"⚠️ YouTube API 오류: {e}")
            print("📚 기존 데이터베이스의 데이터를 사용합니다.")
            return {
                'keyword': keyword,
                'hot_videos': [],
                'hot_comments': [],
                'total_videos_analyzed': 0,
                'total_comments_analyzed': 0,
                'status': 'api_error',
                'message': f'YouTube API 오류: {e}'
            }
        except Exception as e:
            print(f"⚠️ 예상치 못한 오류: {e}")
            print("📚 기존 데이터베이스의 데이터를 사용합니다.")
            return {
                'keyword': keyword,
                'hot_videos': [],
                'hot_comments': [],
                'total_videos_analyzed': 0,
                'total_comments_analyzed': 0,
                'status': 'unexpected_error',
                'message': f'예상치 못한 오류: {e}'
            }
    
    def _analyze_recent_comments(self, video_id: str) -> Dict[str, Any]:
        """최근 댓글 활동을 분석하여 영상의 현재 화제성 측정"""
        
        if not self.youtube:
            return {'recent_comments_count': 10, 'hot_comments': []}
        
        try:
            # 댓글 수집 (최신순으로)
            comments_response = self.youtube.commentThreads().list(  # pylint: disable=no-member
                part='snippet',
                videoId=video_id,
                maxResults=100,  # 더 많이 수집해서 최근 활동 분석
                order='relevance'
            ).execute()
            
            now = datetime.now(timezone.utc)
            recent_threshold = now - timedelta(days=7)  # 최근 7일
            very_recent_threshold = now - timedelta(days=1)  # 최근 1일
            
            recent_comments_count = 0
            very_recent_comments_count = 0
            hot_comments = []
            
            for comment_item in comments_response['items']:
                comment = comment_item['snippet']['topLevelComment']['snippet']
                
                # 댓글 시간 파싱
                try:
                    comment_time = datetime.fromisoformat(comment['publishedAt'].replace('Z', '+00:00'))
                    
                    # 최근성 체크
                    if comment_time > recent_threshold:
                        recent_comments_count += 1
                        
                        if comment_time > very_recent_threshold:
                            very_recent_comments_count += 1
                    
                    # 조회수, 답글수 
                    like_count = comment.get('likeCount', 0)
                    reply_count = comment_item['snippet'].get('totalReplyCount', 0)
                    
                    comment_hot_score = self._calculate_comment_hot_score(
                        like_count, reply_count, comment['publishedAt'], comment_time
                    )
                    
                    if comment_hot_score >= 45:  # 새로운 임계값
                        hot_comment_data = {
                            'comment_id': comment_item['id'],
                            'content': comment['textDisplay'],
                            'author': comment['authorDisplayName'],
                            'like_count': like_count,
                            'reply_count': reply_count,
                            'published_at': comment['publishedAt'],
                            'hot_score': comment_hot_score
                        }
                        hot_comments.append(hot_comment_data)
                        
                except:
                    continue
            
            return {
                'recent_comments_count': recent_comments_count,
                'very_recent_comments_count': very_recent_comments_count,
                'hot_comments': sorted(hot_comments, key=lambda x: x['hot_score'], reverse=True)[:5]
            }
            
        except Exception as e:
            print(f"최근 댓글 분석 오류 (비디오 {video_id}): {e}")
            return {'recent_comments_count': 0, 'hot_comments': []}
    
    def _calculate_video_hot_score(self, view_count: int, like_count: int, comment_count: int, 
                                  published_at: str, recent_comments_data: Dict) -> float:
        """영상의 핫한 정도 점수 계산 - 최근 댓글 활동 중심"""
        import math
        
        # 1. 최근 댓글 활동 점수 (가장 중요) - 60%
        recent_comments = recent_comments_data.get('recent_comments_count', 0)
        very_recent_comments = recent_comments_data.get('very_recent_comments_count', 0)
        
        # 최근 7일 댓글 점수
        # recent_comments가 2^15(=32,768)일 때 최고점(36점)이 되도록 스케일
        recent_activity_score = min(math.log2(max(recent_comments, 1)) / 15 * 36, 36)
        
        # 최근 1일 댓글 보너스 
        # very_recent_comments가 2^10(=1024)일 때 최고점(24점)이 되도록 스케일
        very_recent_bonus = min(math.log2(max(very_recent_comments, 1)) / 10 * 24, 24)
        
        recent_total_score = recent_activity_score + very_recent_bonus
        # recent_total_score = min(recent_total_score, 60)
        
        # 2. 조회수 - 25%
        # view_count가 2^23(=8,388,608)일 때 최고점(25점)이 되도록 스케일
        view_score = min(math.log2(max(view_count, 1)) / 23 * 25, 25)
        
        # 3. 좋아요 비율 점수 - 10%
        # like : view = 1 : 100 일때 최고점(10점)
        like_ratio = like_count / max(view_count, 1)
        if like_ratio >= 0.01:
            like_score = 10
        elif like_ratio < 0.001:
            like_score = 0
        else:
            like_score = like_ratio * 1000  # 0.00n => n점
        
        # 4. 영상 신선도 점수 - 5%
        pub_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        days_old = (now - pub_date).days

        if days_old <= 3:
            freshness_score = 5
        elif days_old <= 7:
            freshness_score = 2.5
        else:
            freshness_score = 0
        
        # 총 점수 계산
        total_score = recent_total_score + view_score + like_score + freshness_score
        
        return round(total_score, 2)
    
    def _calculate_comment_hot_score(self, like_count: int, reply_count: int, 
                                   published_at: str, comment_time: datetime = None) -> float:
        """댓글의 핫한 정도 점수 계산 - 답글 활동에 높은 가중치"""
        import math     # pylint: disable=import-outside-toplevel
        
        # 1. 좋아요 점수 - 30% (가중치 감소)
        like_score = min(math.log10(max(like_count, 1)) * 10, 30)
        
        # 2. 답글 활동 점수 - 50% (가중치 증가 + 계산 강화)
        # 답글이 많을수록 기하급수적으로 점수 증가
        if reply_count == 0:
            reply_score = 0
        elif reply_count <= 5:
            reply_score = reply_count * 6  # 답글 1-5개: 기본 점수
        elif reply_count <= 15:
            reply_score = 30 + (reply_count - 5) * 4  # 답글 6-15개: 가속 점수
        else:
            reply_score = 70 + (reply_count - 15) * 2  # 답글 16개 이상: 최고 점수
        
        reply_score = min(reply_score, 50)  # 최대 50점
        
        # 3. 최근성 점수 - 20%
        if comment_time is None:
            try:
                comment_time = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            except:
                comment_time = datetime.now(timezone.utc) - timedelta(days=30)
        
        now = datetime.now(timezone.utc)
        hours_old = (now - comment_time).total_seconds() / 3600
        
        # 최근성 보너스 (24시간 이내 최대 보너스)
        if hours_old <= 24:
            recency_score = 20  # 최대 점수
        elif hours_old <= 72:  # 3일 이내
            recency_score = 15
        elif hours_old <= 168:  # 1주일 이내
            recency_score = 10
        else:
            recency_score = max(0, 5 - (hours_old - 168) / 24)  # 1주일 후 감소
        
        # 특별 보너스: 최근 1시간 이내 댓글
        if hours_old <= 1:
            recency_score += 10
        
        total_score = like_score + reply_score + recency_score
        return round(total_score, 2)
    

    

    
    def get_video_stats(self, video_id: str) -> Dict[str, Any]:
        """비디오 통계 정보 수집
        
        Args:
            video_id: YouTube 비디오 ID
            
        Returns:
            비디오 통계 데이터
        """
        if not self.youtube:
            print("⚠️ YouTube API를 사용할 수 없어 비디오 통계를 가져올 수 없습니다.")
            return {
                'status': 'api_unavailable',
                'message': 'YouTube API 사용 불가능'
            }
        
        try:
            response = self.youtube.videos().list(  # pylint: disable=no-member
                part='statistics,contentDetails',
                id=video_id
            ).execute()
            
            if response['items']:
                stats = response['items'][0]['statistics']
                content = response['items'][0]['contentDetails']
                
                return {
                    'view_count': int(stats.get('viewCount', 0)),
                    'like_count': int(stats.get('likeCount', 0)),
                    'comment_count': int(stats.get('commentCount', 0)),
                    'duration': content.get('duration', 'PT0S')
                }
        except Exception as e:
            print(f"비디오 통계 수집 오류: {e}")
        
        return {}

# 사용 예시 (테스트용)
if __name__ == "__main__":
    collector = YouTubeCollector()
    result = collector.find_hot_content("삼성 갤럭시", 10)
    comments = result['hot_comments']
    
    for comment in comments[:3]:
        print(f"작성자: {comment['author']}")
        print(f"내용: {comment['content'][:50]}...")
        print(f"좋아요: {comment['like_count']}")
        print("---") 