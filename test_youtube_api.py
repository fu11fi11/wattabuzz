#!/usr/bin/env python3
"""
YouTube API 연결 테스트 스크립트
실제 API 키가 작동하는지 확인하고 기본적인 댓글 수집을 테스트합니다.
"""

import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def test_youtube_api():
    """YouTube API 연결 및 기본 기능 테스트"""
    
    # 환경변수 로드
    load_dotenv()
    api_key = os.getenv('YOUTUBE_API_KEY')
    
    if not api_key or api_key == 'YOUR_YOUTUBE_API_KEY_HERE':
        print("❌ YouTube API 키가 설정되지 않았습니다!")
        print("📝 .env 파일에서 YOUTUBE_API_KEY를 설정해주세요.")
        return False
    
    try:
        # YouTube API 클라이언트 생성
        youtube = build('youtube', 'v3', developerKey=api_key)
        print("✅ YouTube API 클라이언트 생성 성공!")
        
        # 1. 간단한 검색 테스트
        print("\n🔍 검색 테스트: '삼성 갤럭시'")
        search_response = youtube.search().list(
            q='삼성 갤럭시',
            part='id,snippet',
            maxResults=3,
            type='video'
        ).execute()
        
        print(f"📹 검색된 비디오 수: {len(search_response['items'])}")
        
        # 첫 번째 비디오 정보 출력
        if search_response['items']:
            video = search_response['items'][0]
            video_id = video['id']['videoId']
            video_title = video['snippet']['title']
            
            print(f"\n📺 첫 번째 비디오:")
            print(f"   제목: {video_title}")
            print(f"   ID: {video_id}")
            print(f"   URL: https://www.youtube.com/watch?v={video_id}")
            
            # 2. 댓글 수집 테스트
            print(f"\n💬 댓글 수집 테스트...")
            try:
                comments_response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    maxResults=5,
                    order='relevance'
                ).execute()
                
                comments = comments_response['items']
                print(f"✅ 댓글 {len(comments)}개 수집 성공!")
                
                # 댓글 샘플 출력
                for i, comment_item in enumerate(comments[:2], 1):
                    comment = comment_item['snippet']['topLevelComment']['snippet']
                    print(f"\n📝 댓글 {i}:")
                    print(f"   작성자: {comment['authorDisplayName']}")
                    print(f"   내용: {comment['textDisplay'][:100]}...")
                    print(f"   좋아요: {comment.get('likeCount', 0)}")
                
            except HttpError as e:
                if e.resp.status == 403:
                    print("⚠️ 이 비디오는 댓글이 비활성화되어 있습니다.")
                    print("✅ 하지만 API 연결은 성공했습니다!")
                else:
                    print(f"❌ 댓글 수집 오류: {e}")
                    return False
        
        # 3. 할당량 정보 확인
        print(f"\n📊 API 할당량 정보:")
        print(f"   일일 할당량: 10,000 단위")
        print(f"   검색 1회: ~100 단위")
        print(f"   댓글 수집 1회: ~1 단위")
        print(f"   예상 가능 검색: ~100회/일")
        
        print(f"\n🎉 YouTube API 연동 성공!")
        return True
        
    except HttpError as e:
        print(f"❌ YouTube API 오류: {e}")
        if e.resp.status == 403:
            print("💡 가능한 원인:")
            print("   - API 키가 잘못되었습니다")
            print("   - YouTube Data API v3가 활성화되지 않았습니다")
            print("   - 할당량을 초과했습니다")
        return False
        
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")
        return False

if __name__ == "__main__":
    print("🎬 YouTube API 연결 테스트 시작...")
    print("=" * 50)
    
    success = test_youtube_api()
    
    print("=" * 50)
    if success:
        print("✅ 테스트 완료! 이제 실제 댓글 수집이 가능합니다.")
        print("📝 다음 단계: streamlit run app.py 실행 후 실제 키워드로 분석해보세요!")
    else:
        print("❌ 테스트 실패. API 키 설정을 확인해주세요.")
