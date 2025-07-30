import time
from datetime import datetime, timedelta # pylint: disable=unused-import

import numpy as np
import streamlit as st

from config.keywords import COLLECTION_SETTINGS, TARGET_KEYWORDS # pylint: disable=unused-import
from core.database.database_manager import DatabaseManager

# Page config
st.set_page_config(
    page_title="WattaBuzz",
    page_icon="💻",
    layout="wide",
    initial_sidebar_state="expanded"
)

def show_welcome():
    """웰컴 메시지"""
    st.markdown(f"""
    ## 🎯 WattaBuzz에 오신 것을 환영합니다!
    
    **Apache Airflow로 자동 관리되는 YouTube 핫 콘텐츠 모니터링 시스템**
    
    ### 🔥 현재 모니터링 중인 키워드 ({', '.join(TARGET_KEYWORDS)})
    """)
    
    cols = st.columns(len(TARGET_KEYWORDS))
    for i, keyword in enumerate(TARGET_KEYWORDS):
        with cols[i]:
            st.metric(f"{keyword}", "모니터링 중", "Airflow 관리")
    
    st.markdown(f"""
    ### 주요 기능:
    - **Airflow 자동 관리**: Apache Airflow에서 {COLLECTION_SETTINGS.get('collection_interval_hours')}시간마다 자동 수집
    - **핫한 콘텐츠 탐지**: 최근 댓글 활동과 인기도 기반 점수 계산
    - **실시간 대시보드**: 수집된 데이터를 즉시 시각화
    - **직접 링크**: 핫한 콘텐츠로 바로 이동 가능
    - **수집 이력 추적**: Airflow 웹 UI에서 실행 상태 모니터링
    
    ---
    
    아래에서 최신 핫한 콘텐츠를 확인해보세요! 🚀
    """)

def show_airflow_status():
    """Airflow 상태 표시"""
    with st.sidebar:
        st.markdown("### 🚀 Airflow 파이프라인 상태")
        
        # Airflow 관리 중 안내
        st.info(f"""
        **현재 Apache Airflow에서 자동 관리 중입니다.**
        
        🔄 **수집 주기**: {COLLECTION_SETTINGS.get('collection_interval_hours')}시간마다
        🎯 **키워드**: {', '.join(TARGET_KEYWORDS)}
        📊 **모니터링**: Airflow 웹 UI에서 확인
        """)
        
        # 데이터베이스 통계 표시
        db_manager = DatabaseManager()
        
        st.markdown("#### 📊 수집 통계")
        total_videos = db_manager.get_total_stored_videos()
        total_comments = db_manager.get_total_stored_comments()
        last_collection = db_manager.get_last_collection_time()
        
        st.metric("총 영상", f"{total_videos}개")
        st.metric("총 댓글", f"{total_comments}개")
        st.metric("마지막 수집", last_collection)
        
        # 키워드별 통계
        st.markdown("#### 🏷️ 키워드별 통계")
        keywords_stats = db_manager.get_keywords_stats()
        
        if keywords_stats:
            for stat in keywords_stats:
                with st.expander(f"📈 {stat['keyword']}"):
                    st.metric("영상 수", f"{stat['video_count']}개")
                    st.metric("평균 핫점수", f"{stat['avg_hot_score']:.1f}")
                    st.metric("마지막 업데이트", stat['last_update'])
        else:
            st.warning("아직 Airflow에서 수집된 데이터가 없습니다.")
        
        # Airflow 링크
        st.markdown("---")
        st.markdown("""
        ### 🔗 Airflow 관리
        
        - **웹 UI**: `http://localhost:8080`
        - **DAG ID**: `hotspotter_collection`
        
        Airflow 웹 UI에서 실행 상태, 로그, 스케줄 등을 확인할 수 있습니다.
        """)

def show_stored_hot_content():
    """저장된 핫한 콘텐츠 표시"""
    db_manager = DatabaseManager()
    
    # 키워드 선택 탭
    keyword_tabs = st.tabs(["🔥 전체"] + [f"📊 {kw}" for kw in TARGET_KEYWORDS])
    
    # 전체 탭
    with keyword_tabs[0]:
        hot_content = db_manager.get_stored_hot_content()
        display_hot_content(hot_content, "전체")
    
    # 키워드별 탭
    for i, keyword in enumerate(TARGET_KEYWORDS):
        with keyword_tabs[i+1]:
            hot_content = db_manager.get_stored_hot_content(keyword)
            display_hot_content(hot_content, keyword)

def display_hot_content(hot_content: dict, keyword: str):
    """핫한 콘텐츠 표시"""
    if not hot_content['hot_videos'] and not hot_content['hot_comments']:
        st.info(f"""
        **{keyword}**에 대한 데이터가 아직 없습니다.
        
        Airflow DAG가 실행되면 자동으로 데이터가 수집됩니다.
        - Airflow 웹 UI: `http://localhost:8080`
        - DAG ID: `hotspotter_collection`
        """)
        return
    
    # 통계 표시
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("🎬 핫한 영상", f"{len(hot_content['hot_videos'])}개")
    with col2:
        st.metric("💬 핫한 댓글", f"{len(hot_content['hot_comments'])}개")
    with col3:
        avg_video_score = np.mean([v['hot_score'] for v in hot_content['hot_videos']]) if hot_content['hot_videos'] else 0
        st.metric("평균 영상 핫점수", f"{avg_video_score:.1f}")
    with col4:
        avg_comment_score = np.mean([c['hot_score'] for c in hot_content['hot_comments']]) if hot_content['hot_comments'] else 0
        st.metric("평균 댓글 핫점수", f"{avg_comment_score:.1f}")
    
    # 핫한 영상 섹션
    if hot_content['hot_videos']:
        st.markdown("### 🎬 핫한 영상")
        
        for i, video in enumerate(hot_content['hot_videos'][:10]):  # 상위 10개만 표시
            with st.expander(f"🔥 #{i+1} | {video['title'][:60]}... (핫점수: {video['hot_score']:.1f})", expanded=(i < 3)):
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    if video['thumbnail']:
                        st.image(video['thumbnail'], use_container_width=True)
                
                with col2:
                    st.markdown(f"**📺 채널**: {video['channel']}")
                    st.markdown(f"**👀 조회수**: {video['view_count']:,}회")
                    st.markdown(f"**👍 좋아요**: {video['like_count']:,}개")
                    st.markdown(f"**💬 댓글**: {video['comment_count']:,}개")
                    st.markdown(f"**🔥 핫점수**: {video['hot_score']:.1f}/100")
                    st.markdown(f"**📅 업로드**: {video['published_at']}")
                    
                    # 링크 버튼
                    st.link_button("🎬 영상 보러가기", video['url'])
                
                # 설명
                if video.get('description'):
                    st.markdown(f"**📄 설명**: {video['description'][:200]}...")
    
    # 핫한 댓글 섹션
    if hot_content['hot_comments']:
        st.markdown("### 💬 핫한 댓글")
        
        for i, comment in enumerate(hot_content['hot_comments'][:20]):  # 상위 20개만 표시
            with st.expander(f"💬 #{i+1} | {comment['author']} (핫점수: {comment['hot_score']:.1f})", expanded=(i < 5)):
                st.markdown(f"**👤 작성자**: {comment['author']}")
                st.markdown(f"**🎬 영상**: {comment['video_title']}")
                st.markdown(f"**💬 내용**: {comment['content']}")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("👍 좋아요", f"{comment['like_count']}개")
                with col2:
                    st.metric("💭 답글", f"{comment['reply_count']}개")
                with col3:
                    st.metric("🔥 핫점수", f"{comment['hot_score']:.1f}")
                with col4:
                    st.markdown(f"**📅**: {comment['published_at']}")
                
                # 링크 버튼
                st.link_button("🎬 원본 영상 보기", comment['video_url'])

def show_hot_score_explanation():
    """핫점수 계산 방법 설명"""
    with st.expander("🔥 핫한 점수 계산 방식"):
        st.markdown("""
        ### 🎬 영상 핫점수 계산:
        - **최근 댓글 활동** (60%): 최근 7일 내 댓글 수 (핵심 지표!), 최근 1일 내 댓글 수 (보너스)
        - **전체 조회수** (25%): 로그 스케일로 점수 계산
        - **좋아요 비율** (10%): 조회수 대비 좋아요 비율(100:1 -> 10점, 1000:1 -> 0점)
        - **영상 신선도** (5%): 최근 업로드일수록 높은 점수
        
        ### 💬 댓글 핫점수 계산:
        - **좋아요 수** (40%): 댓글 좋아요 수 (로그 스케일)
        - **답글 활동** (40%): 답글 수가 많을수록 높은 점수
        - **최근성** (20%): 1시간 이내: 특별 보너스 (+10점), 24시간 이내: 최대 점수 (20점), 3일 이내: 높은 점수 (15점), 1주일 이내: 보통 점수 (10점)
        
        ### 🎯 핵심 포인트:
        **현재 진행형으로 활발한 반응**을 보이는 콘텐츠가 가장 핫한 콘텐츠!
        """)

def main():
    st.title("🔥 HotSpotter")
    st.markdown("### YouTube 핫한 영상 & 댓글 자동 탐지기 (Airflow 관리)")
    
    # 사이드바에 Airflow 상태 표시
    show_airflow_status()
    
    # 메인 콘텐츠
    show_welcome()
    
    st.markdown("---")
    
    # 핫점수 계산 방법 설명
    show_hot_score_explanation()
    
    st.markdown("---")
    
    # 저장된 핫한 콘텐츠 표시
    show_stored_hot_content()
    
    # 자동 새로고침 (옵션)
    with st.sidebar:
        st.markdown("---")
        auto_refresh = st.checkbox("🔄 자동 새로고침 (5분)", value=False)
        
        if auto_refresh:
            time.sleep(300)
            st.rerun()

if __name__ == "__main__":
    main() 