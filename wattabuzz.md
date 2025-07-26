# 소셜미디어 감성분석 파이프라인 - "WattaBuzz" 프로젝트

## 🎯 프로젝트 컨셉
특정 주제(브랜드, 제품, 이슈, 인물 등)에 대한 다양한 소셜미디어 플랫폼의 실시간 반응을 수집하고 감성을 분석하여 종합적인 여론 트렌드를 시각화하는 플랫폼

## 🏗️ 시스템 아키텍처

```
[Data Sources] → [Collection Layer] → [Processing Layer] → [Analysis Layer] → [Presentation Layer]
      ↓               ↓                    ↓                 ↓                 ↓
   YouTube API     Apache Kafka        Apache Spark      ML Models         React WebApp
   Twitter API     Apache NiFi         Apache Flink      BERT/RoBERTa      Grafana Dashboard
   Reddit API      Scrapy Cluster      Kafka Streams     spaCy NLP         Mobile App
   Instagram API   Apache Airflow      Apache Storm      scikit-learn      Slack Bot
   TikTok API      
```

## 📱 핵심 기능 명세

### 1. 데이터 수집 기능
**지원 플랫폼:**
- **YouTube**: 댓글, 좋아요/싫어요, 조회수 트렌드
- **X (Twitter)**: 트윗, 리트윗, 멘션, 해시태그
- **Reddit**: 게시글, 댓글, upvote/downvote
- **Instagram**: 해시태그 게시글, 댓글
- **TikTok**: 해시태그, 댓글 (제한적)
- **네이버 카페/블로그**: 키워드 기반 검색
- **디시인사이드**: 갤러리별 게시글/댓글

### 2. 실시간 모니터링 기능
```python
# 키워드 모니터링 예시
keywords = [
    "삼성 갤럭시 S24",
    "아이폰 15",
    "테슬라 모델 Y",
    "BTS 신곡",
    "대통령 정책"
]

# 실시간 수집 설정
monitoring_config = {
    "keyword": "삼성 갤럭시 S24",
    "platforms": ["youtube", "twitter", "reddit"],
    "collection_interval": "5분",
    "sentiment_analysis": True,
    "alert_threshold": {
        "negative_spike": 70,  # 부정 감성 70% 이상시 알림
        "volume_increase": 300  # 언급량 300% 증가시 알림
    }
}
```

### 3. 감성분석 엔진
**다층 분석 구조:**
- **1단계**: 기본 감성 (긍정/부정/중립)
- **2단계**: 세부 감정 (기쁨, 분노, 슬픔, 놀람, 두려움, 혐오)
- **3단계**: 주제별 감성 (제품 품질, 가격, 디자인, 성능 등)

## 🛠️ 기술 스택 상세

### Backend 아키텍처
```yaml
# docker-compose.yml 예시
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
  
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - ES_JAVA_OPTS=-Xms1g -Xmx1g
  
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: wattabuzz
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: password
  
  redis:
    image: redis:7-alpine
    
  spark:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=master
```

### 데이터 수집 모듈
```python
# collectors/youtube_collector.py
from googleapiclient.discovery import build
import asyncio
from kafka import KafkaProducer

class YouTubeCollector:
    def __init__(self, api_key, kafka_producer):
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.producer = kafka_producer
    
    async def collect_comments(self, keyword, max_results=1000):
        # 키워드로 비디오 검색
        search_response = self.youtube.search().list(
            q=keyword,
            part='id,snippet',
            maxResults=50,
            order='relevance',
            publishedAfter='2024-01-01T00:00:00Z'
        ).execute()
        
        for video in search_response['items']:
            video_id = video['id']['videoId']
            
            # 댓글 수집
            comments = self.youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100,
                order='relevance'
            ).execute()
            
            for comment in comments['items']:
                comment_data = {
                    'platform': 'youtube',
                    'keyword': keyword,
                    'content': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'author': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'timestamp': comment['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'likes': comment['snippet']['topLevelComment']['snippet']['likeCount'],
                    'video_id': video_id,
                    'video_title': video['snippet']['title']
                }
                
                # Kafka로 전송
                self.producer.send('social-media-raw', comment_data)
```

### 감성분석 모듈
```python
# sentiment/analyzer.py
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from torch.nn.functional import softmax
import torch

class MultiLevelSentimentAnalyzer:
    def __init__(self):
        # 한국어 감성분석 모델 (KoBERT 기반)
        self.tokenizer = AutoTokenizer.from_pretrained("monologg/kobert")
        self.model = AutoModelForSequenceClassification.from_pretrained("monologg/kobert")
        
        # 세부 감정 분류 모델
        self.emotion_model = AutoModelForSequenceClassification.from_pretrained(
            "j-hartmann/emotion-english-distilroberta-base"
        )
    
    def analyze_basic_sentiment(self, text):
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = softmax(outputs.logits, dim=-1)
            
        sentiment_labels = ['negative', 'neutral', 'positive']
        confidence_scores = predictions[0].tolist()
        
        return {
            'sentiment': sentiment_labels[torch.argmax(predictions)],
            'confidence': max(confidence_scores),
            'scores': dict(zip(sentiment_labels, confidence_scores))
        }
    
    def analyze_detailed_emotion(self, text):
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
        
        with torch.no_grad():
            outputs = self.emotion_model(**inputs)
            predictions = softmax(outputs.logits, dim=-1)
        
        emotion_labels = ['joy', 'sadness', 'anger', 'fear', 'surprise', 'disgust']
        scores = predictions[0].tolist()
        
        return {
            'primary_emotion': emotion_labels[torch.argmax(predictions)],
            'emotion_scores': dict(zip(emotion_labels, scores))
        }
```

## 📊 데이터베이스 스키마

```sql
-- 원본 데이터 테이블
CREATE TABLE social_media_posts (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    keyword VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    author VARCHAR(100),
    timestamp TIMESTAMP WITH TIME ZONE,
    engagement_metrics JSONB,  -- 좋아요, 댓글 수 등
    metadata JSONB,  -- 플랫폼별 추가 정보
    created_at TIMESTAMP DEFAULT NOW()
);

-- 감성분석 결과 테이블
CREATE TABLE sentiment_analysis (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES social_media_posts(id),
    basic_sentiment VARCHAR(20),  -- positive, negative, neutral
    sentiment_confidence FLOAT,
    detailed_emotions JSONB,  -- 세부 감정 분석 결과
    topic_sentiments JSONB,  -- 주제별 감성 (가격, 품질 등)
    analyzed_at TIMESTAMP DEFAULT NOW()
);

-- 트렌드 집계 테이블 (시계열)
CREATE TABLE sentiment_trends (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(200),
    platform VARCHAR(50),
    time_bucket TIMESTAMP,  -- 1시간 단위 집계
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,
    total_mentions INTEGER,
    average_engagement FLOAT,
    top_emotions JSONB
);

-- 인덱스 생성
CREATE INDEX idx_posts_keyword_timestamp ON social_media_posts(keyword, timestamp);
CREATE INDEX idx_trends_keyword_time ON sentiment_trends(keyword, time_bucket);
```

## 🎨 프론트엔드 기능 명세

### 1. 메인 대시보드
```javascript
// components/Dashboard.jsx
import React, { useState, useEffect } from 'react';
import { Line, Pie, Bar } from 'react-chartjs-2';

const Dashboard = () => {
    const [selectedKeyword, setSelectedKeyword] = useState('삼성 갤럭시 S24');
    const [timeRange, setTimeRange] = useState('24h');
    const [sentimentData, setSentimentData] = useState(null);

    // 실시간 데이터 업데이트
    useEffect(() => {
        const ws = new WebSocket('ws://localhost:8000/ws/sentiment');
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.keyword === selectedKeyword) {
                setSentimentData(data);
            }
        };
        return () => ws.close();
    }, [selectedKeyword]);

    return (
        <div className="dashboard">
            <div className="controls">
                <KeywordSelector 
                    value={selectedKeyword}
                    onChange={setSelectedKeyword}
                />
                <TimeRangeSelector 
                    value={timeRange}
                    onChange={setTimeRange}
                />
            </div>
            
            <div className="charts-grid">
                <SentimentTimeline data={sentimentData?.timeline} />
                <PlatformComparison data={sentimentData?.platforms} />
                <EmotionBreakdown data={sentimentData?.emotions} />
                <KeywordCloud data={sentimentData?.keywords} />
            </div>
            
            <RecentPosts 
                posts={sentimentData?.recent_posts}
                keyword={selectedKeyword}
            />
        </div>
    );
};
```

### 2. 주요 시각화 컴포넌트

**감성 타임라인 차트:**
- 시간대별 긍정/부정/중립 감성 변화
- 플랫폼별 색상 구분
- 주요 이벤트 마커 표시

**플랫폼 비교 차트:**
- 플랫폼별 감성 분포 비교
- 언급량 vs 감성 점수 산점도
- 플랫폼별 참여도 지표

**감정 히트맵:**
- 시간대별 세부 감정 강도
- 키워드별 감정 패턴 비교

### 3. 알림 시스템
```python
# alerts/notification_manager.py
class NotificationManager:
    def __init__(self):
        self.alert_rules = []
        self.notification_channels = ['email', 'slack', 'discord', 'webhook']
    
    def check_sentiment_spike(self, keyword, current_data, threshold=0.7):
        """부정 감성 급증 감지"""
        negative_ratio = current_data['negative'] / current_data['total']
        
        if negative_ratio > threshold:
            self.send_alert({
                'type': 'negative_spike',
                'keyword': keyword,
                'severity': 'high',
                'message': f'{keyword}에 대한 부정 반응이 {negative_ratio:.1%}로 급증했습니다.',
                'timestamp': datetime.now(),
                'data': current_data
            })
    
    def check_viral_content(self, posts, engagement_threshold=1000):
        """바이럴 컨텐츠 감지"""
        for post in posts:
            if post['engagement'] > engagement_threshold:
                self.send_alert({
                    'type': 'viral_content',
                    'platform': post['platform'],
                    'content': post['content'][:100] + '...',
                    'engagement': post['engagement'],
                    'sentiment': post['sentiment']
                })
```

## 📈 구현 로드맵

### Phase 1: MVP 개발 (4-5주)
**Week 1-2: 기반 인프라**
- Docker 환경 구성
- Kafka 파이프라인 구축
- 기본 데이터 수집기 (YouTube, Twitter) 개발
- 간단한 감성분석 모델 통합

**Week 3-4: 핵심 기능**
- 웹 대시보드 개발 (React)
- 실시간 데이터 스트리밍
- 기본 시각화 차트 구현
- REST API 개발

**Week 5: 테스트 및 배포**
- 시스템 테스트
- 성능 최적화
- 문서화

### Phase 2: 고도화 (3-4주)
- 추가 플랫폼 연동 (Reddit, Instagram)
- 고급 감성분석 (다중 언어, 세부 감정)
- 머신러닝 모델 개선
- 알림 시스템 구축

### Phase 3: 확장 기능 (2-3주)
- 모바일 앱 개발
- 경쟁사 비교 분석
- 예측 모델링
- API 상용화

## 💡 활용 사례 시나리오

### 1. 브랜드 모니터링
**시나리오**: 삼성이 새로운 갤럭시 출시 후 반응 모니터링
- 실시간 언급량 추적
- 경쟁사(애플) 대비 감성 비교
- 주요 불만사항 키워드 추출
- 마케팅 캠페인 효과 측정

### 2. 위기 관리
**시나리오**: 제품 결함 이슈 발생시 여론 관리
- 부정 감성 급증 즉시 알림
- 이슈 확산 플랫폼 파악
- 대응 메시지 효과 실시간 측정

### 3. 투자 분석
**시나리오**: 상장사 주가 연관 소셜 감성 분석
- 기업 관련 소셜 감성과 주가 상관관계 분석
- 실적 발표 전후 감성 변화 추적

이 프로젝트는 실시간 데이터 처리, NLP, 웹 개발, 데이터 시각화 등 다양한 기술 스택을 포괄하는 종합적인 데이터 사이언스 프로젝트가 될 것입니다.

어떤 부분부터 구체적으로 진행해보고 싶으신가요?