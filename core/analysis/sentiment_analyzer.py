import os
import re
from typing import Dict, Any, List
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np

class SentimentAnalyzer:
    """감성분석 엔진
    
    나중에 FastAPI로 마이그레이션할 때도 그대로 사용 가능한 독립적인 모듈
    """
    
    def __init__(self, model_name: str = None):
        """감성분석 모델 초기화
        
        Args:
            model_name: 사용할 모델명. None이면 기본 한국어 모델 사용
        """
        self.model_name = model_name or "matthewburke/korean-sentiment-analysis-dataset"
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        try:
            print(f"🧠 감성분석 모델 로딩 중... ({self.model_name})")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            self.model.to(self.device)
            self.model.eval()
            print("✅ 감성분석 모델 로딩 완료")
            self.model_loaded = True
            
        except Exception as e:
            print(f"⚠️ 모델 로딩 실패: {e}")
            print("📝 규칙 기반 감성분석을 사용합니다.")
            self.model_loaded = False
    
    def analyze(self, text: str) -> Dict[str, Any]:
        """텍스트 감성분석
        
        Args:
            text: 분석할 텍스트
            
        Returns:
            감성분석 결과 딕셔너리
        """
        if not text or not text.strip():
            return {
                'sentiment': 'neutral',
                'confidence': 0.5,
                'scores': {'positive': 0.33, 'negative': 0.33, 'neutral': 0.34}
            }
        
        # 텍스트 전처리
        cleaned_text = self._preprocess_text(text)
        
        if self.model_loaded:
            return self._analyze_with_model(cleaned_text)
        else:
            return self._analyze_with_rules(cleaned_text)
    
    def _analyze_with_model(self, text: str) -> Dict[str, Any]:
        """딥러닝 모델을 사용한 감성분석
        
        Args:
            text: 전처리된 텍스트
            
        Returns:
            감성분석 결과
        """
        try:
            # 토크나이징
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True
            )
            inputs = {key: value.to(self.device) for key, value in inputs.items()}
            
            # 예측
            with torch.no_grad():
                outputs = self.model(**inputs)
                probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
                probabilities = probabilities.cpu().numpy()[0]
            
            # 레이블 매핑 (모델에 따라 다를 수 있음)
            labels = ['negative', 'positive']  # 대부분의 이진 분류 모델
            if len(probabilities) == 3:
                labels = ['negative', 'neutral', 'positive']
            
            # 결과 정리
            scores = {label: float(prob) for label, prob in zip(labels, probabilities)}
            predicted_sentiment = labels[np.argmax(probabilities)]
            confidence = float(np.max(probabilities))
            
            # neutral이 없는 모델의 경우 임계값으로 neutral 판단
            if len(labels) == 2 and confidence < 0.8:
                predicted_sentiment = 'neutral'
                scores['neutral'] = 1.0 - confidence
                confidence = scores['neutral']
            
            return {
                'sentiment': predicted_sentiment,
                'confidence': confidence,
                'scores': scores
            }
            
        except Exception as e:
            print(f"모델 분석 중 오류: {e}")
            return self._analyze_with_rules(text)
    
    def _analyze_with_rules(self, text: str) -> Dict[str, Any]:
        """규칙 기반 감성분석 (모델이 없을 때 대안)
        
        Args:
            text: 전처리된 텍스트
            
        Returns:
            감성분석 결과
        """
        # 긍정 키워드
        positive_keywords = [
            '좋', '훌륭', '최고', '완벽', '대박', '굿', '좋아', '만족', '추천',
            '감사', '고마', '놀라', '신나', '기쁘', '행복', '사랑', '좋네',
            '멋지', '환상', '완전', '정말', '진짜', '예쁘', '아름다', '달콤',
            '맛있', '시원', '편리', '유용', '도움'
        ]
        
        # 부정 키워드  
        negative_keywords = [
            '별로', '나쁘', '싫', '실망', '최악', '끔찍', '짜증', '화나', '미워',
            '슬프', '우울', '걱정', '불안', '무서', '아쉽', '후회', '비싸',
            '어려', '복잡', '불편', '느리', '더러', '못생', '맛없', '아프',
            '지겨', '힘들', '피곤', '귀찮'
        ]
        
        # 점수 계산
        positive_score = sum(1 for keyword in positive_keywords if keyword in text)
        negative_score = sum(1 for keyword in negative_keywords if keyword in text)
        
        # 감성 판단
        total_score = positive_score + negative_score
        
        if total_score == 0:
            sentiment = 'neutral'
            confidence = 0.5
            scores = {'positive': 0.33, 'negative': 0.33, 'neutral': 0.34}
        else:
            pos_ratio = positive_score / total_score
            neg_ratio = negative_score / total_score
            
            if pos_ratio > neg_ratio:
                sentiment = 'positive'
                confidence = min(0.9, 0.5 + pos_ratio * 0.4)
                scores = {
                    'positive': confidence,
                    'negative': (1 - confidence) * 0.3,
                    'neutral': (1 - confidence) * 0.7
                }
            elif neg_ratio > pos_ratio:
                sentiment = 'negative'  
                confidence = min(0.9, 0.5 + neg_ratio * 0.4)
                scores = {
                    'negative': confidence,
                    'positive': (1 - confidence) * 0.3,
                    'neutral': (1 - confidence) * 0.7
                }
            else:
                sentiment = 'neutral'
                confidence = 0.6
                scores = {'positive': 0.3, 'negative': 0.3, 'neutral': 0.6}
        
        return {
            'sentiment': sentiment,
            'confidence': confidence,
            'scores': scores
        }
    
    def _preprocess_text(self, text: str) -> str:
        """텍스트 전처리
        
        Args:
            text: 원본 텍스트
            
        Returns:
            전처리된 텍스트
        """
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', '', text)
        
        # URL 제거
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # 특수문자 정리 (한글, 영문, 숫자, 공백만 유지)
        text = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', text)
        
        # 연속된 공백 제거
        text = re.sub(r'\s+', ' ', text)
        
        # 앞뒤 공백 제거
        text = text.strip()
        
        return text
    
    def analyze_batch(self, texts: List[str]) -> List[Dict[str, Any]]:
        """여러 텍스트 배치 분석
        
        Args:
            texts: 분석할 텍스트 리스트
            
        Returns:
            감성분석 결과 리스트
        """
        results = []
        for text in texts:
            result = self.analyze(text)
            results.append(result)
        
        return results
    
    def get_emotion_details(self, text: str) -> Dict[str, Any]:
        """세부 감정 분석 (확장 기능)
        
        Args:
            text: 분석할 텍스트
            
        Returns:
            세부 감정 분석 결과
        """
        # 기본 감성분석
        basic_sentiment = self.analyze(text)
        
        # 감정별 키워드 (간단한 버전)
        emotions = {
            'joy': ['기쁘', '행복', '신나', '좋아', '즐거', '웃', 'ㅎㅎ', 'ㅋㅋ'],
            'anger': ['화나', '짜증', '분노', '열받', '빡', '미치'],
            'sadness': ['슬프', '우울', '눈물', 'ㅠㅠ', '속상', '서러'],
            'fear': ['무서', '두려', '걱정', '불안', '떨려'],
            'surprise': ['놀라', '헉', '와', '대박', '어머'],
            'disgust': ['역겹', '더러', '싫어', '혐오', '징그']
        }
        
        emotion_scores = {}
        for emotion, keywords in emotions.items():
            score = sum(1 for keyword in keywords if keyword in text) / len(keywords)
            emotion_scores[emotion] = min(1.0, score)
        
        # 주된 감정 찾기
        main_emotion = max(emotion_scores.items(), key=lambda x: x[1])
        
        return {
            'basic_sentiment': basic_sentiment,
            'emotions': emotion_scores,
            'main_emotion': main_emotion[0],
            'emotion_confidence': main_emotion[1]
        }

# 사용 예시 (테스트용)
if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    
    test_texts = [
        "삼성 갤럭시 정말 좋네요! 추천합니다.",
        "아이폰이 별로에요... 실망입니다.",
        "이 제품 어떤가요? 궁금합니다."
    ]
    
    for text in test_texts:
        result = analyzer.analyze(text)
        print(f"텍스트: {text}")
        print(f"감성: {result['sentiment']} (신뢰도: {result['confidence']:.2f})")
        print("---") 