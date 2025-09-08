import openai
from sentence_transformers import SentenceTransformer
from typing import Dict, List, Any
import logging
import json
from config.settings import settings

logger = logging.getLogger(__name__)

class LLMService:
    """
    Service for LLM-based conversation understanding and content generation
    """
    
    def __init__(self):
        if settings.OPENAI_API_KEY:
            openai.api_key = settings.OPENAI_API_KEY
        
        # Load embedding model
        try:
            self.embedding_model = SentenceTransformer(settings.EMBEDDING_MODEL)
        except Exception as e:
            logger.warning(f"Failed to load embedding model: {str(e)}")
            self.embedding_model = None
    
    async def analyze_conversation(self, conversation_text: str) -> Dict[str, Any]:
        """
        Analyze conversation to extract summary, topics, sentiment, and needs
        """
        try:
            # Create analysis prompt
            prompt = f"""
            Analyze this educational conversation and provide:
            1. A 3-5 sentence summary
            2. List of educational topics discussed (format: Subject>Topic)
            3. Sentiment score from -1 (frustrated) to +1 (engaged)
            4. List of learning needs identified
            
            Conversation:
            {conversation_text}
            
            Respond in JSON format:
            {{
                "summary": "...",
                "topics": ["Biology>Cells", "Algebra>Variables"],
                "sentiment": 0.5,
                "needs": ["practice set", "visual diagram"]
            }}
            """
            
            if settings.OPENAI_API_KEY:
                response = await openai.ChatCompletion.acreate(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=500
                )
                
                analysis = json.loads(response.choices[0].message.content)
            else:
                # Fallback analysis without OpenAI
                analysis = self._fallback_analysis(conversation_text)
            
            # Generate embedding
            embedding = self._generate_embedding(analysis['summary'])
            analysis['embedding'] = embedding
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze conversation: {str(e)}")
            return self._fallback_analysis(conversation_text)
    
    async def generate_email_content(self, template_data: Dict[str, Any]) -> str:
        """
        Generate personalized email content using LLM
        """
        try:
            prompt = f"""
            Generate a 2-sentence personalized learning encouragement for:
            - Student: {template_data.get('first_name', 'Student')}
            - Topic: {template_data.get('topic', 'learning')}
            - Context: {template_data.get('observed_struggle', 'general study')}
            
            Tone: encouraging, concise, educational
            """
            
            if settings.OPENAI_API_KEY:
                response = await openai.ChatCompletion.acreate(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=100
                )
                
                return response.choices[0].message.content.strip()
            else:
                return self._fallback_email_content(template_data)
                
        except Exception as e:
            logger.error(f"Failed to generate email content: {str(e)}")
            return self._fallback_email_content(template_data)
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text"""
        if not self.embedding_model or not text:
            return [0.0] * 1536  # Default OpenAI embedding size
        
        try:
            embedding = self.embedding_model.encode(text)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Failed to generate embedding: {str(e)}")
            return [0.0] * 1536
    
    def _fallback_analysis(self, conversation_text: str) -> Dict[str, Any]:
        """Fallback analysis when LLM is not available"""
        # Simple keyword-based analysis
        topics = []
        if 'cell' in conversation_text.lower():
            topics.append('Biology>Cells')
        if 'algebra' in conversation_text.lower():
            topics.append('Algebra>Variables')
        if 'marketing' in conversation_text.lower():
            topics.append('Marketing>Basics')
        
        # Simple sentiment based on keywords
        positive_words = ['good', 'great', 'understand', 'clear', 'helpful']
        negative_words = ['confused', 'difficult', 'hard', 'stuck', 'frustrated']
        
        text_lower = conversation_text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        sentiment = (positive_count - negative_count) / max(positive_count + negative_count, 1)
        
        return {
            'summary': 'Educational conversation analyzed with fallback method.',
            'topics': topics,
            'sentiment': max(-1, min(1, sentiment)),
            'needs': ['practice set'] if negative_count > 0 else []
        }
    
    def _fallback_email_content(self, template_data: Dict[str, Any]) -> str:
        """Fallback email content when LLM is not available"""
        first_name = template_data.get('first_name', 'Student')
        topic = template_data.get('topic', 'your studies')
        
        return f"Keep up the great work on {topic}, {first_name}! Practice makes perfect, and you're making excellent progress."
