import openai
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
        self.client = None
        if settings.OPENAI_API_KEY:
            self.client = openai.Client(api_key=settings.OPENAI_API_KEY)
        
        # Load embedding model
        try:
            self.embedding_model = None
        except Exception as e:
            logger.warning(f"Failed to load embedding model: {str(e)}")
            self.embedding_model = None
    
    async def analyze_conversation(self, conversation_text: str) -> Dict[str, Any]:
        """
        Analyze conversation to extract summary, topics, sentiment, needs, and follow-up triggers
        """
        try:
            # Enhanced analysis prompt for educational context
            prompt = f"""
            Analyze this educational conversation between a student and AI tutor. Extract:
            
            1. Educational topics discussed (format: Subject>Topic, e.g., "Biology>Cells", "USMLE>Cardiology")
            2. Sentiment score from -1 (frustrated/struggling) to +1 (confident/engaged)
            3. Learning needs and struggles identified
            4. Upcoming events mentioned (exams, tests, appointments, deadlines)
            5. Follow-up opportunities for personalized emails
            
            Conversation:
            {conversation_text}
            
            Respond in JSON format:
            {{
                "summary": "Brief 2-3 sentence summary",
                "topics": ["Biology>Cells", "USMLE>Cardiology"],
                "sentiment": 0.5,
                "needs": ["practice questions", "concept clarification"],
                "upcoming_events": [
                    {{"type": "exam", "subject": "Biology", "timeframe": "next week", "confidence": "low"}},
                    {{"type": "appointment", "context": "study session", "timeframe": "tomorrow"}}
                ],
                "follow_up_triggers": [
                    {{"trigger": "exam_prep", "subject": "Biology", "days_before": 2, "message_type": "last_minute_prep"}},
                    {{"trigger": "post_exam", "subject": "Biology", "days_after": 1, "message_type": "how_did_it_go"}},
                    {{"trigger": "appointment_followup", "days_after": 1, "message_type": "session_feedback"}}
                ],
                "learning_gaps": ["weak in cellular respiration", "needs more practice with diagrams"],
                "engagement_level": "high|medium|low"
            }}
            """
            
            if self.client:
                response = self.client.chat.completions.create(
                    model="gpt-4",  # Using GPT-4 for better analysis
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,  # Lower temperature for more consistent analysis
                    max_tokens=800
                )
                
                analysis = json.loads(response.choices[0].message.content)
            else:
                # Enhanced fallback analysis
                analysis = self._enhanced_fallback_analysis(conversation_text)
            
            # Generate embedding
            embedding = self._generate_embedding(analysis['summary'])
            analysis['embedding'] = embedding
            
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze conversation: {str(e)}")
            return self._enhanced_fallback_analysis(conversation_text)
    
    async def generate_email_triggers(self, user_data: Dict[str, Any], conversation_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate intelligent email triggers based on conversation analysis and user context
        """
        try:
            prompt = f"""
            Based on this student's data and recent conversation analysis, generate personalized email triggers:
            
            User Context:
            - Recent activity: {user_data.get('recent_activity', 'N/A')}
            - Course progress: {user_data.get('course_completion', 'N/A')}%
            - Test performance: {user_data.get('test_accuracy', 'N/A')}
            - Subject focus: {user_data.get('subject_affinity', 'N/A')}
            
            Conversation Analysis:
            - Topics discussed: {conversation_analysis.get('topics', [])}
            - Upcoming events: {conversation_analysis.get('upcoming_events', [])}
            - Learning gaps: {conversation_analysis.get('learning_gaps', [])}
            - Engagement level: {conversation_analysis.get('engagement_level', 'medium')}
            
            Generate 2-3 specific email triggers with timing and content suggestions:
            
            {{
                "triggers": [
                    {{
                        "trigger_type": "exam_prep_reminder",
                        "timing": "2 days before exam",
                        "subject_line": "Last-minute Biology prep tips for your exam",
                        "content_focus": "cellular respiration practice questions",
                        "priority": "high",
                        "personalization": "Based on your recent questions about cellular respiration"
                    }}
                ]
            }}
            """
            
            if self.client:
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=600
                )
                
                result = json.loads(response.choices[0].message.content)
                return result.get('triggers', [])
            else:
                return self._fallback_email_triggers(user_data, conversation_analysis)
                
        except Exception as e:
            logger.error(f"Failed to generate email triggers: {str(e)}")
            return self._fallback_email_triggers(user_data, conversation_analysis)

    async def generate_email_content(self, template_data: Dict[str, Any]) -> str:
        """
        Generate personalized email content using LLM with enhanced context awareness
        """
        try:
            prompt = f"""
            Generate a personalized educational email for:
            
            Student: {template_data.get('first_name', 'Student')}
            Context: {template_data.get('context', 'general learning')}
            Trigger: {template_data.get('trigger_type', 'engagement')}
            Subject Focus: {template_data.get('subject', 'studies')}
            Recent Activity: {template_data.get('recent_activity', 'N/A')}
            Learning Gaps: {template_data.get('learning_gaps', [])}
            
            Email should be:
            - 2-3 sentences maximum
            - Encouraging and supportive
            - Specific to their learning context
            - Include actionable next steps
            
            Examples:
            - For exam prep: "Hi Sarah! I noticed you've been working hard on cellular respiration. Here's a quick practice quiz to boost your confidence before tomorrow's Biology exam."
            - For post-exam: "Hey Mike! How did your USMLE practice test go yesterday? If you'd like to review any challenging topics, I'm here to help."
            - For course progress: "Great job completing 60% of your course, Lisa! You're making excellent progress - let's tackle the next module on organic chemistry reactions."
            """
            
            if self.client:
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=150
                )
                
                return response.choices[0].message.content.strip()
            else:
                return self._enhanced_fallback_email_content(template_data)
                
        except Exception as e:
            logger.error(f"Failed to generate email content: {str(e)}")
            return self._enhanced_fallback_email_content(template_data)
    
    async def analyze_conversations_for_triggers(self, conversations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze multiple conversations and extract email triggers
        """
        if not conversations:
            return {
                'topics': [],
                'triggers': [],
                'insights': {},
                'sentiment_avg': 0.0
            }
        
        try:
            # Combine all conversation text
            all_text = []
            for conv in conversations:
                if isinstance(conv, dict) and 'content' in conv:
                    all_text.append(conv['content'])
            
            combined_text = ' '.join(all_text)
            
            # Analyze the combined conversations
            analysis = await self.analyze_conversation(combined_text)
            
            # Extract topics and triggers
            topics = analysis.get('topics', [])
            triggers = analysis.get('follow_up_triggers', [])
            
            # Calculate average sentiment
            sentiment_avg = analysis.get('sentiment', 0.0)
            
            # Generate insights
            insights = {
                'engagement_level': analysis.get('engagement_level', 'medium'),
                'learning_gaps': analysis.get('learning_gaps', []),
                'upcoming_events': analysis.get('upcoming_events', []),
                'needs': analysis.get('needs', [])
            }
            
            return {
                'topics': topics,
                'triggers': triggers,
                'insights': insights,
                'sentiment_avg': sentiment_avg
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze conversations for triggers: {str(e)}")
            return {
                'topics': [],
                'triggers': [],
                'insights': {},
                'sentiment_avg': 0.0
            }
    
    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for text using OpenAI or fallback"""
        if not text:
            return [0.0] * 1536  # Default OpenAI embedding size
        
        try:
            if self.client:
                response = self.client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=text
                )
                return response.data[0].embedding
            else:
                # Simple fallback - hash-based pseudo-embedding
                import hashlib
                hash_obj = hashlib.md5(text.encode())
                hash_hex = hash_obj.hexdigest()
                # Convert hex to normalized float values
                embedding = []
                for i in range(0, len(hash_hex), 2):
                    val = int(hash_hex[i:i+2], 16) / 255.0 - 0.5  # Normalize to [-0.5, 0.5]
                    embedding.append(val)
                # Pad or truncate to 1536 dimensions
                while len(embedding) < 1536:
                    embedding.extend(embedding[:min(len(embedding), 1536 - len(embedding))])
                return embedding[:1536]
        except Exception as e:
            logger.error(f"Failed to generate embedding: {str(e)}")
            return [0.0] * 1536
    
    def _enhanced_fallback_analysis(self, conversation_text: str) -> Dict[str, Any]:
        """Enhanced fallback analysis when LLM is not available"""
        text_lower = conversation_text.lower()
        
        topics = []
        topic_patterns = {
            'Biology>Cells': ['cell', 'cellular', 'mitochondria', 'nucleus', 'membrane', 'organelle'],
            'Biology>Photosynthesis': ['photosynthesis', 'chloroplast', 'sunlight', 'glucose', 'oxygen'],
            'Biology>Genetics': ['gene', 'dna', 'chromosome', 'heredity', 'mutation'],
            'History>Ancient': ['stone age', 'neolithic', 'paleolithic', 'ancient', 'civilization', 'prehistoric'],
            'History>Medieval': ['medieval', 'middle ages', 'feudal', 'crusades', 'renaissance'],
            'History>Modern': ['industrial revolution', 'world war', 'modern history', '20th century'],
            'USMLE>Cardiology': ['heart', 'cardiac', 'ecg', 'blood pressure', 'arrhythmia'],
            'USMLE>Pharmacology': ['drug', 'medication', 'dosage', 'side effect', 'pharmacokinetics', 'antibiotic', 'vancomycin', 'acyclovir'],
            'USMLE>Pathology': ['pathology', 'disease', 'diagnosis', 'symptoms', 'pathogen'],
            'Medicine>General': ['medical', 'patient', 'treatment', 'clinical', 'hospital'],
            'Algebra>Variables': ['variable', 'equation', 'solve', 'x =', 'algebra'],
            'Chemistry>Organic': ['organic', 'carbon', 'molecule', 'reaction', 'synthesis'],
            'Exam>Preparation': ['exam', 'test', 'quiz', 'preparation', 'study', 'tomorrow', 'prepared']
        }
        
        for topic, keywords in topic_patterns.items():
            if any(keyword in text_lower for keyword in keywords):
                topics.append(topic)
        
        upcoming_events = []
        if any(word in text_lower for word in ['exam tomorrow', 'test tomorrow', 'exam', 'test', 'quiz']):
            # Determine subject from detected topics
            subject = "General"
            if any('Biology>' in topic for topic in topics):
                subject = "Biology"
            elif any('History>' in topic for topic in topics):
                subject = "History"
            elif any('USMLE>' in topic for topic in topics):
                subject = "USMLE"
            
            upcoming_events.append({
                "type": "exam",
                "subject": subject,
                "timeframe": "tomorrow" if "tomorrow" in text_lower else "upcoming",
                "confidence": "high" if "tomorrow" in text_lower else "medium"
            })
        
        if any(word in text_lower for word in ['appointment', 'meeting', 'session']):
            upcoming_events.append({
                "type": "appointment",
                "context": "study session",
                "timeframe": "soon"
            })
        
        # Enhanced sentiment analysis
        positive_words = ['understand', 'clear', 'helpful', 'good', 'great', 'confident', 'ready']
        negative_words = ['confused', 'difficult', 'hard', 'stuck', 'frustrated', 'worried', 'struggling']
        
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        sentiment = (positive_count - negative_count) / max(positive_count + negative_count, 1)
        
        return {
            'summary': f'Educational conversation covering {", ".join(topics[:2]) if topics else "general topics"}.',
            'topics': topics,
            'sentiment': max(-1, min(1, sentiment)),
            'needs': ['practice questions', 'concept review'] if negative_count > 0 else ['advanced practice'],
            'upcoming_events': upcoming_events,
            'follow_up_triggers': [
                {"trigger": "check_progress", "days_after": 2, "message_type": "how_are_you_doing"}
            ] if upcoming_events else [],
            'learning_gaps': ['needs more practice'] if negative_count > 0 else [],
            'engagement_level': 'high' if positive_count > negative_count else 'medium' if sentiment >= 0 else 'low'
        }
    
    def _fallback_email_triggers(self, user_data: Dict[str, Any], conversation_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fallback email triggers when LLM is not available"""
        triggers = []
        
        # Course completion trigger
        completion = user_data.get('course_completion', 0)
        if 40 <= completion < 60:
            triggers.append({
                "trigger_type": "course_progress_encouragement",
                "timing": "immediate",
                "subject_line": f"You're {completion}% there! Keep going!",
                "content_focus": "progress celebration and next steps",
                "priority": "medium"
            })
        
        # Low performance trigger
        if user_data.get('test_accuracy', 1.0) < 0.6:
            triggers.append({
                "trigger_type": "performance_support",
                "timing": "immediate",
                "subject_line": "Let's boost your test scores together",
                "content_focus": "study strategies and practice resources",
                "priority": "high"
            })
        
        return triggers
    
    def _enhanced_fallback_email_content(self, template_data: Dict[str, Any]) -> str:
        """Enhanced fallback email content when LLM is not available"""
        first_name = template_data.get('first_name', 'Student')
        context = template_data.get('context', 'your studies')
        trigger_type = template_data.get('trigger_type', 'engagement')
        
        if trigger_type == 'exam_prep':
            return f"Hi {first_name}! I noticed you have an upcoming exam. Here are some targeted practice questions to help you feel confident and prepared."
        elif trigger_type == 'post_exam':
            return f"Hey {first_name}! How did your exam go? I'm here if you'd like to review any topics or discuss what's next in your learning journey."
        elif trigger_type == 'course_progress':
            return f"Great progress on {context}, {first_name}! You're doing excellent work - let's keep the momentum going with the next section."
        else:
            return f"Keep up the fantastic work on {context}, {first_name}! Your dedication to learning is inspiring, and I'm here to support your success."

    async def generate_educational_email(self, rule_id: str, user_features: Dict[str, Any], user_email: str) -> Dict[str, str]:
        """
        Generate educational email content based on rule and user features
        """
        try:
            # Extract user context
            first_name = user_features.get('first_name', user_email.split('@')[0].title())
            top_topics = user_features.get('top_topics', [])
            engagement_level = 'high' if user_features.get('frequency_7d', 0) > 50 else 'medium'
            course_completion = user_features.get('icp_completion_rate', 0)
            test_accuracy = user_features.get('test_accuracy', 0)
            
            # Create context-aware prompt
            prompt = f"""
            Generate a personalized educational email for a student:
            
            Student: {first_name}
            Email Rule: {rule_id}
            Learning Topics: {', '.join(top_topics[:3]) if top_topics else 'General studies'}
            Engagement Level: {engagement_level}
            Course Progress: {course_completion:.0f}%
            Test Performance: {test_accuracy:.0%}
            
            Create an encouraging, educational email that:
            - Is appropriate for K4-graduate level students
            - Addresses their specific learning context
            - Provides actionable next steps
            - Is 2-3 sentences maximum
            - Has an engaging subject line
            
            Return JSON format:
            {{
                "subject": "Engaging subject line",
                "content": "Personalized email content"
            }}
            """
            
            if self.client:
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7,
                    max_tokens=200
                )
                
                import json
                result = json.loads(response.choices[0].message.content)
                return {
                    'subject': result.get('subject', f'Keep learning, {first_name}!'),
                    'content': result.get('content', f'Great work on your studies, {first_name}!')
                }
            else:
                # Fallback educational email generation
                return self._generate_fallback_educational_email(rule_id, user_features, first_name)
                
        except Exception as e:
            logger.error(f"Failed to generate educational email: {str(e)}")
            return self._generate_fallback_educational_email(rule_id, user_features, first_name)
    
    def _generate_fallback_educational_email(self, rule_id: str, user_features: Dict[str, Any], first_name: str) -> Dict[str, str]:
        """Generate fallback educational email when OpenAI is unavailable"""
        top_topics = user_features.get('top_topics', [])
        course_completion = user_features.get('icp_completion_rate', 0)
        
        if 'engagement' in rule_id:
            subject = f"Amazing progress, {first_name}! 🎓"
            content = f"Hi {first_name}! Your dedication to learning is impressive. Keep up the excellent work on {top_topics[0] if top_topics else 'your studies'} - you're making real progress!"
        elif 'biology' in rule_id or any('Biology' in topic for topic in top_topics):
            subject = f"Biology study boost for {first_name}"
            content = f"Hey {first_name}! I see you're diving deep into biology concepts. Here are some practice questions to reinforce your understanding of cellular processes."
        elif 'history' in rule_id or any('History' in topic for topic in top_topics):
            subject = f"History insights for {first_name}"
            content = f"Hi {first_name}! Your exploration of historical topics is fascinating. Let's connect those ancient civilizations to modern concepts you're studying."
        elif course_completion > 0:
            subject = f"You're {course_completion:.0f}% there, {first_name}!"
            content = f"Great job reaching {course_completion:.0f}% completion, {first_name}! Your consistent effort is paying off - let's tackle the next section together."
        else:
            subject = f"Keep learning, {first_name}!"
            content = f"Hi {first_name}! Your learning journey is unique and valuable. I'm here to support you every step of the way with personalized guidance."
        
        return {
            'subject': subject,
            'content': content
        }
