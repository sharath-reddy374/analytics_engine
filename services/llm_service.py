# services/llm_service.py
import openai
from typing import Dict, List, Any, Optional
import logging
import json
import re
from config.settings import settings

logger = logging.getLogger(__name__)

def _safe_json_extract(text: str) -> Dict[str, Any]:
    """
    Best-effort: extract first {...} JSON object from a string and load it.
    Falls back to empty dict if parsing fails.
    """
    if not text:
        return {}
    # Try direct
    try:
        return json.loads(text)
    except Exception:
        pass
    # Try to find a JSON object inside the text
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return {}
    return {}

class LLMService:
    """
    Service for LLM-based conversation understanding and content generation
    """

    def __init__(self):
        self.client = None
        if getattr(settings, "OPENAI_API_KEY", None):
            self.client = openai.Client(api_key=settings.OPENAI_API_KEY)

        # Embedding model (optional)
        try:
            self.embedding_model = None
        except Exception as e:
            logger.warning(f"Failed to load embedding model: {str(e)}")
            self.embedding_model = None

    # --------- ANALYSIS (unchanged major logic) ------------------------------

    async def analyze_conversation(self, conversation_text: str) -> Dict[str, Any]:
        """
        Analyze conversation to extract summary, topics, sentiment, needs, and follow-up triggers
        """
        try:
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
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                    max_tokens=800
                )
                analysis = _safe_json_extract(response.choices[0].message.content)
            else:
                analysis = self._enhanced_fallback_analysis(conversation_text)

            # Optional embedding
            embedding = self._generate_embedding(analysis.get('summary', ''))
            analysis['embedding'] = embedding
            return analysis

        except Exception as e:
            logger.error(f"Failed to analyze conversation: {str(e)}")
            return self._enhanced_fallback_analysis(conversation_text)

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
            all_text = []
            for conv in conversations:
                if isinstance(conv, dict) and 'content' in conv:
                    all_text.append(conv['content'])
            combined_text = ' '.join(all_text)

            analysis = await self.analyze_conversation(combined_text)
            topics = analysis.get('topics', [])
            triggers = analysis.get('follow_up_triggers', [])
            sentiment_avg = analysis.get('sentiment', 0.0)
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

    # --------- EMAIL GENERATION (updated) -----------------------------------

    async def generate_educational_email(
        self,
        rule_id: str,
        user_features: Dict[str, Any],
        user_email: str,
        preferred_subject: Optional[str] = None,
        day_hint: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate educational email content based on rule and user features.
        Now accepts preferred_subject + day_hint so urgent triggers (e.g. IELTS tonight)
        override generic top_topics.
        """
        try:
            first_name = user_features.get('first_name', user_email.split('@')[0].title())
            top_topics = user_features.get('top_topics', [])
            engagement_level = 'high' if user_features.get('frequency_7d', 0) > 50 else 'medium'
            course_completion = user_features.get('icp_completion_rate', 0)
            test_accuracy = user_features.get('test_accuracy', 0)

            preferred_subject = preferred_subject or (
                (top_topics[0].split('>')[0]) if top_topics else 'General studies'
            )
            day_hint = (day_hint or '').strip()  # "", "tonight", "today", "tomorrow", "soon"

            # Build explicit, overridable instructions
            time_context = ""
            if day_hint:
                time_context = f"\n- TIME CONTEXT: The correct time descriptor you MUST use is \"{day_hint}\".\n"

            prompt = f"""
You are writing a short, supportive educational email.

HARD REQUIREMENTS:
- PURPOSE is "{rule_id}". Always write copy suited to that purpose.
- PREFERRED_SUBJECT is "{preferred_subject}". You MUST focus on this subject. Ignore other subjects if mentioned elsewhere.
{time_context}- Keep it to 2–3 sentences total.
- Output must be STRICT JSON with keys "subject" and "content" only.

CONTEXT:
- Student name: {first_name}
- Engagement level: {engagement_level}
- Course progress: {course_completion:.0%}
- Test performance: {test_accuracy:.0%}
- Recent topics (for reference only): {', '.join(topics if (topics := [t for t in top_topics]) else ['General studies'])}

STYLE & TEMPLATES:
- Be encouraging, specific, and actionable.
- If PURPOSE is "exam_last_minute_prep":
  - Subject should include the TIME CONTEXT word (if provided) and the PREFERRED_SUBJECT.
  - Example subject: "Tonight’s {preferred_subject} exam: 45-minute crash plan ✅"
  - Body: a compact, practical mini-plan (no more than 2–3 sentences total).
- If PURPOSE is "exam_post_checkin": ask how it went and offer a short debrief.
- If PURPOSE is "course_completion_celebration": congratulate + suggest next step.

Return ONLY JSON:
{{
  "subject": "…",
  "content": "…"
}}
"""

            if self.client:
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.5,
                    max_tokens=220
                )
                result = _safe_json_extract(response.choices[0].message.content)
                subject = result.get('subject') or f"Keep learning, {first_name}!"
                content = result.get('content') or f"Hi {first_name}! Quick study nudge for {preferred_subject}."
                return {'subject': subject, 'content': content}
            else:
                # Fallback if no client
                subject_time = (day_hint.capitalize() + "’s ") if day_hint else ""
                return {
                    'subject': f"{subject_time}{preferred_subject} — quick plan to make progress",
                    'content': f"Hi {first_name}! Focus on a 20-minute review of {preferred_subject} now and a 10-question check right after. Short bursts beat cramming."
                }

        except Exception as e:
            logger.error(f"Failed to generate educational email: {str(e)}")
            first_name = user_email.split('@')[0].title()
            subj = "Keep going — you’ve got this! 🎓"
            body = f"Quick nudge: take a short review today and try 5 practice questions, {first_name}. Small steps compound fast."
            return {'subject': subj, 'content': body}

    # --------- Other helpers (unchanged) ------------------------------------

    def _generate_embedding(self, text: str) -> List[float]:
        if not text:
            return [0.0] * 1536
        try:
            if self.client:
                response = self.client.embeddings.create(
                    model="text-embedding-ada-002",
                    input=text
                )
                return response.data[0].embedding
            else:
                import hashlib
                h = hashlib.md5(text.encode()).hexdigest()
                emb = []
                for i in range(0, len(h), 2):
                    val = int(h[i:i+2], 16) / 255.0 - 0.5
                    emb.append(val)
                while len(emb) < 1536:
                    emb.extend(emb[:min(len(emb), 1536 - len(emb))])
                return emb[:1536]
        except Exception as e:
            logger.error(f"Failed to generate embedding: {str(e)}")
            return [0.0] * 1536

    def _enhanced_fallback_analysis(self, conversation_text: str) -> Dict[str, Any]:
        text_lower = conversation_text.lower()
        topics = []
        topic_patterns = {
            'Biology>Cells': ['cell', 'cellular', 'mitochondria', 'nucleus', 'membrane', 'organelle'],
            'Biology>Photosynthesis': ['photosynthesis', 'chloroplast', 'sunlight', 'glucose', 'oxygen'],
            'Biology>Genetics': ['gene', 'dna', 'chromosome', 'heredity', 'mutation'],
            'History>Ancient': ['stone age', 'neolithic', 'paleolithic', 'ancient', 'civilization', 'prehistoric'],
            'USMLE>Pharmacology': ['drug', 'medication', 'dosage', 'side effect', 'pharmacokinetics', 'antibiotic', 'vancomycin', 'acyclovir'],
            'Medicine>General': ['medical', 'patient', 'treatment', 'clinical', 'hospital'],
            'Exam>Preparation': ['exam', 'test', 'quiz', 'preparation', 'study', 'tomorrow', 'today', 'tonight', 'prepared'],
            'IELTS>Listening': ['ielts', 'listening', 'band', 'speaking', 'reading', 'writing']
        }
        for topic, keywords in topic_patterns.items():
            if any(k in text_lower for k in keywords):
                topics.append(topic)

        upcoming_events = []
        if any(w in text_lower for w in ['exam', 'test', 'quiz']):
            subj = "Biology" if any('Biology>' in t for t in topics) else "IELTS" if any('IELTS>' in t for t in topics) else "General"
            tf = "tonight" if "tonight" in text_lower else "today" if "today" in text_lower else "tomorrow" if "tomorrow" in text_lower else "upcoming"
            upcoming_events.append({"type": "exam", "subject": subj, "timeframe": tf, "confidence": "medium"})

        sentiment = 0.0
        if any(w in text_lower for w in ['confused', 'difficult', 'hard', 'stuck', 'frustrated', 'worried', 'struggling']):
            sentiment -= 0.5
        if any(w in text_lower for w in ['understand', 'clear', 'helpful', 'good', 'great', 'confident', 'ready']):
            sentiment += 0.5

        return {
            'summary': f'Educational conversation covering {", ".join(topics[:2]) if topics else "general topics"}.',
            'topics': topics,
            'sentiment': max(-1, min(1, sentiment)),
            'needs': ['practice questions', 'concept review'] if sentiment < 0 else ['advanced practice'],
            'upcoming_events': upcoming_events,
            'follow_up_triggers': [
                {"trigger": "check_progress", "days_after": 2, "message_type": "how_are_you_doing"}
            ] if upcoming_events else [],
            'learning_gaps': ['needs more practice'] if sentiment < 0 else [],
            'engagement_level': 'high' if sentiment > 0 else 'medium' if sentiment == 0 else 'low'
        }
