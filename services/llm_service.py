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
        **kwargs,  # <- accept optional steering hints
    ) -> Dict[str, str]:
        """
        Generate short educational email copy. Supports optional hints:
          - preferred_subject: str | None    e.g., "IELTS", "Biology"
          - day_hint: str | None             one of: tonight|today|tomorrow|soon
          - metrics_scope: "overall"|"subject" (default "overall")
          - instructions_extra: str          additional guardrails

        Returns {"subject": "...", "content": "..."}.
        Never invent per-subject percentages/accuracy unless explicitly provided.
        """
        preferred_subject: Optional[str] = kwargs.get("preferred_subject")
        day_hint: Optional[str] = kwargs.get("day_hint")
        metrics_scope: str = kwargs.get("metrics_scope") or "overall"
        instructions_extra: str = kwargs.get("instructions_extra") or ""

        # Derive a friendly first name
        first_name = user_features.get("first_name")
        if not first_name:
            try:
                first_name = user_email.split("@")[0]
            except Exception:
                first_name = "Student"
        first_name = (first_name or "Student").title()

        top_topics = user_features.get("top_topics", []) or []
        test_accuracy_overall = user_features.get("test_accuracy")
        course_completion_overall = user_features.get("icp_completion_rate")

        # Build steering lines
        subject_line_nudge = ""
        body_opening_nudge = ""
        if rule_id == "exam_last_minute_prep":
            # Keep this generic enough to work for any subject/exam
            if preferred_subject and day_hint:
                subject_line_nudge = f"{day_hint.title()}’s {preferred_subject} exam"
                body_opening_nudge = f"Your {preferred_subject} exam is {day_hint}."
            elif preferred_subject:
                subject_line_nudge = f"{preferred_subject} exam"
                body_opening_nudge = f"Your {preferred_subject} exam is coming up."
            elif day_hint:
                subject_line_nudge = f"{day_hint.title()}’s exam"
                body_opening_nudge = f"Your exam is {day_hint}."

        # Guardrails about metrics
        if metrics_scope == "overall":
            metrics_rules = (
                "Do NOT state or imply per-subject percentages/accuracy/progress. "
                "If you mention progress or accuracy, make it clear these are overall metrics "
                "across studies; better yet, avoid numeric percentages entirely."
            )
        else:
            metrics_rules = (
                "Only mention per-subject metrics if they are explicitly provided. "
                "Never invent numbers."
            )

        # Compose the prompt
        prompt = f"""
Return ONLY valid JSON with keys "subject" and "content", nothing else.

Student: {first_name}
Rule: {rule_id}
PreferredSubjectHint: {preferred_subject or "None"}
DayHint: {day_hint or "None"}
TopTopics (for context only): {top_topics}

Requirements:
- 2–3 sentences max in the email body, supportive, actionable.
- If Rule is "exam_last_minute_prep" and hints are present, include the subject/day in BOTH subject and body.
- {metrics_rules}
- Avoid making up facts, scores, dates, or links.
- Tone: encouraging, academic coach, concise.

Style nudges (use if they fit):
- Subject should be short and actionable.
- If day hint given (tonight/today/tomorrow), reflect immediacy.

{("Extra instructions: " + instructions_extra) if instructions_extra else ""}

Examples of acceptable subjects (not literal):
- "Tonight’s IELTS exam: 45-minute crash plan"
- "Tomorrow’s Biology exam: quick last-minute checklist"

Now produce JSON:
{{
  "subject": "S concise subject line{(' — ' + subject_line_nudge) if subject_line_nudge else ''}",
  "content": "{body_opening_nudge + ' ' if body_opening_nudge else ''}<<Write 2–3 supportive, actionable sentences with no fabricated metrics.>>"
}}
""".strip()

        try:
            if self.client:
                resp = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.5,
                    max_tokens=220,
                )
                raw = resp.choices[0].message.content.strip()
                try:
                    result = json.loads(raw)
                except Exception:
                    # Try to recover JSON object from text
                    import re
                    m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
                    result = json.loads(m.group(0)) if m else {}
                subject = (result.get("subject") or "").strip()
                content = (result.get("content") or "").strip()
                if not subject or not content:
                    raise ValueError("LLM returned empty fields")
                return {"subject": subject, "content": content}
        except Exception as e:
            logger.warning("LLM email generation failed in LLMService: %s", str(e))

        # Fallback: deterministic copy guided by hints
        subj_hint = preferred_subject or "your"
        when = (day_hint or "soon")
        if rule_id == "exam_last_minute_prep":
            subject = f"{when.title()}’s {subj_hint} exam: 45-minute crash plan ✅"
            content = (
                f"Hello {first_name}!\n\n"
                f"Your {subj_hint} exam is {when}. Here’s a focused, high-yield plan:\n\n"
                "• 15 min — quick review of your most-missed ideas\n"
                "• 15 min — 10 mixed practice Qs (no notes)\n"
                "• 10 min — check answers & fix 2 weak patterns\n"
                "• 5  min — one-page cheat sheet (from memory, then fill gaps)\n\n"
                "Reply “START” and I’ll queue a 10-question mini-set now."
            )
        elif rule_id == "exam_post_checkin":
            subject = f"How did the {subj_hint} exam go? 📚"
            content = (
                f"Hi {first_name}! How did it go? Jot one solid concept, one surprise, and one target for next week. "
                "Reply “DEBRIEF” and I’ll prep a quick review set from your tricky areas."
            )
        else:
            subject = f"Keep going, {first_name}! 🎓"
            content = (
                f"Hi {first_name}! Let’s lock in a quick 10-minute study block today. "
                "Reply “GO” and I’ll send a focused set right away."
            )

        return {"subject": subject, "content": content}
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
