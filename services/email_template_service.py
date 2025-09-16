# email_template_service.py
from typing import Dict, Any, Optional, List, Tuple
import logging
from datetime import datetime
import asyncio
from services.llm_service import LLMService

logger = logging.getLogger(__name__)

class EmailTemplateService:
    """
    Service for generating dynamic educational email content using OpenAI.
    Adds purpose-aware fallbacks so time-sensitive emails (exams/appointments)
    always read correctly even if the LLM returns generic copy.
    """

    def __init__(self):
        self.llm_service = LLMService()

    # ---- Public API ---------------------------------------------------------

    def generate_email_content(
        self,
        template_id: str,
        features: Dict[str, Any],
        ai_triggers: Optional[List[Dict[str, Any]]] = None
    ) -> Optional[Dict[str, str]]:
        """
        Generate educational email content dynamically using OpenAI.
        If the LLM output doesn't match the intended purpose, fall back to a
        deterministic, purpose-specific subject and body.
        """
        try:
            # Build context
            purpose = self._determine_email_purpose(template_id, features, ai_triggers)
            email_context = self._build_email_context(features, ai_triggers, purpose)

            # Build purpose-specific subject/body (deterministic)
            fallback_subject, fallback_body = self._compose_subject_content(email_context)

            # Try LLM
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                email_result = loop.run_until_complete(
                    self.llm_service.generate_educational_email(
                        template_id,                      # rule_id
                        features,                         # user_features
                        features.get('email', 'student@example.com')  # user_email
                    )
                )
                loop.close()

                llm_subject = (email_result or {}).get('subject') or ''
                llm_body = (email_result or {}).get('content') or ''

                # If LLM output doesn't align with purpose, use fallback
                if not self._is_alignment_ok(purpose, llm_subject, llm_body):
                    logger.info("LLM output misaligned with purpose '%s' — using fallback.", purpose)
                    subject, content = fallback_subject, fallback_body
                else:
                    subject, content = llm_subject, llm_body

            except Exception as e:
                logger.warning(f"OpenAI generation failed, using fallback: {str(e)}")
                subject, content = fallback_subject, fallback_body

            return {
                'subject': subject,
                'content': content,
                'template_id': template_id,
                'generated_at': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to generate email content for {template_id}: {str(e)}")
            return None

    def get_available_templates(self) -> list:
        """Return empty list since we no longer use predefined templates."""
        return []

    def preview_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Return None since we generate content dynamically."""
        return None

    # ---- Context / Purpose helpers -----------------------------------------

    def _build_email_context(
        self,
        features: Dict[str, Any],
        ai_triggers: Optional[List[Dict[str, Any]]],
        purpose: str
    ) -> Dict[str, Any]:
        """Collect everything we might want for composition."""
        topics = features.get('top_topics', [])
        primary_subjects = self._extract_subjects_from_topics(topics)
        subject_area = primary_subjects[0] if primary_subjects else self._guess_subject_from_topics(topics)

        context = {
            'email_purpose': purpose,
            'user_profile': {
                'learning_level': self._determine_learning_level(features),
                'primary_subjects': primary_subjects,
                'engagement_level': self._assess_engagement_level(features),
                'recent_activity': {
                    'conversations': features.get('conversations_7d', 0),
                    'tests_taken': features.get('tests_7d', 0),
                    'minutes_studied': features.get('minutes_7d', 0),
                    'courses_active': features.get('active_courses', 0),
                }
            },
            'learning_insights': {
                'top_topics': topics,
                'strong_subjects': features.get('strong_subjects', []),
                'weak_subjects': features.get('weak_subjects', []),
                'test_accuracy': features.get('test_accuracy', 0.0),
                'course_completion': features.get('icp_completion_rate', 0.0),
                'completed_course_titles': features.get('completed_course_titles', []),
                'conversation_insights': features.get('conversation_insights', {}),
            },
            'ai_triggers': ai_triggers or features.get('ai_email_triggers', []) or [],
            'personalization': {
                'recency_days': features.get('recency_days', 0),
                'frequency': features.get('frequency_7d', 0),
                'improvement_trend': features.get('itp_improvement_trend', 0),
            },
            'subject_area': subject_area
        }
        return context

    def _determine_email_purpose(
        self,
        template_id: str,
        features: Dict[str, Any],
        ai_triggers: Optional[List[Dict[str, Any]]]
    ) -> str:
        """Determine the main purpose of the email from triggers/state."""
        recency = features.get('recency_days', 0)
        engagement = self._assess_engagement_level(features)

        triggers = ai_triggers or features.get('ai_email_triggers', []) or []
        for t in triggers:
            if not isinstance(t, dict):
                continue
            t_type = t.get('trigger') or t.get('trigger_type')
            stage = t.get('message_type')

            # Exams
            if stage == 'last_minute_prep' or t_type in ('exam_prep', 'pre_exam'):
                return 'exam_last_minute_prep'
            if stage in ('how_did_it_go', 'post_exam') or t_type in ('exam_followup', 'post_exam'):
                return 'exam_followup'

            # Appointments
            if t_type == 'appointment_reminder' or stage == 'reminder':
                return 'appointment_reminder'
            if t_type == 'appointment_followup' or stage in ('session_feedback', 'post_appointment'):
                return 'appointment_followup'

            # Learning support
            if t_type == 'learning_support' or stage == 'learning_support_offer':
                return 'learning_support'

        # Fallbacks by state
        if recency > 7:
            return 'winback'
        if engagement == 'very_high':
            return 'engagement_reward'
        if features.get('completed_courses', 0) > 0:
            return 'completion_celebration'
        if features.get('test_accuracy', 0) > 0.8:
            return 'performance_praise'
        return 'learning_encouragement'

    # ---- Subject/Body composition ------------------------------------------

    def _compose_subject_content(self, ctx: Dict[str, Any]) -> Tuple[str, str]:
        """Deterministic subject/body for each purpose."""
        purpose = ctx['email_purpose']
        subject_area = ctx.get('subject_area') or 'your studies'
        level = ctx['user_profile']['learning_level']
        greet = "Hi there!" if level == 'middle_school' else "Hello!"

        # Pull helpful context if present
        conv = ctx['learning_insights'].get('conversation_insights', {}) or {}
        upcoming = conv.get('upcoming_events', []) if isinstance(conv, dict) else []
        has_exam_tomorrow = any(e.get('type') == 'exam' and 'tomorrow' in str(e.get('timeframe', '')).lower() for e in upcoming)
        has_appt_tomorrow = any(e.get('type') == 'appointment' and 'tomorrow' in str(e.get('timeframe', '')).lower() for e in upcoming)

        # Completed course title if any
        completed_titles = ctx['learning_insights'].get('completed_course_titles', [])
        course_name = completed_titles[0] if completed_titles else subject_area

        if purpose == 'exam_last_minute_prep':
            day_hint = "tomorrow" if has_exam_tomorrow else "soon"
            subject = f"{day_hint.title()}’s {subject_area} exam: 45-minute crash plan ✅"
            body = f"""{greet}

Your {subject_area} exam is {day_hint}. Here’s a focused, high-yield plan:

⏱️ **45-minute sprint**
• 15 min — Quick review: key formulas/definitions you’ve missed recently  
• 15 min — 10 mixed practice Qs (no notes)  
• 10 min — Check answers + fix 2 weakest patterns  
• 5  min — One-page cheat sheet (from memory → then fill the gaps)

🧠 **Hit these targets**
• 2 concepts you tripped on this week  
• 1 typical trap (timing or careless error)  
• 1 confidence topic to warm up

⚙️ **Setup**
• Timer on, notifications off  
• Close-book first pass, open-book second pass  
• If a question takes >90s, mark & move — keep momentum

Reply “START” and I’ll send a 10-question mini-quiz tailored to {subject_area} now. Good luck — you’ve got this!"""
            return subject, body

        if purpose == 'exam_followup':
            subject = f"How did the {subject_area} exam go? 📚"
            body = f"""{greet}

How did your {subject_area} exam go? A 3-minute reflection now will boost retention:

📝 **Reflect**
• One concept that felt solid  
• One that surprised you  
• One you want to master next week

Want a quick debrief quiz from your tricky areas? Reply “DEBRIEF” and I’ll tailor it."""
            return subject, body

        if purpose == 'appointment_reminder':
            when = "tomorrow" if has_appt_tomorrow else "soon"
            subject = f"Reminder: your appointment {when} 🕒"
            body = f"""{greet}

Quick reminder: your appointment is {when}. A 10-minute prep helps it go smoothly:

✅ **Prep checklist**
• Confirm time/location & any materials  
• Write 2–3 questions you want answered  
• Plan travel time + a buffer

You’ve got this!"""
            return subject, body

        if purpose == 'appointment_followup':
            subject = "How did your session go? 📝"
            body = f"""{greet}

Hope your session went well. Capture value while it’s fresh:

🔁 **Post-session notes**
• Most helpful insight (1–2 lines)  
• Any action items + deadlines  
• What needs clarification?

Reply with your notes — I’ll turn them into a simple plan."""
            return subject, body

        if purpose == 'completion_celebration':
            subject = f"🎉 You finished {course_name}! Ready for what’s next?"
            body = f"""{greet}

Congratulations on completing <b>{course_name}</b>! That’s a big milestone.

🎓 **Lock it in**
• 5 review Qs now, 5 tomorrow (spaced)  
• Summarize the 3 biggest takeaways  
• Teach one idea to a friend — best retention hack

Want me to queue your next lesson in {ctx.get('subject_area') or 'your subject'}?"""
            return subject, body

        if purpose == 'engagement_reward':
            subject = f"Amazing progress in {subject_area}! 🌟"
            body = f"""{greet}

Your consistency in {subject_area} is outstanding. Let’s channel it:

🌟 **Next step**
• One stretch goal for this week  
• One 20-minute focused block (book it now)  
• One timed mini-set to measure improvement

Keep going — momentum is your superpower!"""
            return subject, body

        if purpose == 'learning_support':
            subject = f"Boost your {subject_area} understanding 🚀"
            body = f"""{greet}

I saw you’ve been digging into {subject_area}. Want a targeted explainer + practice set?
Reply with a topic (e.g., “cell membrane transport”) and I’ll tailor it."""
            return subject, body

        if purpose == 'winback':
            subject = f"Ready to continue your {subject_area} journey? 🎯"
            body = f"""{greet}

Let’s ease back in: 10 minutes, one concept, one quick win. I’ll line it up — just say “GO”."""
            return subject, body

        if purpose == 'performance_praise':
            subject = f"Excellent {subject_area} performance! 📈"
            body = f"""{greet}

You’re crushing {subject_area}. Want an advanced challenge set to push further?"""
            return subject, body

        # Generic encouragement
        subject = f"Your {subject_area} learning continues! 💪"
        body = f"""{greet}

Small, consistent steps compound. I can queue a 10-minute set right now — just say “START”."""
        return subject, body

    # ---- Alignment guard ----------------------------------------------------

    def _is_alignment_ok(self, purpose: str, subject: str, body: str) -> bool:
        """Lightweight purpose-vs-copy sanity check."""
        s = f"{subject} {body}".lower()

        def has_any(*words): return any(w in s for w in words)

        if purpose == 'exam_last_minute_prep':
            return has_any('exam', 'tomorrow', 'prep', 'plan', 'crash')
        if purpose == 'exam_followup':
            return has_any('exam', 'how did', 'went', 'score', 'debrief')
        if purpose == 'appointment_reminder':
            return has_any('appointment', 'reminder', 'tomorrow')
        if purpose == 'appointment_followup':
            return has_any('appointment', 'session', 'follow-up', 'follow up', 'went')
        if purpose == 'completion_celebration':
            return has_any('congrats', 'congratulations', 'completed', 'finish', 'finished')
        # Others can be looser
        return True

    # ---- Utility helpers ----------------------------------------------------

    def _determine_learning_level(self, features: Dict[str, Any]) -> str:
        topics = features.get('top_topics', [])
        advanced_indicators = ['USMLE', 'Medicine', 'Pharmacology', 'Immunology', 'Pathology']
        if any(ind in ' '.join(topics) for ind in advanced_indicators):
            return 'graduate_medical'
        intermediate_indicators = ['Biology', 'Chemistry', 'Physics', 'History', 'Algebra']
        if any(ind in ' '.join(topics) for ind in intermediate_indicators):
            return 'high_school_college'
        return 'middle_school'

    def _extract_subjects_from_topics(self, topics: list) -> list:
        subjects = set()
        for t in topics:
            if '>' in t:
                subjects.add(t.split('>')[0])
        return list(subjects)

    def _guess_subject_from_topics(self, topics: list) -> str:
        # Best-effort fallback — first token before '>' or whole topic
        if not topics:
            return 'your studies'
        t0 = topics[0]
        return t0.split('>')[0] if '>' in t0 else t0

    def _assess_engagement_level(self, features: Dict[str, Any]) -> str:
        conversations = features.get('conversations_7d', 0)
        frequency = features.get('frequency_7d', 0)
        if conversations > 50 and frequency > 100:
            return 'very_high'
        elif conversations > 20 and frequency > 50:
            return 'high'
        elif conversations > 5 and frequency > 10:
            return 'moderate'
        else:
            return 'low'
