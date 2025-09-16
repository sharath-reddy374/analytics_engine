# services/email_template_service.py
from typing import Dict, Any, Optional, List, Tuple
import logging
from datetime import datetime
import asyncio
import re
from services.llm_service import LLMService

logger = logging.getLogger(__name__)

class EmailTemplateService:
    """
    Service for generating dynamic educational email content using OpenAI.
    Maps template_id -> rule_id before calling the LLM, and includes
    purpose-aware fallbacks so time-sensitive emails (exams/appointments)
    always read correctly even if the LLM returns generic copy.
    """

    # Map your template IDs to the corresponding RULE IDs from email_rules.yaml
    TPL_TO_RULE: Dict[str, str] = {
        # --- Time-sensitive exam flows
        "exam_last_minute_prep_v1": "exam_last_minute_prep",
        "exam_post_checkin_v1": "exam_post_checkin",

        # --- Achievement / engagement
        "engagement_reward_v1": "high_engagement_reward",
        "course_completion_v1": "course_completion_celebration",

        # --- AI-assisted support
        "learning_support_v1": "learning_support_trigger",

        # --- Subject nudges
        "biology_help_v1": "help_biology_general",
        "pharmacology_help_v1": "help_pharmacology_general",
        "immunology_help_v1": "help_immunology_general",
        "history_help_v1": "help_history_general",

        # --- Performance / winback
        "test_improvement_v1": "test_performance_help",
        "winback_study_plan_v1": "winback_idle",
    }

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
            # Resolve rule_id from template_id for the LLM
            rule_id = self._template_to_rule(template_id)
            user_email = features.get('email', 'student@example.com')

            # Determine purpose from triggers/state for alignment + fallback copy
            purpose = self._determine_email_purpose(template_id, features, ai_triggers)
            email_context = self._build_email_context(features, ai_triggers, purpose)

            # Build purpose-specific subject/body (deterministic)
            fallback_subject, fallback_body = self._compose_subject_content(email_context)

            # Try LLM
            try:
                # Reuse or create an event loop safely
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                email_result = loop.run_until_complete(
                    self.llm_service.generate_educational_email(
                        rule_id,                          # pass RULE ID
                        features,                         # user_features
                        user_email,                       # user_email
                        preferred_subject=email_context.get('subject_area'),
                        day_hint=email_context.get('day_hint')
                    )
                )

                llm_subject = (email_result or {}).get('subject') or ''
                llm_body = (email_result or {}).get('content') or ''

                # If LLM output doesn't align with purpose/subject urgency, use fallback
                if not self._is_alignment_ok(purpose, llm_subject, llm_body, email_context):
                    logger.debug(
                        "LLM output misaligned with purpose '%s' — using fallback. "
                        "(rule_id=%s, template_id=%s, subject='%s')",
                        purpose, rule_id, template_id, llm_subject
                    )
                    subject, content = fallback_subject, fallback_body
                else:
                    subject, content = llm_subject, llm_body

            except Exception as e:
                logger.warning("OpenAI generation failed, using fallback: %s", str(e))
                subject, content = fallback_subject, fallback_body

            return {
                'subject': subject.strip(),
                'content': content.strip(),
                'template_id': template_id,
                'generated_at': datetime.now().isoformat(),
                'rule_id': rule_id,  # helpful for observability
            }

        except Exception as e:
            logger.exception("Failed to generate email content for %s: %s", template_id, str(e))
            # Hard fallback if something unexpected happens early
            return {
                'subject': "Keep going — you’ve got this! 🎓",
                'content': "Quick nudge: take a short review today and try 5 practice questions. Small steps compound fast.",
                'template_id': template_id,
                'generated_at': datetime.now().isoformat(),
                'rule_id': self._template_to_rule(template_id),
            }

    def get_available_templates(self) -> list:
        """Return empty list since we no longer use predefined templates."""
        return []

    def preview_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Return None since we generate content dynamically."""
        return None

    # ---- Context / Purpose helpers -----------------------------------------

    def _template_to_rule(self, template_id: str) -> str:
        """
        Normalize a template_id (e.g. 'exam_last_minute_prep_v1') to its rule_id
        (e.g. 'exam_last_minute_prep'). Falls back to stripping a trailing _vN.
        """
        if not template_id:
            return "learning_support_trigger"
        if template_id in self.TPL_TO_RULE:
            return self.TPL_TO_RULE[template_id]
        stripped = re.sub(r"_v\d+$", "", template_id)
        return stripped or "learning_support_trigger"

    def _build_email_context(
        self,
        features: Dict[str, Any],
        ai_triggers: Optional[List[Dict[str, Any]]],
        purpose: str
    ) -> Dict[str, Any]:
        """Collect everything we might want for composition."""
        triggers = ai_triggers or features.get('ai_email_triggers', []) or []
        topics = features.get('top_topics', [])
        primary_subjects = self._extract_subjects_from_topics_ordered(topics)

        # Prefer the most-urgent exam trigger to pick subject + day hint
        subject_area, day_hint, chosen_trigger = self._choose_primary_subject_area(
            purpose=purpose,
            triggers=triggers,
            topics=topics,
            conversation_insights=features.get('conversation_insights', {}) or {}
        )

        # If nothing urgent selected, fall back to topics or generic
        if not subject_area:
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
            'ai_triggers': triggers,
            'personalization': {
                'recency_days': features.get('recency_days', 0),
                'frequency': features.get('frequency_7d', 0),
                'improvement_trend': features.get('itp_improvement_trend', 0),
            },
            'subject_area': subject_area,
            'day_hint': day_hint,              # <- "tonight" | "today" | "tomorrow" | "soon"
            'chosen_trigger': chosen_trigger,  # <- the trigger we prioritized (if any)
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

        # Helpful context
        conv = ctx['learning_insights'].get('conversation_insights', {}) or {}
        upcoming = conv.get('upcoming_events', []) if isinstance(conv, dict) else []
        has_exam_tomorrow = any(e.get('type') == 'exam' and 'tomorrow' in str(e.get('timeframe', '')).lower() for e in upcoming)
        has_appt_tomorrow = any(e.get('type') == 'appointment' and 'tomorrow' in str(e.get('timeframe', '')).lower() for e in upcoming)

        # Day hint coming from chosen trigger (preferred)
        day_hint = ctx.get('day_hint')
        if not day_hint and purpose == 'exam_last_minute_prep':
            # Fallback to insights if we didn't get it from the trigger
            day_hint = "tomorrow" if has_exam_tomorrow else "soon"

        if purpose == 'exam_last_minute_prep':
            subject = f"{(day_hint or 'soon').title()}’s {subject_area} exam: 45-minute crash plan ✅"
            body = f"""{greet}

Your {subject_area} exam is {(day_hint or 'soon')}. Here’s a focused, high-yield plan:

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
            completed_titles = ctx['learning_insights'].get('completed_course_titles', []) or []
            course_name = completed_titles[0] if completed_titles else subject_area
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
    def _is_alignment_ok(self, purpose: str, subject: str, body: str, ctx: Dict[str, Any]) -> bool:
        """
        Purpose-vs-copy sanity check.
        Also enforces that, when we selected a specific subject/day from triggers
        (e.g., "Physics" + "tonight"), the LLM output mentions them (with synonyms allowed).
        """
        s = f"{subject} {body}".lower()

        def has_any(*words) -> bool:
            return any(w in s for w in words)

        # If we selected a subject/day from triggers, enforce it shows up
        chosen_subject = (ctx.get('subject_area') or '').lower().strip()
        day_hint = (ctx.get('day_hint') or '').lower().strip()

        # Day synonyms (kept minimal; you can expand any time)
        DAY_SYNONYMS = {
            'tonight': ['tonight', 'this evening', 'evening', 'later this evening'],
            'today':   ['today', 'this afternoon', 'this morning', 'later today', 'in a few hours'],
            'tomorrow':['tomorrow', 'tmrw'],
            'soon':    ['soon', 'coming up', 'upcoming'],
        }

        if chosen_subject and chosen_subject not in s:
            return False

        if day_hint:
            synonyms = DAY_SYNONYMS.get(day_hint, [day_hint])
            if not any(word in s for word in synonyms):
                return False

        if purpose == 'exam_last_minute_prep':
            return has_any('exam', 'test', 'quiz', 'assessment', 'prep', 'plan', 'crash', 'review')
        if purpose == 'exam_followup':
            return has_any('exam', 'test', 'quiz', 'assessment', 'how did', 'went', 'score', 'debrief')
        if purpose == 'appointment_reminder':
            return has_any('appointment', 'reminder', 'tomorrow', 'today', 'tonight')
        if purpose == 'appointment_followup':
            return has_any('appointment', 'session', 'follow-up', 'follow up', 'went')
        if purpose == 'completion_celebration':
            return has_any('congrats', 'congratulations', 'completed', 'finish', 'finished')
        # Others can be looser
        return True

    # ---- Utility helpers ----------------------------------------------------

    def _determine_learning_level(self, features: Dict[str, Any]) -> str:
        topics = features.get('top_topics', [])
        advanced_indicators = ['USMLE', 'Medicine', 'Pharmacology', 'Immunology', 'Pathology', 'IELTS']
        if any(ind in ' '.join(topics) for ind in advanced_indicators):
            return 'graduate_medical'
        intermediate_indicators = ['Biology', 'Chemistry', 'Physics', 'History', 'Algebra']
        if any(ind in ' '.join(topics) for ind in intermediate_indicators):
            return 'high_school_college'
        return 'middle_school'

    def _extract_subjects_from_topics_ordered(self, topics: list) -> list:
        """
        Preserve order from top_topics while de-duplicating subjects.
        Avoids the set() ordering bug that caused random subject selection.
        """
        seen = set()
        ordered = []
        for t in topics or []:
            subj = t.split('>')[0] if '>' in t else t
            if subj and subj not in seen:
                seen.add(subj)
                ordered.append(subj)
        return ordered

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

    # ---- Trigger prioritization --------------------------------------------
    def _choose_primary_subject_area(
        self,
        purpose: str,
        triggers: List[Dict[str, Any]],
        topics: List[str],
        conversation_insights: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
        """
        Choose subject + day hint using most-urgent relevant signal.

        For exam_last_minute_prep:
          - consider both AI triggers AND insights.upcoming_events
          - normalize time hint from 'days_before' and/or free-text timeframe
          - pick the most urgent: hours/0-day > today/tonight > tomorrow > later
          - prefer a subject that also appears first in top_topics when urgency ties

        Returns (subject_area, day_hint, chosen_trigger_or_event_dict)
        """
        if not isinstance(triggers, list):
            triggers = []
        insights = conversation_insights or {}
        upcoming = insights.get('upcoming_events', []) or []

        def extract_subject_from_topics(topics_list: List[str]) -> Optional[str]:
            if not topics_list:
                return None
            t0 = topics_list[0]
            return t0.split('>')[0] if '>' in t0 else t0

        def normalize_time_hint_from_str(s: str) -> Tuple[str, int, int]:
            """
            Map free-text timeframes to (hint, days, hours).
            hint in {'tonight','today','tomorrow','soon'}; smaller days/hours => more urgent.
            """
            txt = (s or '').lower()
            # explicit hours: "in 2 hours", "in 5h"
            m = re.search(r'in\s+(\d+)\s*(?:hours|hour|h)\b', txt)
            if m:
                hours = int(m.group(1))
                return ('today', 0, max(0, hours))

            if any(k in txt for k in ['tonight', 'this evening', 'evening']):
                return ('tonight', 0, 6)
            if any(k in txt for k in ['today', 'this afternoon', 'this morning', 'later today']):
                return ('today', 0, 12)
            if 'tomorrow' in txt or 'tmrw' in txt:
                return ('tomorrow', 1, 24)
            if 'next week' in txt:
                return ('soon', 7, 0)
            return ('soon', 3, 0)

        def normalize_time_hint_from_trigger(t: Dict[str, Any]) -> Tuple[str, int, int]:
            tf = str(t.get('timeframe', '') or '').lower()
            days_before = t.get('days_before')
            if days_before is not None:
                try:
                    d = int(days_before)
                except Exception:
                    d = 3
                if d <= 0:
                    # If 0 and we can infer evening, call it 'tonight'
                    return ('tonight', 0, 6) if 'night' in tf or 'evening' in tf else ('today', 0, 6)
                if d == 1:
                    return ('tomorrow', 1, 24)
                return ('soon', d, d * 24)
            # Fall back to parsing the timeframe string
            return normalize_time_hint_from_str(tf)

        def urgency_score(days: int, hours: int, hint: str, subject: Optional[str]) -> int:
            # Lower days/hours = higher urgency
            score = 10_000 - (days * 300 + hours * 5)
            if hint == 'tonight':
                score += 50
            elif hint == 'today':
                score += 40
            elif hint == 'tomorrow':
                score += 20
            # Tiny tie-breaker if this subject is also first in topics
            first_topic_subject = extract_subject_from_topics(topics)
            if subject and first_topic_subject and subject.lower() == first_topic_subject.lower():
                score += 3
            return score

        # Build a unified list of candidates (exam/test/quiz/assessment)
        candidates: List[Tuple[int, str, str, Dict[str, Any]]] = []

        def push_candidate(subject: Optional[str], hint: str, days: int, hours: int, src: Dict[str, Any]):
            subj = subject or extract_subject_from_topics(topics) or 'General'
            candidates.append((urgency_score(days, hours, hint, subj), subj, hint, src))

        if purpose == 'exam_last_minute_prep':
            # From explicit triggers
            for t in triggers:
                if not isinstance(t, dict):
                    continue
                t_type = (t.get('trigger') or t.get('trigger_type') or '').lower()
                stage = (t.get('message_type') or '').lower()
                if stage == 'last_minute_prep' or t_type in ('exam_prep', 'pre_exam'):
                    hint, d, h = normalize_time_hint_from_trigger(t)
                    push_candidate(t.get('subject'), hint, d, h, t)

            # From insights.upcoming_events (exam-like types)
            for ev in upcoming:
                if not isinstance(ev, dict):
                    continue
                ev_type = (ev.get('type') or '').lower()
                if ev_type in ('exam', 'test', 'quiz', 'assessment'):
                    hint, d, h = normalize_time_hint_from_str(ev.get('timeframe', ''))
                    push_candidate(ev.get('subject'), hint, d, h, ev)

            if candidates:
                candidates.sort(key=lambda x: x[0], reverse=True)
                _, subj, hint, chosen = candidates[0]
                return subj, hint, chosen

        # Fallback: use topics order
        ordered_subjects = self._extract_subjects_from_topics_ordered(topics)
        return (ordered_subjects[0] if ordered_subjects else None), None, None
