from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import math

from config.settings import settings
from database.dynamodb_models import DataFetcher


def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    """
    Parse timestamps like "2025-01-14,21:45:33" or ISO. Return aware UTC dt.
    """
    if not ts_str:
        return None
    try:
        if "," in ts_str and "T" not in ts_str:
            dt = datetime.strptime(ts_str, "%Y-%m-%d,%H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        # ISO-ish
        if isinstance(ts_str, str):
            if ts_str.endswith("Z"):
                ts_str = ts_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    except Exception:
        return None
    return None


class ItpIcpAnalyzer:
    """
    Analyzer for:
      - ITP weak topics across User_Infinite_TestSeries_Prod + TestSereiesRecord_Prod
      - Stalled ICP courses (no progress beyond threshold)
      - Stalled ITP tests (incomplete and idle beyond threshold)
      - Build ICP request payloads based on weak topics

    Outputs a feature-dict that can be merged into FeatureEngine results.
    """

    def __init__(self):
        self.fetcher = DataFetcher()
        # Thresholds
        self.min_attempts = getattr(settings, "WEAK_TOPIC_MIN_ATTEMPTS", 5)
        self.weak_acc_max = float(getattr(settings, "WEAK_TOPIC_MAX_ACCURACY", 0.60))
        self.icp_stalled_days = int(getattr(settings, "ICP_STALLED_DAYS", 14))
        self.itp_stalled_days = int(getattr(settings, "ITP_STALLED_DAYS", 7))
        self.max_weak_topics = int(getattr(settings, "MAX_WEAK_TOPICS", 3))

    def analyze_user(self, email: str) -> Dict[str, Any]:
        """
        Returns dict to merge into features:
          - has_weak_topics: bool
          - weak_topics_detailed: List[{topic, subject, attempts, correct, accuracy, source}]
          - icp_recommendations: List[{subject, topic, description}]
          - stalled_icp_recent: bool
          - stalled_itp_recent: bool
          - resume_target: Optional[str] ('icp'|'itp')
          - resume_details: Optional[dict]
        """
        # Compute weak topics and ICP payloads
        weak_topics = self._compute_weak_topics(email)
        icp_payloads = self._build_icp_payloads_from_weak_topics(weak_topics)

        # Detect stalled ICP and ITP
        stalled_icp = self._detect_icp_stalled(email)
        stalled_itp = self._detect_itp_stalled(email)

        # Choose resume target: the one that is "most recently stalled" i.e.,
        # still beyond threshold but with the lowest days_since_last_activity.
        resume_target: Optional[str] = None
        resume_details: Optional[Dict[str, Any]] = None

        def _candidate_sort_key(info: Optional[Dict[str, Any]]) -> Tuple[int, int]:
            if not info or not info.get("is_stalled"):
                return (10_000, 10_000)
            # prefer lower days_since_last_activity (more recent)
            return (info.get("days_since_last_activity", 10_000), info.get("days_over_threshold", 10_000))

        c_icp = _candidate_sort_key(stalled_icp)
        c_itp = _candidate_sort_key(stalled_itp)

        if stalled_icp and stalled_icp.get("is_stalled") and stalled_itp and stalled_itp.get("is_stalled"):
            if c_icp <= c_itp:
                resume_target, resume_details = "icp", stalled_icp
            else:
                resume_target, resume_details = "itp", stalled_itp
        elif stalled_icp and stalled_icp.get("is_stalled"):
            resume_target, resume_details = "icp", stalled_icp
        elif stalled_itp and stalled_itp.get("is_stalled"):
            resume_target, resume_details = "itp", stalled_itp

        return {
            "has_weak_topics": len(weak_topics) > 0,
            "weak_topics_detailed": weak_topics[: self.max_weak_topics],
            "icp_recommendations": icp_payloads[: self.max_weak_topics],
            "stalled_icp_recent": bool(stalled_icp and stalled_icp.get("is_stalled")),
            "stalled_itp_recent": bool(stalled_itp and stalled_itp.get("is_stalled")),
            "resume_target": resume_target,
            "resume_details": resume_details,
        }

    # ---------------- Weak topics ----------------

    def _compute_weak_topics(self, email: str) -> List[Dict[str, Any]]:
        """
        Aggregate correctness by topic label from both ITP tables.
        Topic derivation priority:
          - Per-response Topic (if present)
          - record['topic'] or record['series_title'] or record['name']
          - record['Subject']
        """
        aggregates: Dict[str, Dict[str, Any]] = {}

        def bump(topic_label: str, subject: str, is_correct: bool, source: str):
            if not topic_label:
                topic_label = subject or "General"
            if topic_label not in aggregates:
                aggregates[topic_label] = {
                    "topic": topic_label,
                    "subject": subject or "General",
                    "attempts": 0,
                    "correct": 0,
                    "sources": set(),
                }
            aggregates[topic_label]["attempts"] += 1
            if is_correct:
                aggregates[topic_label]["correct"] += 1
            aggregates[topic_label]["sources"].add(source)

        # User_Infinite_TestSeries_Prod
        series = self.fetcher.test_series.get_user_test_series(email, limit=500) or []
        for rec in series:
            subject = rec.get("Subject") or rec.get("grade_subject") or ""
            base_topic = (
                rec.get("topic")
                or rec.get("series_title")
                or rec.get("name")
                or subject
                or "General"
            )

            resp = rec.get("Response") or {}
            if isinstance(resp, dict):
                for _ts, resp_list in resp.items():
                    if not isinstance(resp_list, list):
                        continue
                    for r in resp_list:
                        if not isinstance(r, dict):
                            continue
                        correct_resp = r.get("Correct_Response")
                        user_resp = r.get("Response")
                        is_correct = (correct_resp == user_resp) if correct_resp is not None else bool(r.get("check_response"))
                        topic_label = r.get("Topic") or base_topic
                        bump(str(topic_label), str(subject), bool(is_correct), "User_Infinite_TestSeries_Prod")

        # TestSereiesRecord_Prod
        records = self.fetcher.test_records.get_user_test_records(email, limit=500) or []
        for rec in records:
            subject = rec.get("Subject") or ""  # may not exist
            base_topic = rec.get("name") or subject or "General"
            resp = rec.get("Response") or {}
            if isinstance(resp, dict):
                for _ts, resp_list in resp.items():
                    if not isinstance(resp_list, list):
                        continue
                    for r in resp_list:
                        if not isinstance(r, dict):
                            continue
                        correct_resp = r.get("Correct_Response")
                        user_resp = r.get("Response")
                        is_correct = (correct_resp == user_resp) if correct_resp is not None else bool(r.get("check_response"))
                        topic_label = r.get("Topic") or base_topic
                        bump(str(topic_label), str(subject), bool(is_correct), "TestSereiesRecord_Prod")

        # Compute accuracies and filter weak ones
        weak: List[Dict[str, Any]] = []
        for topic, agg in aggregates.items():
            attempts = agg["attempts"]
            correct = agg["correct"]
            accuracy = (correct / attempts) if attempts > 0 else 0.0
            if attempts >= self.min_attempts and accuracy < self.weak_acc_max:
                weak.append({
                    "topic": agg["topic"],
                    "subject": agg["subject"] or "General",
                    "attempts": attempts,
                    "correct": correct,
                    "accuracy": round(accuracy, 4),
                    "source": ",".join(sorted(list(agg["sources"]))) if agg.get("sources") else "",
                })

        # Sort by lowest accuracy then highest attempts
        weak.sort(key=lambda x: (x["accuracy"], -x["attempts"]))
        return weak

    def _build_icp_payloads_from_weak_topics(self, weak_topics: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Build ICP API payloads with complete details embedded in description.
        Schema:
          {
            "subject": "...",
            "topic": "...",
            "description": "rich details for course generator"
          }
        """
        payloads: List[Dict[str, str]] = []
        for w in weak_topics:
            subject = (w.get("subject") or "General").strip() or "General"
            topic = (w.get("topic") or "Focused Practice").strip()
            acc_pct = int(round((w.get("accuracy", 0.0) or 0.0) * 100))
            attempts = int(w.get("attempts", 0))

            # Encode a compact, structured outline inside description
            description = (
                f"Goal: Improve mastery in '{topic}' within {subject}. "
                f"Current accuracy: ~{acc_pct}% over {attempts} attempts. "
                f"Design a course from basics to professional with:\n"
                f"- Brief diagnostic quiz on {topic}\n"
                f"- Concept explanations (common misconceptions + tips)\n"
                f"- 3-5 micro-lessons with examples\n"
                f"- 20-30 mixed practice questions (progressively harder)\n"
                f"- Spaced review schedule and quick-check quizzes\n"
                f"- Final assessment with detailed feedback\n"
                f"Emphasize correcting typical errors observed in this topic."
            )

            payloads.append({
                "subject": subject,
                "topic": topic,
                "description": description
            })
        return payloads

    # ---------------- Stalled detection ----------------

    def _detect_itp_stalled(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Consider a test stalled if:
          - current_Position < Total_Question (incomplete)
          - and days since Last_Trigger >= ITP_STALLED_DAYS
        Choose the most recently-stalled one (smallest days since last activity).
        """
        candidates: List[Dict[str, Any]] = []

        def consider(rec: Dict[str, Any], source: str):
            total_q = rec.get("Total_Question")
            cur_pos = rec.get("current_Position") or rec.get("current_Answer_Position")
            last_trig = _parse_ts(rec.get("Last_Trigger"))
            if total_q is None or cur_pos is None or not last_trig:
                return
            try:
                total_q = int(total_q)
                cur_pos = int(cur_pos)
            except Exception:
                return
            if total_q <= 0:
                return
            incomplete = cur_pos < total_q
            if not incomplete:
                return
            days_since = (datetime.now(timezone.utc) - last_trig).days
            if days_since >= self.itp_stalled_days:
                title = rec.get("series_title") or rec.get("name") or rec.get("topic") or rec.get("Subject") or "Test Series"
                subject = rec.get("Subject") or "General"
                candidates.append({
                    "title": title,
                    "subject": subject,
                    "source": source,
                    "days_since_last_activity": days_since,
                    "days_over_threshold": days_since - self.itp_stalled_days,
                    "last_activity": last_trig.isoformat(),
                    "current_position": cur_pos,
                    "total_questions": total_q,
                })

        # Both tables
        series = self.fetcher.test_series.get_user_test_series(email, limit=500) or []
        for rec in series:
            consider(rec, "User_Infinite_TestSeries_Prod")

        records = self.fetcher.test_records.get_user_test_records(email, limit=500) or []
        for rec in records:
            consider(rec, "TestSereiesRecord_Prod")

        if not candidates:
            return {"is_stalled": False}

        # Pick the one with smallest days_since_last_activity (more recent)
        candidates.sort(key=lambda c: (c["days_since_last_activity"], c["days_over_threshold"]))
        best = candidates[0]
        return {"is_stalled": True, **best}

    def _detect_icp_stalled(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Consider an ICP course stalled if:
          - not completed
          - completion_rate > 0
          - and days since last_activity >= ICP_STALLED_DAYS
        last_activity pulled from record['updated_at'] or 'created_at' if missing.
        """
        items = self.fetcher.icp.get_user_course_plans(email, limit=500) or []
        candidates: List[Dict[str, Any]] = []

        for rec in items:
            # Timestamps
            last_ts = _parse_ts(rec.get("updated_at")) or _parse_ts(rec.get("created_at"))
            if not last_ts:
                continue

            # Completion heuristics from stored status if any
            # If sections/lessons are present, try to derive completion_rate quickly
            completion_rate = 0.0
            is_completed = False

            status = rec.get("status") or {}
            # Common fields many schemas include
            try:
                total_sections = int(status.get("total_sections") or rec.get("total_sections") or 0)
                completed_sections = int(status.get("completed_sections") or rec.get("completed_sections") or 0)
                if total_sections > 0:
                    completion_rate = completed_sections / total_sections
                    is_completed = (completed_sections >= total_sections)
            except Exception:
                # fallback: if an explicit 'is_completed' exists
                is_completed = bool(status.get("is_completed") or rec.get("is_completed"))

            if is_completed:
                continue

            days_since = (datetime.now(timezone.utc) - last_ts).days
            if (completion_rate > 0.0) and (days_since >= self.icp_stalled_days):
                title = rec.get("title") or rec.get("course_title") or "Course"
                subject = rec.get("subject") or rec.get("course_subject") or "General"
                candidates.append({
                    "title": title,
                    "subject": subject,
                    "days_since_last_activity": days_since,
                    "days_over_threshold": days_since - self.icp_stalled_days,
                    "last_activity": last_ts.isoformat(),
                    "completion_rate": completion_rate,
                })

        if not candidates:
            return {"is_stalled": False}

        candidates.sort(key=lambda c: (c["days_since_last_activity"], c["days_over_threshold"]))
        best = candidates[0]
        return {"is_stalled": True, **best}
