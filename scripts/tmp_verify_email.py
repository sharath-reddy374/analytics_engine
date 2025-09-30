#!/usr/bin/env python3
import os
from services.email_template_service import EmailTemplateService
from config.settings import settings

# Disable OpenAI usage to force deterministic fallback during verification
try:
    settings.OPENAI_API_KEY = None
except Exception:
    pass

svc = EmailTemplateService()

cases = [
    (
        "exam_post_checkin_v1",
        {
            "email": "sharath_b2c@yopmail.com",
            "top_topics": ["Biology>Cells"],
            "ai_email_triggers": [
                {"trigger": "post_exam", "message_type": "how_did_it_go", "subject": "Biology"}
            ],
            "conversations_7d": 2,
            "tests_7d": 1,
            "minutes_7d": 15,
            "active_courses": 1,
            "recency_days": 3,
        },
    ),
    (
        "exam_last_minute_prep_v1",
        {
            "email": "sharath_b2c@yopmail.com",
            "top_topics": ["USMLE>Pharmacology"],
            "ai_email_triggers": [
                {"trigger": "exam_prep", "message_type": "last_minute_prep", "subject": "USMLE", "timeframe": "tomorrow"}
            ],
            "conversations_7d": 2,
            "tests_7d": 1,
            "minutes_7d": 15,
            "active_courses": 1,
            "recency_days": 0,
        },
    ),
]

os.makedirs("logs", exist_ok=True)
out_path = "logs/tmp_email_verify.txt"

with open(out_path, "w") as f:
    for template_id, features in cases:
        triggers = features.get("ai_email_triggers", [])
        res = svc.generate_email_content(template_id, features, triggers) or {}
        f.write(f"=== {template_id} ===\n")
        f.write("Subject: " + (res.get("subject") or "") + "\n")
        f.write("Content:\n" + (res.get("content") or "") + "\n\n")

print(out_path)
