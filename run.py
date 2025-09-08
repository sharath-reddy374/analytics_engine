#!/usr/bin/env python3
"""
Main entry point for the EdYou AI Engine
"""

import uvicorn
from config.settings import settings

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )
