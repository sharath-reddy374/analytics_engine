import logging
from typing import Optional, Iterator

from config.settings import settings

logger = logging.getLogger(__name__)

# Try to import SQLAlchemy only if available
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    SQLALCHEMY_AVAILABLE = True
except Exception:
    create_engine = None  # type: ignore
    sessionmaker = None  # type: ignore
    SQLALCHEMY_AVAILABLE = False


engine = None
SessionLocal: Optional[object] = None  # will be set to a callable/session factory when enabled


class _DisabledSessionFactory:
    def __call__(self, *args, **kwargs):
        raise RuntimeError(
            "SQL/ORM features are disabled. To enable, set DATABASE_URL in .env and install SQLAlchemy + a DB driver."
        )


def _init_engine_and_session():
    global engine, SessionLocal

    if not SQLALCHEMY_AVAILABLE:
        logger.info("SQLAlchemy not installed; disabling ORM features.")
        SessionLocal = _DisabledSessionFactory()
        engine = None
        return

    if not getattr(settings, "DATABASE_URL", None):
        logger.info("DATABASE_URL not set; disabling ORM features.")
        SessionLocal = _DisabledSessionFactory()
        engine = None
        return

    try:
        engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, echo=settings.DEBUG)  # type: ignore
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)  # type: ignore
        logger.info("SQLAlchemy engine initialized.")
    except Exception as e:
        logger.warning(f"Failed to initialize SQLAlchemy engine: {e}")
        SessionLocal = _DisabledSessionFactory()
        engine = None


_init_engine_and_session()


def get_db() -> Iterator[object]:
    """
    FastAPI dependency. Yields a DB session when ORM is enabled, otherwise raises a clear error.
    """
    if isinstance(SessionLocal, _DisabledSessionFactory) or SessionLocal is None:
        raise RuntimeError(
            "get_db called but ORM is disabled. Set DATABASE_URL and install SQLAlchemy dependencies."
        )

    db = SessionLocal()  # type: ignore
    try:
        yield db
    finally:
        try:
            db.close()
        except Exception:
            pass


def init_db():
    """
    Initialize database tables when ORM is enabled; otherwise log a no-op.
    """
    if engine is None or isinstance(SessionLocal, _DisabledSessionFactory):
        logger.info("init_db() skipped: ORM disabled (no DATABASE_URL or SQLAlchemy not installed).")
        return

    try:
        from database.models import Base  # type: ignore
    except Exception:
        logger.info("No SQLAlchemy Base found in database.models; skipping table creation.")
        return

    try:
        Base.metadata.create_all(bind=engine)  # type: ignore
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
