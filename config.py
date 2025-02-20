import logging

from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging with the specified level."""

    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(funcName)20s %(module)s:%(lineno)d %(levelname)-8s - %(message)s",
    )


def get_db_url(
    db_engine: str = os.environ.get("DB_ENGINE"),
    db_name: str = os.environ.get("DB_NAME"),
    db_host: str = os.environ.get("DB_HOST"),
    db_port: str = os.environ.get("DB_PORT"),
    db_user: str = os.environ.get("DB_USER"),
    db_pass: str = os.environ.get("DB_PASS"),
) -> str:
    """Return a database URL based on environment variables."""

    return f"{db_engine}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


sync_engine = create_engine(
    get_db_url(),
    isolation_level="REPEATABLE READ",
)

session_factory = sessionmaker(bind=sync_engine)
