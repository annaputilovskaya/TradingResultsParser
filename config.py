import logging

from dotenv import load_dotenv
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

load_dotenv()

HOST = "https://spimex.com"
RESULTS_URL = HOST + "/markets/oil_products/trades/results"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging with the specified level."""

    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S",
        format="[%(asctime)s.%(msecs)03d] %(levelname)-8s %(funcName)20s %(module)s:%(lineno)d - %(message)s",
        filename="log_async.log",
        filemode="w",
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


async_engine = create_async_engine(
    get_db_url(),
    isolation_level="REPEATABLE READ",
    pool_size=5,
    max_overflow=10,
)

session_factory = async_sessionmaker(
    bind=async_engine,
    expire_on_commit=False,
)
