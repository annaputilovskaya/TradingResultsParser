from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy.orm import registry

from config import sync_engine
from domain.model import TradingResult

mapper_registry = registry()

spimex_trading_results = Table(
    "spimex_trading_results",
    mapper_registry.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("exchange_product_id", String(30)),
    Column("exchange_product_name", String(255)),
    Column("oil_id", String(30)),
    Column("delivery_basis_id", String(30)),
    Column("delivery_basis_name", String(255)),
    Column("delivery_type_id", String(30)),
    Column("volume", Integer),
    Column("total", Integer),
    Column("count", Integer),
    Column("date", String(8)),
    Column("created_on", DateTime),
    Column("updated_on", DateTime, nullable=True),
)


def start_mappers():
    """Create the mapping between the database table and the TradingResult class."""

    mapper_registry.metadata.create_all(sync_engine)
    mapper_registry.map_imperatively(TradingResult, spimex_trading_results)
