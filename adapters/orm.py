from datetime import datetime

from sqlalchemy import String, DateTime, text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import async_engine


class Base(DeclarativeBase):
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)


class ORMTradingResult(Base):
    """Trading result model in database."""

    __tablename__ = "spimex_trading_results"

    exchange_product_id: Mapped[str] = mapped_column(String(30))
    exchange_product_name: Mapped[str] = mapped_column(String(255))
    oil_id: Mapped[str] = mapped_column(String(4))
    delivery_basis_id: Mapped[str] = mapped_column(String(3))
    delivery_basis_name: Mapped[str] = mapped_column(String(255))
    delivery_type_id: Mapped[str] = mapped_column(String(1))
    volume: Mapped[int]
    total: Mapped[int]
    count: Mapped[int]
    date: Mapped[str] = mapped_column(String(8), index=True)
    created_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("timezone('utc', now())"),
    )
    updated_on: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=text("timezone('utc', now())"),
        onupdate=func.now(),
    )


async def create_tables():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
