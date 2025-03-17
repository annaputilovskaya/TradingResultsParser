from datetime import datetime

from sqlalchemy import String, DateTime, text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import async_engine


class Base(DeclarativeBase):
    """Base abstract class for ORM models.

    Attributes:
        id (int): The unique identifier for the object.
    """
    __abstract__ = True

    id: Mapped[int] = mapped_column(primary_key=True)


class ORMTradingResult(Base):
    """Trading result model in database.

        Attributes:
            exchange_product_id (str): The exchange product ID.
            exchange_product_name (str): The exchange product name.
            oil_id (str): The oil ID.
            delivery_basis_id (str): The delivery basis ID.
            delivery_basis_name (str): The delivery basis name.
            delivery_type_id (str): The delivery type ID.
            volume (int): The volume of contracts in tones.
            total (int): The total value of contracts in rubles.
            count (int): The count of contracts.
            date (str): The date of trading.
            created_on (datetime): The date and time when the result was created.
            updated_on (datetime): The date and time when the result was updated.
    """

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
    """
    Creates tables for the ORM models in the database.
    """
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
