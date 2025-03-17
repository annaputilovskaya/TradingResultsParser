from datetime import datetime


class TradingResult:
    """Trading result."""

    def __init__(
        self,
        exchange_product_id: str,
        exchange_product_name: str,
        delivery_basis_name: str,
        volume: int,
        total: int,
        count: int,
        date: str,
    ):
        self.exchange_product_id = exchange_product_id
        self.exchange_product_name = exchange_product_name
        self.oil_id = exchange_product_id[:4]
        self.delivery_basis_id = exchange_product_id[4:7]
        self.delivery_basis_name = delivery_basis_name
        self.delivery_type_id = exchange_product_id[-1]
        self.volume = volume
        self.total = total
        self.count = count
        self.date = date
        self.created_on = datetime.now()
        self.updated_on = None

    def to_dict(self):
        return {
            "exchange_product_id": self.exchange_product_id,
            "exchange_product_name": self.exchange_product_name,
            "oil_id": self.oil_id,
            "delivery_basis_id": self.delivery_basis_id,
            "delivery_basis_name": self.delivery_basis_name,
            "delivery_type_id": self.delivery_type_id,
            "volume": self.volume,
            "total": self.total,
            "count": self.count,
            "date": self.date,
            "created_on": self.created_on,
            "updated_on": self.updated_on if self.updated_on else None,
        }
