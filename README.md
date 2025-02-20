**TradingResultsParser**
---

**Задача:**

Написать парсер, который скачивает бюллетень по итогам торгов с сайта биржи 
(https://spimex.com/markets/oil_products/trades/results/).

Сохранить информацию по итогам торгов начиная с 2023 года в базу данных.

---

**Технические требования:**

- Язык программирования:
  - Python 3.11
- ORM:
  - SQLAlchemy 2.0 
- Хранение данных:
  - PostgreSQL
---
**Структура базы данных:**

Таблица «spimex_trading_results»:
- id
- exchange_product_id
- exchange_product_name
- oil_id - exchange_product_id[:4]
- delivery_basis_id - exchange_product_id[4:7]
- delivery_basis_name
- delivery_type_id - exchange_product_id[-1]
- volume
- total
- count
- date
- created_on
- updated_on
