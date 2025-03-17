import asyncio
import logging
from time import time

from aiohttp import ClientSession

from adapters.orm import create_tables
from config import configure_logging
from service_layer.data_parser import parse_trading_results
from service_layer.links_parser import get_new_trading_results_links

log = logging.getLogger(__name__)


async def main(earliest_date: str = "20230101"):
    """
    Main function to parse and save trading results.

    Args:
        earliest_date (str): The earliest date for which to retrieve trading results. Defaults to "20230101".
    """
    t0 = time()
    await create_tables()
    async with ClientSession() as session:
        links = await get_new_trading_results_links(session, earliest_date)
    await parse_trading_results(links)
    log.warning(f"Finished. Execution time {time() - t0:.2f} seconds.")


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
