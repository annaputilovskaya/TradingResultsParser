import asyncio
import logging
from io import BytesIO
from time import time

import numpy
import pandas as pd
from aiohttp import ClientSession

from config import session_factory, HOST
from service_layer.results_generator import generate_trading_result_objects

log = logging.getLogger(__name__)


async def get_bytes(url: str, session: ClientSession) -> bytes | None:
    """
    Send a GET request to the given URL and return the response bytes.
    """
    try:
        async with session.get(url) as response:
            data = await response.read()
            return data
    except Exception as e:
        log.error(f"Error: Failed to fetch data from {url}: {e}")
        raise e


def extract_data_from_file(data: bytes) -> pd.DataFrame | None:
    """
    Read data from an XLS file and filter it based on certain conditions.
    """
    file = pd.read_excel(BytesIO(data), index_col=False)
    rows, cols = numpy.where(file == "Единица измерения: Метрическая тонна")
    row = rows[0] + 2

    filtered = pd.read_excel(
        io=data,
        skiprows=row,
        usecols=[1, 2, 3, 4, 5, 14],
    )
    return filtered[filtered["Количество\nДоговоров,\nшт."] != "-"]


async def get_data_by_link(link: str) -> pd.DataFrame | None:
    """
    Send a GET request to the given URL, extract data from the XLS file, and return the DataFrame and the URL.
    """
    t0 = time()
    async with ClientSession() as session:
        filepath = HOST + link
        log.info(f"Started reading {filepath}")
        data = await get_bytes(filepath, session)
        try:
            df = extract_data_from_file(data=data)
        except Exception as e:
            log.error(f"Error extracting data from {filepath}: {e}")
        else:
            log.info(f"Finished reading. Execution time {time() - t0:.2f} seconds.")
            return df, link


async def write_to_db(results_list: list) -> None:
    """
    Save the given trading results to the database.
    """
    t0 = time()
    log.info(f"Start writing results to db.")
    async with session_factory() as session:
        session.add_all(results_list)
        try:
            log.info(
                f"Finished writing results to db. Execution time {time() - t0:.2f} seconds."
            )
            await session.commit()
        except Exception as e:
            log.error(f"Error saving trading results: {e} to db.")
            await session.rollback()


async def parse_trading_results(links: set) -> None:
    """
    Parse trading results from the given links and save them to the database.
    """
    t0 = time()
    tasks = []
    for link in links:
        task = asyncio.create_task(get_data_by_link(link))
        tasks.append(task)
    df_list = list(await asyncio.gather(*tasks))
    results_list = []
    for df in df_list:
        results_list.extend(list(generate_trading_result_objects(df[0], df[1])))
    await write_to_db(results_list)
    log.warning(
        f"Parsed and saved trading results. Execution time {time() - t0:.2f} seconds."
    )
