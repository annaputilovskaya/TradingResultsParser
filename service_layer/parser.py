import logging
from io import BytesIO

import numpy
import pandas as pd

import requests
from bs4 import BeautifulSoup as bs

from service_layer.utils import set_next_page, get_date_from_link

log = logging.getLogger(__name__)


def get_new_trading_results_links(earliest_date: str) -> set[str]:
    """
    Get unique links to XLS files with daily trading results
    from the SPIMEX website.
    """
    url = "https://spimex.com/markets/oil_products/trades/results"
    links = set()
    params = {"page": "page-1"}
    is_new = True
    log.info("Start getting links from the website...")
    while is_new:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            soup = bs(response.text, "html.parser")
            block = soup.find("div", class_="accordeon-inner")
            items = block.find_all("div", class_="accordeon-inner__header")
            for item in items:
                link = item.find("a").get("href")
                if link:
                    date = get_date_from_link(link)
                    if date >= earliest_date:
                        links.add(link)
                    else:
                        is_new = False
        else:
            log.error(f"Error: Failed to fetch data from {url}")
            break
        params["page"] = set_next_page(params["page"])
    log.info("Got links from the website.")
    return links


def get_data_from_file(filepath: str) -> pd.DataFrame | None:
    """
    Read data from an XLS file and filter it based on certain conditions.
    """
    try:
        response = requests.get(filepath)
        if response.status_code == 200:
            log.info(f"Started reading {filepath}")
            io = BytesIO(response.content)
            file = pd.read_excel(io=io, index_col=False)
            rows, cols = numpy.where(file == "Единица измерения: Метрическая тонна")
            row = rows[0] + 2

            filtered = pd.read_excel(
                io=io,
                skiprows=row,
                usecols=[1, 2, 3, 4, 5, 14],
            )
            return filtered[filtered["Количество\nДоговоров,\nшт."] != "-"]

    except Exception as e:
        log.error(f"Error reading file {filepath}: {e}")
