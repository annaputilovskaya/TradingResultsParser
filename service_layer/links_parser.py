import logging
import re
from time import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from config import RESULTS_URL

log = logging.getLogger(__name__)


def get_date_from_link(link: str) -> str:
    """
    Extract date from the link.
    """
    match = re.search(r"oil_xls_(\d{8})\d{6}", link)
    return match.group(1)


async def get_html(url: str, session: ClientSession) -> str | None:
    """
    Send a GET request to the given URL and return the response data.
    """
    try:
        async with session.get(url) as response:
            data = await response.text()
            return data
    except Exception as e:
        log.error(f"Error: Failed to fetch data from {url}: {e}")
        raise e


async def get_new_trading_results_links(
    session: ClientSession, earliest_date: str = "20230101"
) -> set[str]:
    """
    Get unique links to XLS files with daily trading results
    from the SPIMEX website.
    """
    t0 = time()
    page_num = 1
    links = set()
    log.info("Start getting links from the website...")
    is_new = True
    while is_new:
        url = f"{RESULTS_URL}?page=page-{page_num}"
        response = await get_html(url=url, session=session)
        soup = BeautifulSoup(response, "html.parser")
        block = soup.find("div", class_="accordeon-inner")
        items = block.select(
            "div.accordeon-inner__wrap-item",
        )
        for item in items:
            string = item.find("a")
            link = string.get("href")
            if link:
                date = get_date_from_link(link)
                if date >= earliest_date:
                    links.add(link)
                else:
                    is_new = False
                    break
        page_num += 1
    log.warning(
        f"Got links from the website. Execution time {time() - t0:.2f} seconds."
    )
    return links
