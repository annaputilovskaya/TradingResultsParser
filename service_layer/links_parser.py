import asyncio
import logging
import re
from time import time

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from config import RESULTS_URL

log = logging.getLogger(__name__)


def get_date_from_link(link: str) -> str:
    """
    Extracts date from the link.

    Args:
        link (str): The URL of the page containing the trading results.

    Returns:
        str: The date extracted from the link in the format "YYYYMMDD".
    """
    match = re.search(r"oil_xls_(\d{8})\d{6}", link)
    return match.group(1)


async def get_html(url: str, session: ClientSession) -> str | None:
    """
    Sends a GET request to the given URL and returns the response data.

    Args:
        url (str): The URL to fetch data from.
        session (ClientSession): The aiohttp ClientSession object.

    Returns:
        str | None: The response data as a string or None if an error occurred.
    """
    try:
        async with session.get(url) as response:
            data = await response.text()
            return data
    except Exception as e:
        log.error(f"Error: Failed to fetch data from {url}: {e}")
        raise e


def extract_links_from_response(
    response: str, earliest_date: str, links: set[str], is_new: bool
) -> tuple[set[str], bool]:
    """
    Extracts links to XLS files with daily trading results from the given HTML response.

    Args:
        response (str): The HTML response containing the trading results.
        earliest_date (str): The earliest date to consider in the format "YYYYMMDD".
        links (set[str]): The set of links to add new ones to.
        is_new (bool): A boolean indicating if new links were found.

    Returns:
        tuple[set[str], bool]: The updated set of links and a boolean indicating if new links were found.
    """
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
    return links, is_new


async def get_new_trading_results_links(
    session: ClientSession, earliest_date: str = "20230101"
) -> set[str]:
    """
    Get unique links to XLS files with daily trading results from the website.

    Args:
        session (ClientSession): The aiohttp ClientSession object.
        earliest_date (str, optional): The earliest date to consider in the format "YYYYMMDD".

    Returns:
        set[str]: The unique links to XLS files with daily trading results.
    """
    t0 = time()
    page_num = 1
    links = set()
    log.info("Start getting links from the website...")
    is_new = True
    while is_new:
        url = f"{RESULTS_URL}?page=page-{page_num}"
        response = await get_html(url=url, session=session)
        links, is_new = await asyncio.to_thread(
            extract_links_from_response, response, earliest_date, links, is_new
        )
        page_num += 1
    log.warning(
        f"Got links from the website. Execution time {time() - t0:.2f} seconds."
    )
    return links
