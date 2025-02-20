import re


def set_next_page(page: str) -> str:
    """
    Change the page to next page number.
    """
    page_number = str(int(page[5:]) + 1)
    return f"page-{page_number}"


def get_date_from_link(link: str) -> str:
    """
    Extract date from the link.
    """
    match = re.search(r"oil_xls_(\d{8})\d{6}", link)
    return match.group(1)
