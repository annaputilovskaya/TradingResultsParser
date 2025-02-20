def set_next_page(page: str) -> str:
    """
    Change the page to next page number.
    """
    page_number = str(int(page[5:]) + 1)
    return f"page-{page_number}"
