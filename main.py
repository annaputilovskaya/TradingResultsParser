import logging

from adapters.orm import start_mappers
from config import session_factory
from domain.model import TradingResult
from service_layer.parser import get_new_trading_results_links, get_data_from_file
from service_layer.utils import get_date_from_link

from service_layer.tr_generator import generate_trading_result_objects

HOST = "https://spimex.com"

log = logging.getLogger(__name__)


def main(earliest_date: str = "20230101"):
    """Main function to parse and save trading results."""
    links = get_new_trading_results_links(earliest_date)
    for link in links:
        with session_factory() as session:
            result = (
                session.query(TradingResult)
                .filter_by(date=get_date_from_link(link))
                .first()
            )
            if result:
                log.warning(f"Trading results for {link} already exist, skipping.")
                continue
            else:
                data = get_data_from_file(HOST + link)
                for trading_result in generate_trading_result_objects(data, link):
                    session.add(trading_result)
                try:
                    session.commit()
                    log.info(f"Saved trading results from {link}.")
                except Exception as e:
                    log.error(f"Error saving trading results: {e} from {link}.")
                    session.rollback()

    log.info("Finished parsing and saving trading results.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    start_mappers()
    main()
