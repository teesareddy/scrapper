import logging
from typing import Dict, Any, Optional

from .universal_database_handler import UniversalDatabaseHandler
from .data_schemas import ScrapedData

logger = logging.getLogger(__name__)


class DatabaseHandlerFactory:
    _handlers = {}

    @classmethod
    def get_handler(cls, source_website: str, prefix: str) -> UniversalDatabaseHandler:
        handler_key = f"{source_website}_{prefix}"
        if handler_key not in cls._handlers:
            cls._handlers[handler_key] = UniversalDatabaseHandler(source_website, prefix)

        return cls._handlers[handler_key]

    @classmethod
    def save_scraped_data(cls, source_website: str, prefix: str, scraped_data: ScrapedData,
                         scrape_job_id: Optional[str] = None) -> Optional[str]:
        handler = cls.get_handler(source_website, prefix)
        return handler.save_scraped_data(scraped_data, scrape_job_id)




def save_scraper_data(source_website: str, prefix: str, data: Dict[str, Any], scrape_job_id: Optional[str] = None) -> Optional[str]:
    try:
        if isinstance(data, ScrapedData):
            return DatabaseHandlerFactory.save_scraped_data(source_website, prefix, data, scrape_job_id)
        else:
            raise Exception("Unsupported data type")
    except Exception as e:
        logger.error(f"Failed to save scraper data for {source_website}: {e}")
        return None