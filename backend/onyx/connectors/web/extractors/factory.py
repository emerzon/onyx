"""
Factory for creating website-specific extractors.
"""

from typing import List, Optional
from bs4 import BeautifulSoup

from .base import BaseExtractor
from .zoomin import ZoominExtractor
from onyx.utils.logger import setup_logger

logger = setup_logger()


class ExtractorFactory:
    """Factory for creating and managing website-specific extractors."""
    
    def __init__(self):
        self._extractors: List[BaseExtractor] = [
            ZoominExtractor(),
            # Add more extractors here as they are implemented
            # GitBookExtractor(),
            # ConfluenceExtractor(),
            # etc.
        ]
    
    def get_extractor(self, soup: BeautifulSoup, url: str) -> Optional[BaseExtractor]:
        """
        Get the appropriate extractor for a given page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: URL of the page
            
        Returns:
            Appropriate extractor or None if no match found
        """
        for extractor in self._extractors:
            try:
                if extractor.can_extract(soup, url):
                    logger.info(f"Selected {extractor.get_name()} extractor for {url}")
                    return extractor
            except Exception as e:
                logger.warning(f"Error checking extractor {extractor.get_name()}: {e}")
                continue
        
        logger.info(f"No specialized extractor found for {url}")
        return None
    
    def register_extractor(self, extractor: BaseExtractor) -> None:
        """
        Register a new extractor.
        
        Args:
            extractor: Extractor instance to register
        """
        self._extractors.append(extractor)
        logger.info(f"Registered extractor: {extractor.get_name()}")
    
    def list_extractors(self) -> List[str]:
        """Get list of available extractor names."""
        return [extractor.get_name() for extractor in self._extractors]