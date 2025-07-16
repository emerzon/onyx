"""
Factory for creating website-specific extractors.
"""

from typing import List, Optional
from bs4 import BeautifulSoup

from .base import BaseExtractor
from ..selectors.resolver import SelectorResolver
from onyx.utils.logger import setup_logger

logger = setup_logger()


class ExtractorFactory:
    """Factory for creating and managing website-specific extractors."""
    
    def __init__(self, selector_resolver: Optional[SelectorResolver] = None):
        self._extractors: List[BaseExtractor] = [
            # Add more extractors here as they are implemented
            # GitBookExtractor(),
            # ConfluenceExtractor(),
            # etc.
        ]
        self.selector_resolver = selector_resolver
    
    def get_extractor(self, soup: BeautifulSoup, url: str) -> Optional[BaseExtractor]:
        """
        Get the appropriate extractor for a given page.
        
        Priority order:
        1. Generic extractors (if selector_resolver is provided)
        2. Specialized extractors (zoomin, etc.)
        3. None if no match found
        
        Args:
            soup: BeautifulSoup object of the page
            url: URL of the page
            
        Returns:
            Appropriate extractor or None if no match found
        """
        # First, try generic extractors if available
        if self.selector_resolver:
            try:
                generic_extractor = self.selector_resolver.get_extractor(soup, url)
                if generic_extractor:
                    logger.info(f"Selected generic extractor: {generic_extractor.get_name()} for {url}")
                    return generic_extractor
            except Exception as e:
                logger.warning(f"Error checking generic extractors: {e}")
        
        # Then try specialized extractors
        for extractor in self._extractors:
            try:
                if extractor.can_extract(soup, url):
                    logger.info(f"Selected specialized extractor: {extractor.get_name()} for {url}")
                    return extractor
            except Exception as e:
                logger.warning(f"Error checking extractor {extractor.get_name()}: {e}")
                continue
        
        logger.info(f"No extractor found for {url}")
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