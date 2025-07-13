"""
Base extractor interface for website-specific content extraction.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup


@dataclass
class ExtractedContent:
    """Represents extracted content from a web page."""
    title: Optional[str] = None
    content: Optional[str] = None
    metadata: Dict[str, Any] = None
    links: List[str] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
        if self.links is None:
            self.links = []


class BaseExtractor(ABC):
    """Base class for website-specific content extractors."""
    
    @abstractmethod
    def can_extract(self, soup: BeautifulSoup, url: str) -> bool:
        """
        Determine if this extractor can handle the given page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: URL of the page
            
        Returns:
            True if this extractor can handle the page
        """
        pass
    
    @abstractmethod
    def extract_content(self, soup: BeautifulSoup, url: str) -> ExtractedContent:
        """
        Extract content from the page.
        
        Args:
            soup: BeautifulSoup object of the page
            url: URL of the page
            
        Returns:
            ExtractedContent object with extracted data
        """
        pass
    
    @abstractmethod
    def get_wait_selectors(self) -> List[str]:
        """
        Get CSS selectors to wait for when loading the page.
        
        Returns:
            List of CSS selectors
        """
        pass
    
    @abstractmethod
    def get_scroll_strategy(self) -> Dict[str, Any]:
        """
        Get scrolling configuration for this site type.
        
        Returns:
            Dictionary with scroll configuration
        """
        pass
    
    def get_name(self) -> str:
        """Get the name of this extractor."""
        return self.__class__.__name__.replace('Extractor', '').lower()