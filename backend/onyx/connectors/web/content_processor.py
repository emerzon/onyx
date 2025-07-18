"""
Content processing module for web connector.
Handles content extraction, normalization, and deduplication.
"""
import hashlib
import re
from typing import Optional, Tuple, Dict, Any, Protocol
import asyncio

from bs4 import BeautifulSoup

from onyx.file_processing.html_utils import web_html_cleanup, convert_html_to_markdown
from onyx.utils.logger import setup_logger


logger = setup_logger()


class ContentExtractor(Protocol):
    """Protocol for content extractors."""
    def extract_content(self, soup: BeautifulSoup, url: str) -> Any:
        """Extract content from BeautifulSoup object."""
        ...


class ExtractedContent:
    """Data class for extracted content."""
    def __init__(
        self,
        title: Optional[str],
        content: str,
        metadata: Dict[str, Any],
        links: Optional[list[str]] = None
    ):
        self.title = title
        self.content = content
        self.metadata = metadata or {}
        self.links = links or []


class ContentProcessor:
    """
    Handles content extraction, normalization, and deduplication.
    Extracted from AsyncWebConnectorCrawlee for better modularity.
    """
    
    def __init__(
        self,
        mintlify_cleanup: bool = True,
        selector_resolver: Optional[Any] = None,
        extractor_factory: Optional[Any] = None
    ):
        self.mintlify_cleanup = mintlify_cleanup
        self.selector_resolver = selector_resolver
        self.extractor_factory = extractor_factory
        
        # Content deduplication tracking
        self._content_hashes: set[str] = set()
        self._content_hashes_lock = asyncio.Lock()
    
    def extract_content(
        self, 
        url: str, 
        soup: BeautifulSoup, 
        raw_html: str
    ) -> Tuple[Optional[str], str, Dict[str, Any]]:
        """
        Extract content using specialized extractors or fallback methods.
        
        Args:
            url: The URL being processed
            soup: BeautifulSoup parsed HTML
            raw_html: Raw HTML string
            
        Returns:
            Tuple of (title, content, metadata)
        """
        # Try generic extractor first (if configured)
        if self.selector_resolver:
            generic_extractor = self.selector_resolver.get_extractor(soup, url)
            if generic_extractor:
                extracted = generic_extractor.extract_content(soup, url)
                # Ignore links - let Crawlee handle link discovery automatically
                return extracted.title, extracted.content, extracted.metadata or {}
        
        # Try specialized extractor second
        if self.extractor_factory:
            extractor = self.extractor_factory.get_extractor(soup, url)
            if extractor:
                extracted = extractor.extract_content(soup, url)
                # Ignore links - let Crawlee handle link discovery automatically
                return extracted.title, extracted.content, extracted.metadata or {}
        
        # Fallback to standard extraction
        title = soup.title.string if soup.title else None
        text_content = convert_html_to_markdown(raw_html)
        
        # Final fallback to traditional cleanup if needed
        if not text_content or len(text_content) < 50:
            parsed_html = web_html_cleanup(soup, self.mintlify_cleanup)
            title = parsed_html.title
            text_content = parsed_html.cleaned_text
        
        return title, text_content, {}
    
    async def is_content_duplicate(
        self, 
        title: Optional[str], 
        text_content: str, 
        url: str
    ) -> Tuple[bool, str]:
        """
        Check if content is a duplicate using sophisticated fingerprinting.
        
        Args:
            title: Page title
            text_content: Main content text
            url: Source URL
            
        Returns:
            Tuple of (is_duplicate, content_hash)
        """
        # Compute sophisticated content fingerprint
        content_hash = await self.compute_content_fingerprint(title, text_content, url)
        
        # Check for duplicate
        is_duplicate = not await self._add_content_hash(content_hash)
        
        return is_duplicate, content_hash
    
    async def compute_content_fingerprint(
        self,
        title: Optional[str],
        text_content: str,
        url: str
    ) -> str:
        """
        Compute a robust content fingerprint for deduplication.
        
        This creates a more sophisticated hash that considers:
        - Normalized text content (whitespace, case)
        - Content length and structure
        - Title similarity
        
        Args:
            title: Page title
            text_content: Main text content
            url: Source URL
            
        Returns:
            Content fingerprint hash
        """
        # Normalize text content for better deduplication
        normalized_content = self.normalize_content_for_deduplication(text_content)
        
        # Create fingerprint components
        content_length = len(normalized_content)
        
        # Use first and last 500 chars for structural similarity
        content_start = normalized_content[:500]
        content_end = normalized_content[-500:] if len(normalized_content) > 500 else ""
        
        # Normalize title
        normalized_title = (title or "").strip().lower()
        
        # Create composite fingerprint
        fingerprint_data = (
            f"{normalized_title}:{content_length}:{content_start}:{content_end}"
        )
        
        # Compute hash asynchronously in a way that doesn't block
        hash_result = await asyncio.to_thread(
            lambda: hashlib.sha256(fingerprint_data.encode("utf-8")).hexdigest()[:16]
        )
        
        return hash_result
    
    def normalize_content_for_deduplication(self, content: str) -> str:
        """
        Normalize content for better deduplication detection.
        
        This helps identify duplicate content even when there are minor
        formatting differences.
        """
        if not content:
            return ""
        
        # Convert to lowercase and normalize whitespace
        normalized = " ".join(content.lower().split())
        
        # Remove common variations that don't affect content meaning
        # Remove extra punctuation and normalize quotes
        normalized = re.sub(r'[""''‚„‹›«»]', '"', normalized)
        normalized = re.sub(r'[–—−]', '-', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove trailing/leading whitespace
        return normalized.strip()
    
    async def _add_content_hash(self, content_hash: str) -> bool:
        """
        Add content hash in thread-safe manner.
        
        Returns:
            True if hash was added (not duplicate), False if duplicate
        """
        async with self._content_hashes_lock:
            if content_hash in self._content_hashes:
                return False
            self._content_hashes.add(content_hash)
            return True
    
    def get_duplicate_count(self) -> int:
        """Get count of duplicate content found."""
        return len(self._content_hashes)