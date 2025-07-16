"""
Generic web content extractor using configurable selectors.
"""

import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Tag

from ..extractors.base import BaseExtractor, ExtractedContent
from .config import SelectorConfig
from onyx.utils.logger import setup_logger
from onyx.file_processing.html_utils import convert_html_to_markdown

logger = setup_logger()


class GenericWebExtractor(BaseExtractor):
    """Generic web content extractor that uses configurable selectors."""
    
    def __init__(self, config: SelectorConfig):
        """Initialize with selector configuration."""
        self.config = config
        self.name = config.name
        
        # Validate configuration
        validation_issues = config.validate()
        if validation_issues:
            logger.warning(f"Selector config validation issues: {validation_issues}")
    
    def can_extract(self, soup: BeautifulSoup, url: str) -> bool:
        """Check if this extractor can handle the given page."""
        # Check required detection selectors
        for selector in self.config.detection.required:
            if not soup.select_one(selector):
                return False
        
        # If we have optional selectors, at least one should match
        if self.config.detection.optional:
            optional_matches = any(soup.select_one(selector) for selector in self.config.detection.optional)
            if not optional_matches:
                return False
        
        logger.info(f"Generic extractor '{self.name}' can extract from {url}")
        return True
    
    def extract_content(self, soup: BeautifulSoup, url: str) -> ExtractedContent:
        """Extract content using configured selectors."""
        title = self._extract_title(soup)
        content = self._extract_main_content(soup)
        metadata = self._extract_metadata(soup, url)
        links = self._extract_links(soup, url)
        
        # Add extractor information to metadata
        metadata['extractor'] = f'generic_{self.name}'
        metadata['selector_config'] = self.config.name
        
        return ExtractedContent(
            title=title,
            content=content,
            metadata=metadata,
            links=links
        )
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page title using configured selectors."""
        # Try primary title selector
        if self.config.content.title:
            title_element = soup.select_one(self.config.content.title)
            if title_element:
                title = title_element.get_text(strip=True)
                if title and len(title) > 3:
                    return title
        
        # Try fallback selectors
        for selector in self.config.content.title_fallback:
            title_element = soup.select_one(selector)
            if title_element:
                title = title_element.get_text(strip=True)
                if title and len(title) > 3:
                    logger.debug(f"Used fallback title selector: {selector}")
                    return title
        
        logger.warning("No title found with configured selectors")
        return None
    
    def _extract_main_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract main content using configured selectors."""
        content_element = None
        
        # Collect all body selectors to try
        body_selectors = []
        if self.config.content.body:
            body_selectors.append(self.config.content.body)
        if self.config.content.text:
            body_selectors.append(self.config.content.text)
        body_selectors.extend(self.config.content.body_fallback)
        
        # Try each selector until one works (fallback approach)
        for selector in body_selectors:
            content_element = soup.select_one(selector)
            if content_element:
                logger.debug(f"Used body selector: {selector}")
                break
        
        if not content_element:
            logger.error("Could not find content container with configured selectors")
            return None
        
        # Clone the content element to avoid modifying original
        content_copy = BeautifulSoup(str(content_element), 'html.parser')
        
        # Always ensure script and style tags are removed to prevent JS/CSS leakage
        # This is critical for security and content cleanliness
        essential_removals = ['script', 'style', 'noscript']
        
        # Combine user-configured exclusions with essential removals
        all_exclusions = set(self.config.exclusion.remove + essential_removals)
        
        # Remove excluded elements
        for selector in all_exclusions:
            for elem in content_copy.select(selector):
                elem.decompose()
        
        # Check for skip conditions
        if self.config.exclusion.skip_if_contains:
            content_text = content_copy.get_text()
            for skip_text in self.config.exclusion.skip_if_contains:
                if skip_text.lower() in content_text.lower():
                    logger.info(f"Skipping content containing: {skip_text}")
                    return None
        
        # Try to extract as Markdown first
        content_html = str(content_copy)
        try:
            markdown_content = convert_html_to_markdown(
                content_html, 
                ui_elements_to_remove=list(all_exclusions)
            )
            
            if markdown_content and len(markdown_content) > 50:
                logger.info(f"Extracted {len(markdown_content)} chars of Markdown content")
                return self._clean_content(markdown_content)
        except Exception as e:
            logger.warning(f"Markdown conversion failed: {e}")
        
        # Fallback to text extraction
        text = content_copy.get_text(separator=' ', strip=True)
        
        if len(text) > 50:
            logger.info(f"Extracted {len(text)} chars of text content (fallback)")
            return self._clean_content(text)
        else:
            logger.warning(f"Content too short: {len(text)} chars")
            return None
    
    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract metadata using configured selectors."""
        metadata = {}
        
        # Extract configured metadata fields
        metadata_fields = {
            'date': self.config.metadata.date,
            'author': self.config.metadata.author,
            'category': self.config.metadata.category,
            'tags': self.config.metadata.tags
        }
        
        for field, selector in metadata_fields.items():
            if selector:
                element = soup.select_one(selector)
                if element:
                    # Handle different types of metadata extraction
                    if field == 'date':
                        # Try to get from datetime attribute first
                        date_value = element.get('datetime') or element.get('content') or element.get_text(strip=True)
                        if date_value:
                            metadata[field] = date_value
                    elif field == 'tags':
                        # Handle tags as list
                        tags_text = element.get('content') or element.get_text(strip=True)
                        if tags_text:
                            metadata[field] = [tag.strip() for tag in tags_text.split(',') if tag.strip()]
                    else:
                        # Standard text extraction
                        value = element.get('content') or element.get_text(strip=True)
                        if value:
                            metadata[field] = value
        
        # Try fallback selectors for missing metadata
        if 'date' not in metadata:
            for selector in self.config.metadata.date_fallback:
                element = soup.select_one(selector)
                if element:
                    date_value = element.get('datetime') or element.get('content') or element.get_text(strip=True)
                    if date_value:
                        metadata['date'] = date_value
                        break
        
        if 'author' not in metadata:
            for selector in self.config.metadata.author_fallback:
                element = soup.select_one(selector)
                if element:
                    author_value = element.get('content') or element.get_text(strip=True)
                    if author_value:
                        metadata['author'] = author_value
                        break
        
        # Extract from meta tags (standard web metadata)
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name', '') or meta.get('property', '')
            content = meta.get('content', '')
            
            if not name or not content:
                continue
            
            # Map common meta tags
            if name == 'description' and 'description' not in metadata:
                metadata['description'] = content
            elif name == 'keywords' and 'keywords' not in metadata:
                metadata['keywords'] = [k.strip() for k in content.split(',') if k.strip()]
            elif name.startswith('og:') or name.startswith('twitter:'):
                # Store social media metadata
                metadata[name] = content
        
        # Extract URL-based metadata
        parsed_url = urlparse(url)
        metadata['domain'] = parsed_url.netloc
        metadata['url_path'] = parsed_url.path
        
        return metadata
    
    def _extract_links(self, soup: BeautifulSoup, url: str) -> List[str]:
        """Extract links using configured selectors."""
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        found_links = set()
        
        # Always use a comprehensive approach to find all links on the page
        # Primary links selector (if configured)
        if self.config.navigation.links:
            links = soup.select(self.config.navigation.links)
            for link in links:
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    cleaned_url = self._clean_url(absolute_url)
                    # Only include valid links from same domain
                    if cleaned_url and cleaned_url.startswith(base_url):
                        found_links.add(cleaned_url)
        
        # Always check fallback selectors to ensure comprehensive link discovery
        for selector in self.config.navigation.links_fallback:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    absolute_url = urljoin(url, href)
                    cleaned_url = self._clean_url(absolute_url)
                    if cleaned_url and cleaned_url.startswith(base_url):
                        found_links.add(cleaned_url)
        
        logger.debug(f"Found {len(found_links)} links on page")
        return list(found_links)
    
    def _clean_url(self, url: str) -> Optional[str]:
        """Clean URLs by removing anchors/fragments."""
        if not url:
            return None
            
        # Remove fragments (anchors)
        if '#' in url:
            url = url.split('#')[0]
            
        # Skip if URL is now empty after removing fragment
        if not url:
            return None
            
        return url
    
    def _clean_content(self, content: str) -> str:
        """Clean extracted content."""
        if not content:
            return content
        
        # Normalize whitespace but preserve newlines for markdown
        content = re.sub(r'[ \t]+', ' ', content)  # Collapse spaces and tabs
        content = re.sub(r' +\n', '\n', content)   # Remove trailing spaces before newlines
        
        # Remove excessive line breaks
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        # Remove common UI elements that might slip through
        ui_patterns = [
            r'Skip to main content',
            r'Back to top',
            r'Print this page',
            r'Share this page',
            r'Was this (?:page|article) helpful\?',
            r'(?:Like|Thumbs up) (?:Dislike|Thumbs down)',
            r'Sign up for (?:our )?newsletter',
            r'Subscribe to (?:our )?updates'
        ]
        
        for pattern in ui_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        
        # Final cleanup
        content = re.sub(r' {2,}', ' ', content)  # Multiple spaces
        content = content.strip()
        
        return content
    
    def get_wait_selectors(self) -> List[str]:
        """Get selectors to wait for when loading content."""
        return self.config.wait.required + self.config.wait.optional
    
    def get_scroll_strategy(self) -> Dict[str, Any]:
        """Get scroll strategy configuration."""
        return {
            'enabled': len(self.config.wait.required) > 0,
            'max_attempts': 3,
            'wait_time': 1.0,
            'trigger_selectors': self.config.wait.required,
            'timeout': self.config.wait.timeout
        }
    
    def get_name(self) -> str:
        """Get the name of this extractor."""
        return f"generic_{self.name}"