"""
Zoomin documentation framework extractor.
"""

import re
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from .base import BaseExtractor, ExtractedContent
from onyx.utils.logger import setup_logger
from onyx.file_processing.html_utils import convert_html_to_markdown

logger = setup_logger()


class ZoominExtractor(BaseExtractor):
    """Extractor for Zoomin-based documentation sites."""
    
    # Zoomin detection patterns - simplified for modern Zoomin
    DETECTION_PATTERNS = [
        # Primary detection: Zoomin-specific CSS classes
        '#zDocsContent',
        '.zDocsTopicPageBody',
        '.zDocsContainer',
        # Secondary detection: Zoomin resources
        'script[src*="zoominsoftware"]',
        'link[href*="zoominsoftware"]',
    ]
    
    # Specific Zoomin selectors based on structure
    TITLE_SELECTOR = '#zDocsContent > header > h1'
    CONTENT_SELECTOR = '.zDocsTopicPageBodyContent'
    
    # Fallback selectors if primary fails
    FALLBACK_SELECTORS = [
        '.zDocsTopicPageBody',
        '#zDocsContent > div.zDocsTopicPageBody',
        'main#content',
        'article.bodytext'
    ]
    
    # Elements to remove from content
    REMOVAL_SELECTORS = [
        'nav', 'header', 'footer',
        '.navigation', '.sidebar', '.breadcrumb',
        '.zDocsNavigation', '.zDocsSidebar',
        '.feedback', '.page-actions',
        'script', 'style'
    ]
    
    def can_extract(self, soup: BeautifulSoup, url: str) -> bool:
        """Check if this is a Zoomin site."""
        # Quick check for Zoomin patterns
        for pattern in self.DETECTION_PATTERNS:
            if soup.select_one(pattern):
                logger.info(f"Detected Zoomin site using: {pattern}")
                return True
        
        # URL pattern check (Zoomin typically uses /bundle/*/page/*.htm)
        if '/bundle/' in url and '/page/' in url and url.endswith('.htm'):
            logger.info("Detected Zoomin site from URL pattern")
            return True
        
        return False
    
    def extract_content(self, soup: BeautifulSoup, url: str) -> ExtractedContent:
        """Extract content from Zoomin page."""
        title = self._extract_title(soup)
        content = self._extract_main_content(soup)
        metadata = self._extract_metadata(soup, url)
        links = self._extract_links(soup, url)
        
        # Add extractor information to metadata
        metadata['extractor'] = 'zoomin'
        metadata['extractor_version'] = '1.0'
        
        return ExtractedContent(
            title=title,
            content=content,
            metadata=metadata,
            links=links
        )
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title."""
        # Try the specific Zoomin title selector first
        title_element = soup.select_one(self.TITLE_SELECTOR)
        if title_element:
            title = title_element.get_text(strip=True)
            if title and len(title) > 3:
                return title
        
        # Fallback to other selectors
        fallback_selectors = [
            'h1.zDocsTitle',
            '.zDocsContent h1',
            '.content-title',
            'h1',
            'title'
        ]
        
        for selector in fallback_selectors:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 3:
                    return title
        
        return None
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extract main content from Zoomin page."""
        # Try primary selector first
        content_element = soup.select_one(self.CONTENT_SELECTOR)
        
        # If not found, try fallback selectors
        if not content_element:
            for selector in self.FALLBACK_SELECTORS:
                content_element = soup.select_one(selector)
                if content_element:
                    logger.info(f"Using fallback selector: {selector}")
                    break
        
        if not content_element:
            logger.error("Could not find Zoomin content container")
            return None
        
        # Try to extract as Markdown first
        content_html = str(content_element)
        # Pass Zoomin-specific selectors to remove
        zoomin_ui_selectors = [
            '.dataTables_filter', '.dataTables_info', '.dataTables_paginate',
            '.zDocsFilterTableDiv', '.zDocsTopicPageTableExportButton',
            '.dropdown-menu', '.searchTableDiv', 'input[type="search"]',
            'input[type="text"]', 'button', 'svg'
        ]
        markdown_content = convert_html_to_markdown(content_html, ui_elements_to_remove=zoomin_ui_selectors)
        
        if markdown_content and len(markdown_content) > 50:
            logger.info(f"Extracted {len(markdown_content)} chars of Markdown content")
            return self._clean_content(markdown_content)
        
        # Fallback to text extraction if Markdown fails
        content_copy = BeautifulSoup(content_html, 'html.parser')
        
        # Remove unwanted elements
        for selector in self.REMOVAL_SELECTORS:
            for elem in content_copy.select(selector):
                elem.decompose()
        
        # Extract text with basic formatting
        text = content_copy.get_text(separator=' ', strip=True)
        
        if len(text) > 50:
            logger.info(f"Extracted {len(text)} chars of text content (fallback)")
            return self._clean_content(text)
        else:
            logger.warning(f"Content too short: {len(text)} chars")
            return None
    
    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Extract metadata from Zoomin page."""
        metadata = {}
        
        # Extract from meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            name = meta.get('name', '') or meta.get('property', '')
            content = meta.get('content', '')
            
            if not name or not content:
                continue
            
            # Map meta tags to metadata fields
            if name == 'guidename':
                metadata['guide_name'] = content
            elif name == 'lastMod':
                metadata['last_modified'] = content
            elif name == 'Keywords':
                metadata['keywords'] = [k.strip() for k in content.split(',') if k.strip()]
            elif name == 'description':
                metadata['description'] = content
            elif name.startswith('zoomin:'):
                zoomin_key = name.replace('zoomin:', '')
                if content == 'Public':
                    metadata['classification'] = content
                elif content.startswith('version-'):
                    metadata['version'] = content.replace('version-', 'v')
        
        # Extract bundle from URL
        if '/bundle/' in url:
            try:
                bundle_part = url.split('/bundle/')[1].split('/')[0]
                metadata['bundle'] = bundle_part
                
                # Extract version from bundle if present
                version_match = re.search(r'v(\\d+\\.\\d+(?:\\.\\d+)?)', bundle_part)
                if version_match and 'version' not in metadata:
                    metadata['version'] = f"v{version_match.group(1)}"
            except (IndexError, AttributeError):
                pass
        
        # Extract breadcrumbs from JSON-LD
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                if script.string:
                    import json
                    data = json.loads(script.string)
                    if data.get('@type') == 'BreadcrumbList':
                        breadcrumbs = []
                        for item in data.get('itemListElement', []):
                            if 'name' in item:
                                breadcrumbs.append(item['name'])
                        
                        if breadcrumbs:
                            metadata['breadcrumbs'] = breadcrumbs
                            if 'guide_name' not in metadata:
                                metadata['guide_name'] = breadcrumbs[0]
            except:
                continue
        
        return metadata
    
    def _extract_links(self, soup: BeautifulSoup, url: str) -> List[str]:
        """Extract documentation links from Zoomin page."""
        base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        found_links = set()
        
        # Find all links in the navigation area
        nav_areas = soup.select('.zDocsNavigation, .navigation, nav')
        
        for nav in nav_areas:
            for link in nav.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(url, href)
                
                # Only include links from same domain that look like doc pages
                if (absolute_url.startswith(base_url) and 
                    ('/bundle/' in absolute_url or '/page/' in absolute_url or 
                     absolute_url.endswith('.htm'))):
                    found_links.add(absolute_url)
        
        return list(found_links)
    
    
    def _clean_content(self, content: str) -> str:
        """Clean extracted content."""
        if not content:
            return content
        
        # Normalize whitespace but preserve newlines for markdown tables
        content = re.sub(r'[ \t]+', ' ', content)  # Collapse spaces and tabs only
        content = re.sub(r' +\n', '\n', content)   # Remove trailing spaces before newlines
        
        # Remove common Zoomin UI elements that might slip through
        ui_patterns = [
            r'Skip to main content',
            r'Was this page helpful\?',
            r'Like Dislike',
            r'Feedback',
            r'Print this page',
            r'Save PDF'
        ]
        
        for pattern in ui_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        
        # Clean up multiple spaces and line breaks
        content = re.sub(r' {2,}', ' ', content)
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        return content.strip()
    
    def get_wait_selectors(self) -> List[str]:
        """Get selectors to wait for when loading Zoomin content."""
        return [self.CONTENT_SELECTOR, '#zDocsContent']
    
    def get_scroll_strategy(self) -> Dict[str, Any]:
        """Get scroll strategy for Zoomin sites."""
        return {
            'enabled': False,  # Modern Zoomin doesn't need scrolling
            'max_attempts': 0,
            'wait_time': 0,
            'trigger_selectors': []
        }