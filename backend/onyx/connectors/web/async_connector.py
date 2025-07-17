"""
Async Web Connector implementation using proper async patterns.
This replaces the sync-over-async anti-pattern in the original connector.
"""

import asyncio
import hashlib
import re
import tempfile
import shutil
import ipaddress
import socket
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, List, Optional, Dict
from urllib.parse import urljoin, urlparse

import aiofiles
from bs4 import BeautifulSoup
from crawlee.crawlers import AdaptivePlaywrightCrawler, BeautifulSoupCrawler, PlaywrightCrawler
from crawlee import ConcurrencySettings
from crawlee.storages import Dataset
from crawlee.request_loaders import SitemapRequestLoader, RequestList

from onyx.configs.app_configs import INDEX_BATCH_SIZE, WEB_CONNECTOR_VALIDATE_URLS
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput, LoadConnector
from onyx.connectors.models import Document, TextSection
from onyx.connectors.web.async_http_client import AsyncHttpClient
from onyx.file_processing.extract_file_text import read_pdf_file
from onyx.file_processing.html_utils import web_html_cleanup, convert_html_to_markdown
from onyx.utils.logger import setup_logger
from shared_configs.configs import MULTI_TENANT

logger = setup_logger()


class WEB_CONNECTOR_VALID_SETTINGS(str, Enum):
    RECURSIVE = "recursive"
    SINGLE = "single"
    SITEMAP = "sitemap"
    UPLOAD = "upload"


def protected_url_check(url: str) -> None:
    """Validates that URL points to a globally accessible resource."""
    if not WEB_CONNECTOR_VALIDATE_URLS:
        return

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("URL must be of scheme https?://")

    if not parsed.hostname:
        raise ValueError("URL must include a hostname")

    try:
        info = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror as e:
        raise ConnectionError(f"DNS resolution failed for {parsed.hostname}: {e}")

    for address in info:
        ip = address[4][0]
        if not ipaddress.ip_address(ip).is_global:
            raise ValueError(
                f"Non-global IP address detected: {ip}, skipping page {url}. "
                f"The Web Connector is not allowed to read loopback, link-local, or private ranges"
            )


def _read_urls_file(location: str) -> list[str]:
    """Read URLs from a file."""
    with open(location, "r") as f:
        urls = [line.strip() if "://" in line.strip() else f"https://{line.strip()}" 
                for line in f if line.strip()]
    return urls


class AsyncWebConnectorCrawlee(LoadConnector):
    """Async-native web connector implementation using Crawlee library with proper async patterns."""
    
    def __init__(
        self,
        base_url: str,
        web_connector_type: str = WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value,
        mintlify_cleanup: bool = True,
        batch_size: int = INDEX_BATCH_SIZE,
        enable_specialized_extraction: bool = True,
        crawler_mode: str = "adaptive",
        skip_images: bool = False,
        selector_config: Optional[str] = None,
        selector_config_file: Optional[str] = None,
        oauth_token: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.base_url = base_url if "://" in base_url else f"https://{base_url}"
        self.mintlify_cleanup = mintlify_cleanup
        self.batch_size = batch_size
        self.web_connector_type = web_connector_type
        self.crawler_mode = crawler_mode
        self.skip_images = skip_images
        self.selector_config = selector_config
        self.selector_config_file = selector_config_file
        self.oauth_token = oauth_token
        
        # Async-safe state management
        self._content_hashes: set[str] = set()
        self._content_hashes_lock = asyncio.Lock()
        self._documents: list[Document] = []
        self._documents_lock = asyncio.Lock()
        self._failed_urls: dict[str, dict[str, Any]] = {}
        self._failed_urls_lock = asyncio.Lock()
        self._link_references: dict[str, set[str]] = {}
        self._link_references_lock = asyncio.Lock()
        
        # Progress tracking and metrics
        self._metrics = {
            'crawl_start_time': None,
            'pages_processed': 0,
            'pages_successful': 0,
            'pages_failed': 0,
            'pages_skipped': 0,
            'documents_created': 0,
            'duplicates_found': 0,
            'bytes_processed': 0,
            'avg_processing_time': 0.0,
            'error_categories': {},
        }
        self._metrics_lock = asyncio.Lock()
        
        # HTTP client for async operations with enhanced connection pooling
        self.http_client = AsyncHttpClient(
            oauth_token=oauth_token,
            max_connections=20,
            max_keepalive_connections=10,
            max_retries=3,
            retry_backoff_factor=0.5,
        )
        
        # Initialize request management with Crawlee's native loaders
        self.request_loader = None
        self.request_manager = None
        self.sitemap_loader = None
        self.recursive = False
        
        # Initialize extractor factory for specialized content extraction
        self.enable_specialized_extraction = enable_specialized_extraction
        self.extractor_factory = None
        if enable_specialized_extraction:
            from onyx.connectors.web.extractors import ExtractorFactory
            self.extractor_factory = ExtractorFactory()
        
        # Initialize selector resolver for generic extraction
        from onyx.connectors.web.selectors import SelectorResolver
        self.selector_resolver = SelectorResolver()
        if selector_config or selector_config_file:
            self._setup_selector_resolver()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.http_client._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with proper cleanup."""
        await self.http_client.close()
    
    def _setup_selector_resolver(self) -> None:
        """Setup the selector resolver with custom configurations."""
        from onyx.connectors.web.selectors import SelectorConfig
        
        # Load configuration from file if provided
        if self.selector_config_file:
            try:
                custom_config = SelectorConfig.from_file(self.selector_config_file)
                self.selector_resolver.register_custom_config(custom_config)
                logger.info(f"Loaded selector config from file: {self.selector_config_file}")
            except Exception as e:
                logger.error(f"Failed to load selector config from {self.selector_config_file}: {e}")
        
        # Load configuration from JSON/YAML string if provided
        if self.selector_config:
            try:
                # Try JSON first, then YAML
                try:
                    custom_config = SelectorConfig.from_json(self.selector_config)
                except ValueError:
                    custom_config = SelectorConfig.from_yaml(self.selector_config)
                
                self.selector_resolver.register_custom_config(custom_config)
                logger.info("Loaded selector config from parameter")
            except Exception as e:
                logger.error(f"Failed to parse selector config: {e}")
    
    async def _initialize_request_loader(self) -> None:
        """Initialize request loader based on connector type using Crawlee's native loaders."""
        if self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value:
            # For recursive crawling, use RequestList with single URL and let crawler discover more
            self.request_loader = RequestList(requests=[self.base_url])
            self.request_manager = None
            self.recursive = True
            
        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SINGLE.value:
            # For single page, use RequestList with just the base URL
            self.request_loader = RequestList(requests=[self.base_url])
            self.request_manager = None
            self.recursive = False
            
        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value:
            # Use Crawlee's native SitemapRequestLoader for efficient sitemap processing
            logger.info(f"Using Crawlee SitemapRequestLoader for sitemap: {self.base_url}")
            
            # Create URL filtering patterns for better control
            exclude_patterns = self._get_exclude_patterns()
            
            # Convert string patterns to regex patterns for SitemapRequestLoader
            import re
            from crawlee.http_clients import HttpxHttpClient
            
            exclude_regexes = [re.compile(pattern.replace('*', '.*')) for pattern in exclude_patterns] if exclude_patterns else None
            
            # Create HTTP client for sitemap loader
            crawlee_http_client = HttpxHttpClient()
            
            # Create SitemapRequestLoader and convert to RequestManager following Crawlee docs
            try:
                logger.info("Creating SitemapRequestLoader...")
                sitemap_loader = SitemapRequestLoader(
                    sitemap_urls=[self.base_url],
                    http_client=crawlee_http_client,
                    exclude=exclude_regexes,
                    max_buffer_size=100000,  # Increased buffer to handle large sitemaps (~80k URLs)
                )
                
                # Convert to RequestManagerTandem as documented
                logger.info("Converting SitemapRequestLoader to RequestManager...")
                self.request_manager = await sitemap_loader.to_tandem()
                self.request_loader = None  # We'll use request_manager instead
                self.sitemap_loader = sitemap_loader  # Store for fallback use
                logger.info("SitemapRequestLoader converted successfully")
                
            except Exception as e:
                logger.warning(f"Failed to create SitemapRequestLoader: {e}")
                # Fallback: use RequestList
                logger.info("Falling back to treating sitemap URL as a single page")
                self.request_loader = RequestList(requests=[self.base_url])
                self.request_manager = None
                self.sitemap_loader = None
            
            self.recursive = False
            
        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            from shared_configs.configs import MULTI_TENANT
            if MULTI_TENANT:
                raise ValueError(
                    "Upload input for web connector is not supported in cloud environments"
                )
            logger.warning(
                "This is not a UI supported Web Connector flow, "
                "are you sure you want to do this?"
            )
            # Read URLs from file and create RequestList
            urls = _read_urls_file(self.base_url)
            self.request_loader = RequestList(requests=urls)
            self.recursive = False
            
        else:
            raise ValueError(
                f"Invalid Web Connector Config, must choose a valid type: "
                f"{[e.value for e in WEB_CONNECTOR_VALID_SETTINGS]}"
            )
    
    def _get_exclude_patterns(self) -> list[str]:
        """Generate URL exclude patterns based on connector configuration."""
        exclude_patterns = []
        
        # Skip images if configured
        if self.skip_images:
            exclude_patterns.extend([
                "**/*.png", "**/*.jpg", "**/*.jpeg", "**/*.gif", 
                "**/*.bmp", "**/*.tiff", "**/*.ico", "**/*.svg", "**/*.webp"
            ])
        
        # Skip unsupported file types
        exclude_patterns.extend([
            "**/*.zip", "**/*.rar", "**/*.7z", "**/*.tar", "**/*.gz", "**/*.bz2",
            "**/*.mp4", "**/*.avi", "**/*.mov", "**/*.wmv", "**/*.flv", "**/*.webm",
            "**/*.mp3", "**/*.wav", "**/*.flac", "**/*.ogg", "**/*.m4a",
            "**/*.exe", "**/*.msi", "**/*.deb", "**/*.rpm", "**/*.dmg",
            "**/*.bin", "**/*.iso", "**/*.img"
        ])
        
        return exclude_patterns
    
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
    
    async def _compute_content_fingerprint(self, title: Optional[str], text_content: str, url: str) -> str:
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
        normalized_content = self._normalize_content_for_deduplication(text_content)
        
        # Create fingerprint components
        content_length = len(normalized_content)
        
        # Use first and last 500 chars for structural similarity
        content_start = normalized_content[:500]
        content_end = normalized_content[-500:] if len(normalized_content) > 500 else ""
        
        # Normalize title
        normalized_title = (title or "").strip().lower()
        
        # Create composite fingerprint
        fingerprint_data = f"{normalized_title}:{content_length}:{content_start}:{content_end}"
        
        # Compute hash asynchronously in a way that doesn't block
        loop = asyncio.get_event_loop()
        hash_result = await loop.run_in_executor(
            None, 
            lambda: hashlib.sha256(fingerprint_data.encode('utf-8')).hexdigest()[:16]
        )
        
        return hash_result
    
    def _normalize_content_for_deduplication(self, content: str) -> str:
        """
        Normalize content for better deduplication detection.
        
        This helps identify duplicate content even when there are minor
        formatting differences.
        """
        if not content:
            return ""
        
        # Convert to lowercase and normalize whitespace
        normalized = ' '.join(content.lower().split())
        
        # Remove common variations that don't affect content meaning
        # Remove extra punctuation and normalize quotes
        normalized = re.sub(r'[""''‚„‹›«»]', '"', normalized)
        normalized = re.sub(r'[–—−]', '-', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Remove trailing/leading whitespace
        return normalized.strip()
    
    async def _is_content_duplicate(self, title: Optional[str], text_content: str, url: str) -> tuple[bool, str]:
        """
        Check if content is a duplicate using sophisticated fingerprinting.
        
        Returns:
            Tuple of (is_duplicate, content_hash)
        """
        # Compute sophisticated content fingerprint
        content_hash = await self._compute_content_fingerprint(title, text_content, url)
        
        # Check for duplicate
        is_duplicate = not await self._add_content_hash(content_hash)
        
        return is_duplicate, content_hash
    
    async def _update_metrics(self, metric_name: str, value: Any = 1, operation: str = 'increment') -> None:
        """
        Update crawler metrics in thread-safe manner.
        
        Args:
            metric_name: Name of the metric to update
            value: Value to add/set (default: 1)
            operation: 'increment', 'set', or 'average'
        """
        async with self._metrics_lock:
            if operation == 'increment':
                self._metrics[metric_name] = self._metrics.get(metric_name, 0) + value
            elif operation == 'set':
                self._metrics[metric_name] = value
            elif operation == 'average':
                # Simple moving average for processing time
                current_avg = self._metrics.get(metric_name, 0.0)
                count = self._metrics.get('pages_processed', 1)
                self._metrics[metric_name] = (current_avg * (count - 1) + value) / count
    
    async def _increment_error_category(self, category: str) -> None:
        """Increment error count for a specific category."""
        async with self._metrics_lock:
            if 'error_categories' not in self._metrics:
                self._metrics['error_categories'] = {}
            self._metrics['error_categories'][category] = self._metrics['error_categories'].get(category, 0) + 1
    
    async def _get_metrics_snapshot(self) -> dict[str, Any]:
        """Get current metrics snapshot."""
        async with self._metrics_lock:
            snapshot = self._metrics.copy()
            
            # Calculate derived metrics
            if snapshot['crawl_start_time']:
                elapsed_time = time.time() - snapshot['crawl_start_time']
                snapshot['elapsed_time'] = elapsed_time
                if elapsed_time > 0:
                    snapshot['pages_per_second'] = snapshot['pages_processed'] / elapsed_time
                    snapshot['documents_per_second'] = snapshot['documents_created'] / elapsed_time
                else:
                    snapshot['pages_per_second'] = 0
                    snapshot['documents_per_second'] = 0
            else:
                snapshot['elapsed_time'] = 0
                snapshot['pages_per_second'] = 0
                snapshot['documents_per_second'] = 0
            
            # Calculate success rate
            total_pages = snapshot['pages_processed']
            if total_pages > 0:
                snapshot['success_rate'] = snapshot['pages_successful'] / total_pages
                snapshot['failure_rate'] = snapshot['pages_failed'] / total_pages
                snapshot['skip_rate'] = snapshot['pages_skipped'] / total_pages
            else:
                snapshot['success_rate'] = 0
                snapshot['failure_rate'] = 0
                snapshot['skip_rate'] = 0
            
            return snapshot
    
    async def _log_progress_metrics(self, force: bool = False) -> None:
        """Log current progress metrics."""
        metrics = await self._get_metrics_snapshot()
        
        # Only log if significant progress or forced
        if force or metrics['pages_processed'] % 50 == 0:
            logger.info(
                f"Crawl Progress - "
                f"Processed: {metrics['pages_processed']} pages, "
                f"Created: {metrics['documents_created']} documents, "
                f"Failed: {metrics['pages_failed']}, "
                f"Skipped: {metrics['pages_skipped']}, "
                f"Rate: {metrics['pages_per_second']:.1f} pages/sec, "
                f"Success: {metrics['success_rate']:.1%}"
            )
            
            if metrics['error_categories']:
                error_summary = ", ".join([f"{cat}:{count}" for cat, count in metrics['error_categories'].items()])
                logger.info(f"Error breakdown: {error_summary}")
    
    async def _add_document(self, document: Document) -> None:
        """Add document to collection in thread-safe manner."""
        async with self._documents_lock:
            self._documents.append(document)
    
    async def _add_failed_url(self, url: str, error: str, source_pages: list[str]) -> None:
        """Add failed URL info in thread-safe manner."""
        async with self._failed_urls_lock:
            self._failed_urls[url] = {
                "error": error,
                "source_pages": source_pages
            }
    
    async def _add_link_reference(self, link: str, source_page: str) -> None:
        """Add link reference in thread-safe manner."""
        async with self._link_references_lock:
            if link not in self._link_references:
                self._link_references[link] = set()
            self._link_references[link].add(source_page)
    
    async def _get_link_references(self, url: str) -> list[str]:
        """Get link references in thread-safe manner."""
        async with self._link_references_lock:
            return list(self._link_references.get(url, set()))
    
    def _extract_content(self, url: str, soup: BeautifulSoup, raw_html: str) -> tuple[str | None, str, dict[str, Any], List[str]]:
        """Extract content using specialized extractors or fallback methods."""
        # Try generic extractor first (if configured)
        if self.selector_resolver:
            generic_extractor = self.selector_resolver.get_extractor(soup, url)
            if generic_extractor:
                extracted = generic_extractor.extract_content(soup, url)
                return extracted.title, extracted.content, extracted.metadata or {}, extracted.links or []
        
        # Try specialized extractor second
        if self.extractor_factory:
            extractor = self.extractor_factory.get_extractor(soup, url)
            if extractor:
                extracted = extractor.extract_content(soup, url)
                return extracted.title, extracted.content, extracted.metadata or {}, extracted.links or []
        
        # Fallback to standard extraction
        title = soup.title.string if soup.title else None
        text_content = convert_html_to_markdown(raw_html)
        
        # Final fallback to traditional cleanup if needed
        if not text_content or len(text_content) < 50:
            parsed_html = web_html_cleanup(soup, self.mintlify_cleanup)
            title = parsed_html.title
            text_content = parsed_html.cleaned_text
        
        return title, text_content, {}, []
    
    async def _handle_pdf_from_context_async(self, context) -> Optional[Document]:
        """Enhanced async PDF handling with better error recovery and memory management."""
        url = context.request.url
        pdf_content = None
        
        try:
            # Get PDF content with improved error handling
            if hasattr(context, 'response') and context.response:
                pdf_content = context.response.body
                content_length = len(pdf_content) if pdf_content else 0
                logger.info(f"Processing PDF from Crawlee context: {url} ({content_length} bytes)")
            else:
                # Fallback: download PDF using async client with streaming for large files
                logger.info(f"Downloading PDF using HTTP client: {url}")
                
                # Check content size first with HEAD request
                head_response = await self.http_client.head(url)
                content_length = int(head_response.headers.get('content-length', 0))
                
                if content_length > 50 * 1024 * 1024:  # 50MB threshold
                    logger.info(f"Large PDF detected ({content_length} bytes), using streaming download")
                    pdf_content = await self._stream_large_pdf(url)
                else:
                    pdf_content = await self.http_client.get_content(url)
            
            if not pdf_content:
                logger.warning(f"No PDF content received for {url}")
                return None
            
            # Validate PDF content
            if not pdf_content.startswith(b'%PDF'):
                logger.warning(f"Invalid PDF content for {url} (missing PDF header)")
                return None
            
            # Extract text with enhanced error handling
            page_text, metadata, images = await self._extract_pdf_text_async(pdf_content, url)
            
            if not page_text or page_text.strip() == "":
                logger.warning(f"No text content extracted from PDF {url}")
                return None
            
            # Get last modified header if available
            last_modified = None
            if hasattr(context, 'response') and context.response and hasattr(context.response, 'headers'):
                last_modified = context.response.headers.get("Last-Modified")
            
            return Document(
                id=url,
                sections=[TextSection(link=url, text=page_text.strip())],
                source=DocumentSource.WEB,
                semantic_identifier=url.split("/")[-1],
                metadata={
                    'file_type': 'pdf',
                    'file_size': len(pdf_content),
                    'extraction_method': 'async_pdf_processor',
                    **metadata
                },
                doc_updated_at=self.http_client.get_last_modified_datetime(context.response) if hasattr(context, 'response') and context.response else None,
            )
            
        except asyncio.TimeoutError:
            logger.error(f"Timeout processing PDF {url}")
            return None
        except MemoryError:
            logger.error(f"Out of memory processing large PDF {url}")
            return None
        except Exception as e:
            logger.error(f"Failed to process PDF {url}: {type(e).__name__}: {e}")
            return None
        finally:
            # Cleanup large PDF content from memory
            if pdf_content and len(pdf_content) > 10 * 1024 * 1024:  # 10MB
                del pdf_content
    
    async def _stream_large_pdf(self, url: str) -> bytes:
        """Stream large PDF files to avoid memory issues."""
        chunks = []
        total_size = 0
        max_size = 100 * 1024 * 1024  # 100MB limit
        
        async for chunk in self.http_client.stream_content(url):
            chunks.append(chunk)
            total_size += len(chunk)
            
            if total_size > max_size:
                raise ValueError(f"PDF file too large: {total_size} bytes exceeds {max_size} byte limit")
        
        return b''.join(chunks)
    
    async def _extract_pdf_text_async(self, pdf_content: bytes, url: str) -> tuple[str, dict, list]:
        """
        Extract text from PDF content asynchronously to avoid blocking the event loop.
        """
        import asyncio
        import concurrent.futures
        import io
        
        def extract_text():
            """Run PDF extraction in thread pool to avoid blocking."""
            try:
                return read_pdf_file(file=io.BytesIO(pdf_content))
            except Exception as e:
                logger.error(f"PDF text extraction failed for {url}: {e}")
                return "", {}, []
        
        # Run PDF extraction in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            try:
                # Add timeout to prevent hanging on corrupted PDFs
                page_text, metadata, images = await asyncio.wait_for(
                    loop.run_in_executor(executor, extract_text),
                    timeout=60.0  # 60 second timeout
                )
                return page_text, metadata, images
            except asyncio.TimeoutError:
                logger.error(f"PDF text extraction timed out for {url}")
                return "", {}, []
    
    async def _handle_adaptive_page_async(self, context, documents_queue: asyncio.Queue) -> None:
        """Async version of page processing with proper async patterns and metrics tracking."""
        url = context.request.url
        start_time = time.time()
        
        try:
            # Validate URL
            protected_url_check(url)
            
            # Get content from adaptive context - try different access methods
            soup = None
            raw_html = None
            
            # Method 1: Try Playwright page content
            try:
                raw_html = await context.page.content()
                soup = BeautifulSoup(raw_html, "html.parser")
                logger.info(f"Processing with Playwright: {url}")
            except Exception:
                # Method 2: Try static soup
                try:
                    if hasattr(context, 'soup') and context.soup is not None:
                        logger.info(f"Processing with static parser: {url}")
                        soup = context.soup
                        raw_html = str(soup)
                except Exception:
                    pass
            
            # Method 3: Try response content (AdaptivePlaywrightCrawler sometimes uses this)
            if soup is None and hasattr(context, 'response'):
                try:
                    raw_html = await context.response.text()
                    soup = BeautifulSoup(raw_html, "html.parser")
                    logger.info(f"Processing with response content: {url}")
                except Exception:
                    pass
            
            # Final check
            if soup is None:
                logger.debug(f"Available context attributes for {url}: {[attr for attr in dir(context) if not attr.startswith('_')]}")
                raise ValueError(f"No valid content available in context for {url}")
            
            # Extract content using specialized extractors or fallback
            title, text_content, metadata, extracted_links = self._extract_content(url, soup, raw_html)
            
            # Track link references for broken link reporting
            if extracted_links:
                for link in extracted_links:
                    await self._add_link_reference(link, url)
            
            # Add discovered links for recursive crawling
            if self.recursive:
                if extracted_links:
                    logger.info(f"Enqueueing {len(extracted_links)} extracted links for {url}")
                    for link in extracted_links:
                        await context.add_requests([link])
                else:
                    await context.enqueue_links(strategy="same-hostname")
            
            # Validate and sanitize text content
            if text_content is None or text_content == "":
                logger.warning(f"No text content extracted for {url}, skipping")
                return
            
            text_content = str(text_content)
            
            # Check for duplicate content using sophisticated async-native fingerprinting
            is_duplicate, content_hash = await self._is_content_duplicate(title, text_content, url)
            
            if is_duplicate:
                logger.info(f"Skipping duplicate content for {url} (fingerprint: {content_hash})")
                await self._update_metrics('pages_skipped')
                await self._update_metrics('duplicates_found')
                await self._update_metrics('pages_processed')
                return
            
            # Get last modified header
            last_modified = getattr(context.response, 'headers', {}).get("Last-Modified") if hasattr(context, 'response') else None
            
            # Create document
            doc = Document(
                id=url,
                sections=[TextSection(link=url, text=text_content)],
                source=DocumentSource.WEB,
                semantic_identifier=title or url,
                metadata=metadata,
                doc_updated_at=self.http_client.get_last_modified_datetime(context.response) if hasattr(context, 'response') and context.response else None,
            )
            
            # Add to queue for batching with backpressure handling
            await self._put_document_with_backpressure(documents_queue, doc, url)
            
            # Update metrics for successful processing
            processing_time = time.time() - start_time
            await self._update_metrics('pages_processed')
            await self._update_metrics('pages_successful')
            await self._update_metrics('documents_created')
            await self._update_metrics('bytes_processed', len(text_content))
            await self._update_metrics('avg_processing_time', processing_time, 'average')
            
            # Log progress periodically
            await self._log_progress_metrics()
            
        except Exception as e:
            if "Page was not crawled with PlaywrightCrawler" in str(e):
                # This is a known issue with AdaptivePlaywrightCrawler - not a critical error
                logger.debug(f"AdaptivePlaywrightCrawler internal error for {url}: {e}")
                # Still count as processed since content was likely extracted
                await self._update_metrics('pages_processed')
            else:
                logger.error(f"Error processing {url}: {e}")
                await self._update_metrics('pages_processed')
                await self._update_metrics('pages_failed')
    
    async def _handle_binary_file_async(self, context, documents_queue: asyncio.Queue) -> None:
        """Async version of binary file handling."""
        url = context.request.url
        
        try:
            # Download the file content
            if hasattr(context, 'response') and context.response:
                file_content = context.response.body
                content_type = context.response.headers.get('content-type', '')
            else:
                file_content = await self.http_client.get_content(url)
                content_type = 'application/octet-stream'
            
            # Process different file types
            if url.lower().endswith('.pdf'):
                doc = await self._process_pdf_async(url, file_content)
            else:
                # For other binary files, use the generic file processor
                doc = await self._process_binary_file_async(url, file_content, content_type)
            
            if doc:
                await self._put_document_with_backpressure(documents_queue, doc, url)
                
        except Exception as e:
            logger.error(f"Error handling binary file {url}: {e}")
    
    async def _process_pdf_async(self, url: str, content: bytes) -> Optional[Document]:
        """Process PDF content asynchronously."""
        try:
            import io
            page_text, metadata, images = read_pdf_file(file=io.BytesIO(content))
            
            if not page_text or page_text.strip() == "":
                logger.warning(f"No text content extracted from PDF {url}")
                return None
            
            return Document(
                id=url,
                sections=[TextSection(link=url, text=page_text.strip())],
                source=DocumentSource.WEB,
                semantic_identifier=url.split("/")[-1],
                metadata={
                    'extracted_from_pdf': True,
                    'file_size': len(content),
                    **metadata
                },
                doc_updated_at=None,
            )
        except Exception as e:
            logger.error(f"Error processing PDF {url}: {e}")
            return None
    
    async def _process_binary_file_async(self, url: str, content: bytes, content_type: str) -> Optional[Document]:
        """Process binary file content asynchronously."""
        try:
            # Determine file extension from URL
            file_extension = None
            for ext in ['.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']:
                if url.lower().endswith(ext):
                    file_extension = ext
                    break
            
            if not file_extension:
                logger.warning(f"Could not determine file extension for {url}")
                return None
            
            # Create temporary file for processing
            async with aiofiles.tempfile.NamedTemporaryFile(suffix=file_extension) as tmp_file:
                await tmp_file.write(content)
                await tmp_file.flush()
                
                # Extract text using existing file processing
                from onyx.file_processing.extract_file_text import extract_file_text
                text_content = extract_file_text(tmp_file.name)
                
                if not text_content or text_content.strip() == "":
                    logger.warning(f"No text content extracted from {url}")
                    return None
                
                return Document(
                    id=url,
                    sections=[TextSection(link=url, text=text_content.strip())],
                    source=DocumentSource.WEB,
                    semantic_identifier=url.split("/")[-1],
                    metadata={
                        'extracted_from_file': True,
                        'content_type': content_type,
                        'file_size': len(content)
                    },
                    doc_updated_at=None,
                )
        except Exception as e:
            logger.error(f"Error processing binary file {url}: {e}")
            return None
    
    async def _crawl_async(self) -> AsyncGenerator[List[Document], None]:
        """
        Pure async crawling implementation using Crawlee's native patterns with backpressure control.
        Yields batches of documents as they become available with proper flow control.
        """
        # Initialize metrics tracking
        await self._update_metrics('crawl_start_time', time.time(), 'set')
        
        # Initialize request loader using Crawlee's native capabilities
        await self._initialize_request_loader()
        
        # Create storage directory
        storage_dir = tempfile.mkdtemp(prefix="async_crawlee_storage_")
        
        try:
            # Set storage directory
            import os
            os.environ['CRAWLEE_STORAGE_DIR'] = storage_dir
            
            # Open dataset
            dataset = await Dataset.open(name='results')
            
            # Create document queue with backpressure control
            # Queue size should be 2-3x batch size to allow for smooth flow without excessive memory usage
            max_queue_size = max(self.batch_size * 3, 50)  # Minimum 50, max 3x batch size
            documents_queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
            
            # Batch management with backpressure
            batch_accumulator: List[Document] = []
            documents_processed = 0
            batches_yielded = 0
            
            # Create crawler based on mode
            crawler = await self._create_crawler()
            
            # Setup handlers with backpressure-aware queue
            await self._setup_crawler_handlers(crawler, documents_queue)
            
            # Handle different request loader types following Crawlee documentation
            if self.request_manager and hasattr(crawler, '_request_manager') and crawler._request_manager:
                # Using RequestManager (from SitemapRequestLoader.to_tandem()) - documented approach
                logger.info("Using Crawlee RequestManager (from SitemapRequestLoader)")
                crawl_task = asyncio.create_task(crawler.run())  # No arguments needed
                feed_task = None
                
            elif self.request_manager:
                # We have a RequestManager but crawler doesn't (SSL context fallback)
                # Use simple manual sitemap parsing
                logger.info("Extracting URLs from sitemap manually due to SSL context fallback")
                try:
                    # Use our async HTTP client to fetch and parse sitemap manually
                    async with self.http_client:
                        sitemap_response = await self.http_client.get(self.base_url)
                        sitemap_content = sitemap_response.text
                        
                        # Parse sitemap XML manually to extract URLs
                        import re
                        url_pattern = r'<loc>(.*?)</loc>'
                        all_urls = re.findall(url_pattern, sitemap_content)
                        
                        # Separate sitemap files from page URLs
                        sitemap_urls = []
                        page_urls = []
                        
                        for url in all_urls:
                            if not url.startswith(('http://', 'https://')):
                                continue
                            elif (url.endswith(('.xml', '.xml.gz')) or 'sitemap' in url.lower()):
                                sitemap_urls.append(url)
                            else:
                                page_urls.append(url)
                        
                        # If we found sitemap parts, fetch them to get actual page URLs
                        if sitemap_urls and not page_urls:
                            logger.info(f"Found {len(sitemap_urls)} sitemap parts, fetching page URLs...")
                            for sitemap_url in sitemap_urls[:10]:  # Limit to first 10 sitemaps
                                try:
                                    sub_response = await self.http_client.get(sitemap_url)
                                    sub_content = sub_response.text
                                    sub_urls = re.findall(url_pattern, sub_content)
                                    
                                    # Add non-sitemap URLs
                                    for sub_url in sub_urls:
                                        if (sub_url.startswith(('http://', 'https://')) and 
                                            not sub_url.endswith(('.xml', '.xml.gz')) and
                                            'sitemap' not in sub_url.lower()):
                                            page_urls.append(sub_url)
                                except Exception as e:
                                    logger.warning(f"Failed to fetch sitemap part {sitemap_url}: {e}")
                        
                        urls_to_crawl = page_urls
                        
                        logger.info(f"Starting crawl with {len(urls_to_crawl)} URLs from sitemap")
                        crawl_task = asyncio.create_task(crawler.run(urls_to_crawl))
                        feed_task = None
                    
                except Exception as e:
                    logger.error(f"Failed to extract URLs from sitemap: {e}")
                    # Final fallback - treat sitemap URL as single page
                    logger.info("Final fallback: treating sitemap URL as single page")
                    crawl_task = asyncio.create_task(crawler.run([self.base_url]))
                    feed_task = None
                
            elif hasattr(self.request_loader, 'get_total_count'):
                # RequestList - extract all requests
                logger.info(f"Using streaming request loader: {type(self.request_loader).__name__}")
                total_count = await self.request_loader.get_total_count()
                logger.info(f"Extracting {total_count} requests from RequestList")
                requests_to_crawl = []
                
                # Extract all requests from the RequestList
                while not await self.request_loader.is_empty():
                    request = await self.request_loader.fetch_next_request()
                    if request:
                        requests_to_crawl.append(request)
                        # Don't mark as handled yet - crawler will handle that
                
                logger.info(f"Starting crawl with {len(requests_to_crawl)} requests")
                crawl_task = asyncio.create_task(crawler.run(requests_to_crawl))
                feed_task = None
                
            elif hasattr(self.request_loader, '__iter__'):
                # Iterable of requests
                requests_to_crawl = list(self.request_loader)
                logger.info(f"Starting crawl with {len(requests_to_crawl)} requests from iterable")
                crawl_task = asyncio.create_task(crawler.run(requests_to_crawl))
                feed_task = None
            else:
                # Fallback - treat as single request
                requests_to_crawl = [self.request_loader]
                logger.info(f"Starting crawl with single request")
                crawl_task = asyncio.create_task(crawler.run(requests_to_crawl))
                feed_task = None
            
            # Create a backpressure monitoring task
            backpressure_stats = {
                'queue_full_events': 0,
                'max_queue_size_seen': 0,
                'last_stats_log': time.time()
            }
            
            # Process documents as they become available with backpressure control
            try:
                while not crawl_task.done() or not documents_queue.empty():
                    try:
                        # Dynamic timeout based on queue status and crawl progress
                        if documents_queue.empty() and crawl_task.done():
                            # No more documents coming, break out
                            break
                        elif documents_queue.qsize() > max_queue_size * 0.8:
                            # Queue is getting full, process faster with shorter timeout
                            timeout = 1.0
                        elif crawl_task.done():
                            # Crawling finished, process remaining with short timeout
                            timeout = 2.0
                        else:
                            # Normal processing
                            timeout = 5.0
                        
                        # Wait for document with adaptive timeout
                        doc = await asyncio.wait_for(documents_queue.get(), timeout=timeout)
                        batch_accumulator.append(doc)
                        documents_processed += 1
                        
                        # Update backpressure statistics
                        current_queue_size = documents_queue.qsize()
                        backpressure_stats['max_queue_size_seen'] = max(
                            backpressure_stats['max_queue_size_seen'], 
                            current_queue_size
                        )
                        
                        # Log backpressure statistics periodically
                        current_time = time.time()
                        if current_time - backpressure_stats['last_stats_log'] > 30:  # Every 30 seconds
                            logger.info(
                                f"Crawl progress: {documents_processed} documents processed, "
                                f"{batches_yielded} batches yielded, queue size: {current_queue_size}/{max_queue_size}, "
                                f"max queue size seen: {backpressure_stats['max_queue_size_seen']}"
                            )
                            backpressure_stats['last_stats_log'] = current_time
                        
                        # Yield batch when full or when backpressure threshold is reached
                        should_yield_batch = (
                            len(batch_accumulator) >= self.batch_size or
                            (current_queue_size > max_queue_size * 0.7 and len(batch_accumulator) > 0)
                        )
                        
                        if should_yield_batch:
                            logger.debug(f"Yielding batch {batches_yielded + 1} with {len(batch_accumulator)} documents")
                            yield batch_accumulator
                            batches_yielded += 1
                            batch_accumulator = []
                            
                            # Brief pause to allow queue to be processed if it's very full
                            if current_queue_size > max_queue_size * 0.8:
                                await asyncio.sleep(0.1)
                            
                    except asyncio.TimeoutError:
                        # Timeout occurred - check why and adjust strategy
                        if crawl_task.done():
                            # Crawling finished, process any remaining documents quickly
                            continue
                        elif documents_queue.qsize() == 0:
                            # No documents available, continue waiting
                            continue
                        else:
                            # Queue has documents but timeout occurred - should not happen
                            logger.warning(f"Unexpected timeout with {documents_queue.qsize()} documents in queue")
                            continue
                
                # Wait for both crawl and feed tasks to complete if they haven't already
                if not crawl_task.done():
                    logger.info("Waiting for crawl task to complete...")
                    await crawl_task
                
                # Also wait for feed task if it exists
                if 'feed_task' in locals() and feed_task and not feed_task.done():
                    logger.info("Waiting for request feeding task to complete...")
                    await feed_task
                
                # Process any remaining documents in queue with faster processing
                logger.debug(f"Processing remaining {documents_queue.qsize()} documents in queue")
                while not documents_queue.empty():
                    try:
                        doc = await asyncio.wait_for(documents_queue.get(), timeout=1.0)
                        batch_accumulator.append(doc)
                        documents_processed += 1
                    except asyncio.TimeoutError:
                        # No more documents available
                        break
                
                # Yield final batch if any documents remain
                if batch_accumulator:
                    logger.debug(f"Yielding final batch with {len(batch_accumulator)} documents")
                    yield batch_accumulator
                    batches_yielded += 1
                
                # Final statistics
                logger.info(
                    f"Crawl completed: {documents_processed} total documents processed, "
                    f"{batches_yielded} batches yielded, "
                    f"max queue size reached: {backpressure_stats['max_queue_size_seen']}/{max_queue_size}"
                )
                
                # Log final comprehensive metrics
                await self._log_progress_metrics(force=True)
                final_metrics = await self._get_metrics_snapshot()
                logger.info(
                    f"Final Crawl Metrics - "
                    f"Duration: {final_metrics['elapsed_time']:.1f}s, "
                    f"Total Pages: {final_metrics['pages_processed']}, "
                    f"Documents: {final_metrics['documents_created']}, "
                    f"Success Rate: {final_metrics['success_rate']:.1%}, "
                    f"Avg Processing: {final_metrics['avg_processing_time']:.2f}s/page, "
                    f"Data Processed: {final_metrics['bytes_processed'] / (1024*1024):.1f}MB"
                )
                
            except Exception as e:
                logger.error(f"Error in document processing: {e}")
                # Cancel crawl task if still running
                if not crawl_task.done():
                    logger.info("Cancelling crawl task due to error...")
                    crawl_task.cancel()
                    try:
                        await crawl_task
                    except asyncio.CancelledError:
                        logger.info("Crawl task cancelled successfully")
                raise
        
        finally:
            # Clean up storage directory
            shutil.rmtree(storage_dir, ignore_errors=True)
    
    async def _create_crawler(self):
        """Create crawler based on configured mode with enhanced error handling."""
        # Use Crawlee's default concurrency settings for optimal performance
        concurrency_settings = ConcurrencySettings()
        
        # Use request_manager if available (for sitemap), otherwise use default
        crawler_kwargs = {
            'concurrency_settings': concurrency_settings,
        }
        if self.request_manager:
            crawler_kwargs['request_manager'] = self.request_manager
        
        # Enhanced browser options for better stability
        browser_args = [
            "--no-sandbox", 
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        
        try:
            if self.crawler_mode == "adaptive":
                # Try with RequestManager first, fallback without it if SSL context issues
                try:
                    logger.info("Creating adaptive crawler with RequestManager")
                    
                    # Create custom rendering type predictor for better learning
                    from crawlee.crawlers._adaptive._adaptive_playwright_crawler import RenderingTypePredictor, RenderingTypePrediction
                    
                    class FrameworkAwareRenderingTypePredictor(RenderingTypePredictor):
                        def __init__(self, selector_resolver):
                            super().__init__()
                            self.selector_resolver = selector_resolver
                            self._failed_static_domains = set()
                            self._framework_requires_js = {}  # Cache framework JS requirements
                            
                        def _requires_javascript_rendering(self, url):
                            """Check if any framework configuration indicates this needs JavaScript"""
                            try:
                                # Check each framework to see if it requires JS rendering
                                for config in self.selector_resolver.framework_loader.get_frameworks_by_priority():
                                    if not config.enabled:
                                        continue
                                    
                                    # Check if framework has JS requirement indicators
                                    framework_name = config.name
                                    if framework_name not in self._framework_requires_js:
                                        # Determine if this framework typically needs JS based on its selectors
                                        detection_selectors = getattr(config.detection, 'required', [])
                                        if not detection_selectors:
                                            # Handle old format where detection is a list
                                            detection_selectors = config.detection if isinstance(config.detection, list) else []
                                        
                                        requires_js = any('script' in selector.lower() or 
                                                        'dynamic' in selector.lower() or
                                                        'javascript' in selector.lower() or
                                                        'js-' in selector.lower()
                                                        for selector in detection_selectors)
                                        self._framework_requires_js[framework_name] = requires_js
                                    
                                    # If this framework requires JS, return True
                                    # The actual framework detection will happen during page processing
                                    if self._framework_requires_js[framework_name]:
                                        return True
                                                
                                return False
                            except Exception as e:
                                logger.debug(f"Error checking framework JS requirements: {e}")
                                return False
                            
                        def predict(self, request) -> RenderingTypePrediction:
                            url = request.url if hasattr(request, 'url') else str(request)
                            
                            # If we know this domain needs Playwright, use it directly
                            domain = url.split('/')[2] if '://' in url else ''
                            if domain in self._failed_static_domains:
                                return RenderingTypePrediction(
                                    rendering_type='client_only',
                                    detection_probability_recommendation=0.95
                                )
                            
                            # Check if framework definitions indicate JS requirement
                            if self._requires_javascript_rendering(url):
                                return RenderingTypePrediction(
                                    rendering_type='client_only',
                                    detection_probability_recommendation=0.8
                                )
                            
                            # Default to trying static first
                            return RenderingTypePrediction(
                                rendering_type='static',
                                detection_probability_recommendation=0.3
                            )
                        
                        def store_result(self, request, result):
                            """Learn from failed static parsing attempts"""
                            url = request.url if hasattr(request, 'url') else str(request)
                            domain = url.split('/')[2] if '://' in url else ''
                            
                            # If static parsing failed or produced poor results, remember this domain
                            if (hasattr(result, 'failed') and result.failed) or \
                               (hasattr(result, 'content') and len(result.content or '') < 100):
                                self._failed_static_domains.add(domain)
                                logger.info(f"Marking domain {domain} as requiring Playwright rendering")
                    
                    return AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
                        rendering_type_predictor=FrameworkAwareRenderingTypePredictor(self.selector_resolver),
                        playwright_crawler_specific_kwargs={
                            "browser_launch_options": {
                                "args": browser_args,
                                "timeout": 30000,  # 30 second timeout
                                "ignore_default_args": ["--enable-automation"],
                            },
                            "request_handler_timeout": timedelta(seconds=60),  # 60 second page timeout
                            "max_request_retries": 3,
                        },
                        **crawler_kwargs,
                    )
                except Exception as e:
                    if "cannot pickle 'SSLContext' object" in str(e) and self.request_manager:
                        logger.warning("SSL context pickling issue detected, creating adaptive crawler without RequestManager")
                        # Create crawler without RequestManager to avoid SSL context issues
                        crawler_kwargs_no_manager = {k: v for k, v in crawler_kwargs.items() if k != 'request_manager'}
                        return AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
                            rendering_type_predictor=FrameworkAwareRenderingTypePredictor(self.selector_resolver),
                            playwright_crawler_specific_kwargs={
                                "browser_launch_options": {
                                    "args": browser_args,
                                    "timeout": 30000,  # 30 second timeout
                                    "ignore_default_args": ["--enable-automation"],
                                },
                                "request_handler_timeout": timedelta(seconds=60),  # 60 second page timeout
                                "max_request_retries": 3,
                            },
                            **crawler_kwargs_no_manager,
                        )
                    else:
                        raise
            elif self.crawler_mode == "static":
                logger.info("Creating static crawler with default concurrency settings")
                return BeautifulSoupCrawler(
                    request_handler_timeout=timedelta(seconds=45),
                    max_request_retries=3,
                    **crawler_kwargs,
                )
            else:  # dynamic
                logger.info("Creating dynamic crawler with default concurrency settings")
                return PlaywrightCrawler(
                    browser_launch_options={
                        "args": browser_args,
                        "timeout": 30000,
                        "ignore_default_args": ["--enable-automation"],
                    },
                    request_handler_timeout=timedelta(seconds=60),
                    max_request_retries=3,
                    **crawler_kwargs,
                )
        except Exception as e:
            logger.error(f"Failed to create {self.crawler_mode} crawler: {e}")
            # Fallback to static crawler with default configuration
            logger.warning("Falling back to static crawler with default configuration")
            # Fallback crawler - use only basic settings without request_manager
            return BeautifulSoupCrawler(
                concurrency_settings=ConcurrencySettings(),
                request_handler_timeout=timedelta(seconds=30),
                max_request_retries=2,
            )
    
    async def _setup_crawler_handlers(self, crawler, documents_queue: asyncio.Queue):
        """Setup crawler handlers for different content types with backpressure support."""
        
        @crawler.router.default_handler
        async def async_handler(context) -> None:
            url = context.request.url
            
            try:
                # Basic URL validation (Crawlee handles most filtering through patterns)
                protected_url_check(url)
                
                # Handle different content types
                if url.lower().endswith(".pdf"):
                    doc = await self._handle_pdf_from_context_async(context)
                    if doc:
                        await self._put_document_with_backpressure(documents_queue, doc, url)
                elif self._is_binary_file(url):
                    await self._handle_binary_file_async(context, documents_queue)
                else:
                    await self._handle_adaptive_page_async(context, documents_queue)
                    
            except Exception as e:
                logger.error(f"Handler error for {url}: {e}")
        
        # Enhanced failed request handler with better error classification
        async def handle_failed_request(context) -> None:
            url = context.request.url
            error_info = getattr(context, 'error', None) or getattr(context, 'exception', None)
            retry_count = getattr(context.request, 'retry_count', 0)
            
            # Classify error type and determine if retry is worthwhile
            error_category, error_reason, should_retry = self._classify_crawl_error(error_info, url)
            
            # Log appropriate level based on error severity
            if error_category == "temporary":
                log_level = logger.warning if retry_count < 2 else logger.error
                log_level(f"Temporary error processing {url} (attempt {retry_count + 1}): {error_reason}")
            elif error_category == "client_error":
                logger.info(f"Client error for {url}: {error_reason}")
            elif error_category == "server_error":
                logger.warning(f"Server error for {url}: {error_reason}")
            elif error_category == "network_error":
                logger.warning(f"Network error for {url}: {error_reason}")
            else:
                logger.error(f"Unexpected error processing {url}: {error_reason}")
            
            # Track error statistics
            source_pages = await self._get_link_references(url)
            await self._add_failed_url(url, f"{error_category}: {error_reason}", source_pages)
            
            # Update metrics
            await self._increment_error_category(error_category)
            
            # For certain errors, we might want to add retry hints or alternative strategies
            if error_category == "rate_limit":
                logger.info(f"Rate limit detected for {url}, crawler will automatically back off")
            elif error_category == "authentication":
                logger.warning(f"Authentication required for {url}, check OAuth configuration")
        
        crawler.failed_request_handler = handle_failed_request
    
    def _classify_crawl_error(self, error_info: Any, url: str) -> tuple[str, str, bool]:
        """
        Classify crawl errors for better handling and reporting.
        
        Returns:
            Tuple of (error_category, error_reason, should_retry)
        """
        if not error_info:
            return "unknown", "Unknown error", False
        
        error_msg = str(error_info).lower()
        
        # HTTP status code errors
        if "status code:" in error_msg:
            try:
                status_code = int(error_msg.split("status code:")[1].strip().rstrip(")").split()[0])
                
                if 400 <= status_code < 500:
                    if status_code == 401:
                        return "authentication", f"HTTP {status_code} - Unauthorized", False
                    elif status_code == 403:
                        return "client_error", f"HTTP {status_code} - Forbidden", False
                    elif status_code == 404:
                        return "client_error", f"HTTP {status_code} - Not Found", False
                    elif status_code == 429:
                        return "rate_limit", f"HTTP {status_code} - Rate Limited", True
                    else:
                        return "client_error", f"HTTP {status_code}", False
                elif 500 <= status_code < 600:
                    return "server_error", f"HTTP {status_code}", True
                else:
                    return "unknown", f"HTTP {status_code}", False
            except (ValueError, IndexError):
                return "unknown", f"Invalid status code in: {error_msg}", False
        
        # Network-related errors
        elif any(keyword in error_msg for keyword in ["timeout", "timed out"]):
            return "temporary", "Connection timeout", True
        elif any(keyword in error_msg for keyword in ["connection", "connect"]):
            if "refused" in error_msg:
                return "network_error", "Connection refused", False
            elif "reset" in error_msg:
                return "temporary", "Connection reset", True
            else:
                return "network_error", "Connection error", True
        elif "dns" in error_msg or "name resolution" in error_msg:
            return "network_error", "DNS resolution failed", False
        elif "ssl" in error_msg or "certificate" in error_msg:
            return "network_error", "SSL/TLS error", False
        
        # Browser/Playwright specific errors
        elif "playwright" in error_msg:
            if "timeout" in error_msg:
                return "temporary", "Browser timeout", True
            elif "navigation" in error_msg:
                return "temporary", "Navigation failed", True
            else:
                return "browser_error", f"Browser error: {error_msg[:100]}", True
        
        # Memory or resource errors
        elif any(keyword in error_msg for keyword in ["memory", "resource", "system"]):
            return "resource_error", "System resource error", False
        
        # Generic categorization
        elif "cancelled" in error_msg:
            return "cancelled", "Request cancelled", False
        else:
            return "unknown", error_msg[:200], False  # Truncate long error messages
    
    async def _put_document_with_backpressure(self, documents_queue: asyncio.Queue, doc: Document, url: str) -> None:
        """
        Put document into queue with backpressure handling.
        
        This method handles the case where the queue is full by implementing
        backpressure strategies to prevent overwhelming the system.
        """
        max_wait_time = 30.0  # Maximum time to wait for queue space
        retry_interval = 0.5  # Initial retry interval
        max_retry_interval = 5.0  # Maximum retry interval
        
        start_time = time.time()
        current_retry_interval = retry_interval
        
        while True:
            try:
                # Try to put document with a timeout
                await asyncio.wait_for(documents_queue.put(doc), timeout=current_retry_interval)
                return  # Success
                
            except asyncio.TimeoutError:
                elapsed_time = time.time() - start_time
                
                if elapsed_time > max_wait_time:
                    logger.error(
                        f"Failed to queue document from {url} after {max_wait_time}s - "
                        f"queue size: {documents_queue.qsize()}/{documents_queue.maxsize}. "
                        f"System may be overwhelmed, dropping document."
                    )
                    return  # Drop the document to prevent hanging
                
                # Log backpressure situation
                queue_size = documents_queue.qsize()
                queue_maxsize = documents_queue.maxsize
                logger.warning(
                    f"Queue backpressure detected for {url} - "
                    f"queue size: {queue_size}/{queue_maxsize}, "
                    f"elapsed: {elapsed_time:.1f}s, retrying in {current_retry_interval:.1f}s"
                )
                
                # Exponential backoff with jitter
                await asyncio.sleep(current_retry_interval)
                current_retry_interval = min(current_retry_interval * 1.5, max_retry_interval)
            
            except Exception as e:
                logger.error(f"Unexpected error queuing document from {url}: {e}")
                return  # Drop document on unexpected error
    
    
    def _is_binary_file(self, url: str) -> bool:
        """Check if URL points to a binary file we can process."""
        supported_extensions = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'}
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in supported_extensions)
    
    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        """Load credentials (not used in async version)."""
        if credentials:
            logger.warning("Unexpected credentials provided for Async Web Connector")
        return None
    
    def load_from_state(self) -> GenerateDocumentsOutput:
        """
        Sync wrapper for async implementation to maintain compatibility.
        This provides the bridge between sync and async worlds.
        """
        import asyncio
        
        async def run_async_crawl():
            """Run the async crawl and collect all batches."""
            batches = []
            async with self:  # Use async context manager for proper cleanup
                async for batch in self._crawl_async():
                    batches.append(batch)
            return batches
        
        # Execute the async operation
        try:
            # Try to get the current event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're in an async context, run in a separate thread
                import concurrent.futures
                import threading
                
                def run_in_thread():
                    return asyncio.run(run_async_crawl())
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    batches = future.result()
            else:
                batches = loop.run_until_complete(run_async_crawl())
        except RuntimeError:
            # No event loop, create a new one
            batches = asyncio.run(run_async_crawl())
        
        # Yield each batch
        for batch in batches:
            yield batch
    
    async def load_from_state_async(self) -> AsyncGenerator[List[Document], None]:
        """
        Native async interface for document generation.
        This is the preferred interface for async consumers.
        """
        async for batch in self._crawl_async():
            yield batch
    
    def validate_connector_settings(self) -> None:
        """Validate connector configuration (sync operation)."""
        # Basic validation that doesn't require async operations
        if not self.base_url:
            from onyx.connectors.exceptions import ConnectorValidationError
            raise ConnectorValidationError("Base URL is required")
        
        # URL format validation
        from urllib.parse import urlparse
        parsed = urlparse(self.base_url if "://" in self.base_url else f"https://{self.base_url}")
        if not parsed.scheme or not parsed.netloc:
            from onyx.connectors.exceptions import ConnectorValidationError
            raise ConnectorValidationError(f"Invalid URL format: {self.base_url}")
        
        # Sync validation calls
        self._validate_basic_settings()
    
    def _validate_basic_settings(self) -> None:
        """Validate basic configuration settings synchronously."""
        from onyx.connectors.exceptions import ConnectorValidationError
        
        # Validate connector type
        valid_types = [e.value for e in WEB_CONNECTOR_VALID_SETTINGS]
        if self.web_connector_type not in valid_types:
            raise ConnectorValidationError(
                f"Invalid connector type '{self.web_connector_type}'. "
                f"Must be one of: {valid_types}"
            )
        
        # Validate crawler mode
        valid_modes = ["adaptive", "static", "dynamic"]
        if self.crawler_mode not in valid_modes:
            raise ConnectorValidationError(
                f"Invalid crawler mode '{self.crawler_mode}'. "
                f"Must be one of: {valid_modes}"
            )
        
        # Validate batch size
        if self.batch_size <= 0:
            raise ConnectorValidationError("Batch size must be positive")
        if self.batch_size > 1000:
            logger.warning(f"Large batch size ({self.batch_size}) may cause memory issues")
        
        # Validate selector configuration
        if self.selector_config and self.selector_config_file:
            raise ConnectorValidationError(
                "Cannot specify both selector_config and selector_config_file"
            )
        
        # Validate file upload constraints
        if self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            from shared_configs.configs import MULTI_TENANT
            if MULTI_TENANT:
                raise ConnectorValidationError(
                    "Upload input for web connector is not supported in cloud environments"
                )
    
    async def validate_connector_settings_async(self) -> None:
        """
        Comprehensive async validation including network checks.
        
        This performs more thorough validation that requires async operations
        like network connectivity checks and URL accessibility.
        """
        # First run basic sync validation
        self.validate_connector_settings()
        
        # Async-specific validations
        await self._validate_network_connectivity()
        await self._validate_url_accessibility()
        await self._validate_authentication_async()
    
    async def _validate_network_connectivity(self) -> None:
        """Validate network connectivity and DNS resolution."""
        from onyx.connectors.exceptions import ConnectorValidationError
        
        try:
            # Basic URL validation with protected URL check
            protected_url_check(self.base_url)
        except ValueError as e:
            raise ConnectorValidationError(f"URL validation failed: {e}")
        except ConnectionError as e:
            raise ConnectorValidationError(f"Network connectivity issue: {e}")
    
    async def _validate_url_accessibility(self) -> None:
        """Validate that the base URL is accessible."""
        from onyx.connectors.exceptions import ConnectorValidationError
        
        if self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            # For upload type, validate file exists
            import os
            if not os.path.exists(self.base_url):
                raise ConnectorValidationError(f"Upload file not found: {self.base_url}")
            return
        
        try:
            # Test basic connectivity to the URL
            response = await self.http_client.head(self.base_url, timeout=10.0)
            
            # Check for obvious issues
            if response.status_code == 401:
                logger.warning(f"Authentication may be required for {self.base_url}")
            elif response.status_code == 403:
                logger.warning(f"Access forbidden for {self.base_url}")
            elif response.status_code >= 400:
                logger.warning(f"URL {self.base_url} returned status {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Could not validate URL accessibility for {self.base_url}: {e}")
            # Don't fail validation for accessibility issues, just warn
    
    async def _validate_authentication_async(self) -> None:
        """Validate authentication configuration if present."""
        if not self.oauth_token:
            return
        
        # Test OAuth token validity by making a simple request
        try:
            test_response = await self.http_client.head(self.base_url, timeout=5.0)
            logger.info("OAuth authentication appears to be working")
        except Exception as e:
            logger.warning(f"OAuth token validation failed: {e}")
            # Don't fail validation, just warn about potential auth issues
    
    @property
    def failed_urls(self) -> dict[str, dict[str, Any]]:
        """Get failed URLs (for compatibility with existing interface)."""
        return self._failed_urls.copy()
    
    async def get_crawl_metrics(self) -> dict[str, Any]:
        """
        Get current crawl metrics for monitoring and reporting.
        
        Returns:
            Dictionary containing crawl metrics and statistics
        """
        return await self._get_metrics_snapshot()
    
    @property
    def metrics(self) -> dict[str, Any]:
        """
        Synchronous access to metrics (may be slightly out of date).
        For real-time metrics, use get_crawl_metrics() instead.
        """
        # Return a copy of metrics without async lock for quick access
        # Note: This might be slightly stale but is useful for compatibility
        return self._metrics.copy()