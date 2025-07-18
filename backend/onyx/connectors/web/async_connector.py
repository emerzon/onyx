"""
Async Web Connector implementation using proper async patterns.
This replaces the sync-over-async anti-pattern in the original connector.
"""

import asyncio
import io
import ipaddress
import re
import shutil
import socket
import tempfile
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, List, Optional, Dict, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    BeautifulSoupCrawler,
    PlaywrightCrawler,
)
from crawlee import ConcurrencySettings
from crawlee.storages import Dataset
from crawlee.request_loaders import SitemapRequestLoader, RequestList

from onyx.configs.app_configs import INDEX_BATCH_SIZE, WEB_CONNECTOR_VALIDATE_URLS
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput, LoadConnector
from onyx.connectors.models import Document, TextSection
from onyx.connectors.web.async_http_client import AsyncHttpClient
from onyx.connectors.web.content_processor import ContentProcessor
from onyx.connectors.web.metrics_collector import MetricsCollector
from onyx.connectors.web.configuration_manager import ConfigurationManager, WebConnectorConfig, WEB_CONNECTOR_TYPE
from onyx.connectors.web.types import ContentType, ProcessedContent, CrawlError
from onyx.file_processing.html_utils import web_html_cleanup, convert_html_to_markdown
from onyx.file_processing.extract_file_text import (
    read_pdf_file,
    docx_to_text_and_images,
    pptx_to_text,
    xlsx_to_text,
)
from onyx.utils.logger import setup_logger

logger = setup_logger()


def detect_content_type(url: str, headers: Optional[Dict[str, str]] = None) -> str:
    """Detect content type from URL and headers."""

    # Define MIME type to content type mappings
    mime_type_mappings = [
        ("application/pdf", "pdf"),
        ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "docx"),
        ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"),
        ("application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx"),
        ("application/msword", "doc"),
        ("application/vnd.ms-excel", "xls"),
        ("application/vnd.ms-powerpoint", "ppt"),
        ("text/plain", "txt"),
        ("text/csv", "csv"),
        ("image/", "image"),  # Partial match for any image type
        ("text/html", "html"),
    ]

    # Define file extension mappings
    extension_mappings = [
        ([".pdf"], "pdf"),
        ([".docx"], "docx"),
        ([".xlsx"], "xlsx"),
        ([".pptx"], "pptx"),
        ([".doc"], "doc"),
        ([".xls"], "xls"),
        ([".ppt"], "ppt"),
        ([".txt", ".text"], "txt"),
        ([".csv"], "csv"),
        ([".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"], "image"),
    ]

    # Check Content-Type header first
    if headers:
        content_type = headers.get("content-type", "").lower()
        for mime_pattern, detected_type in mime_type_mappings:
            if mime_pattern in content_type:
                return detected_type

    # Fall back to URL extension
    url_lower = url.lower()
    for extensions, detected_type in extension_mappings:
        if any(url_lower.endswith(ext) for ext in extensions):
            return detected_type

    return "html"  # Default assumption


# Alias for backward compatibility
WEB_CONNECTOR_VALID_SETTINGS = WEB_CONNECTOR_TYPE


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
        urls = [line.strip() if "://" in line.strip() else f"https://{line.strip()}" for line in f if line.strip()]
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
        # Create configuration object
        self.config = WebConnectorConfig(
            base_url=base_url,
            web_connector_type=web_connector_type,
            batch_size=batch_size,
            mintlify_cleanup=mintlify_cleanup,
            skip_images=skip_images,
            enable_specialized_extraction=enable_specialized_extraction,
            crawler_mode=crawler_mode,
            oauth_token=oauth_token,
            selector_config=selector_config,
            selector_config_file=selector_config_file,
        )

        # Initialize configuration manager
        self.config_manager = ConfigurationManager(self.config)

        # Initialize metrics collector
        self.metrics_collector = MetricsCollector()

        # Legacy properties for backward compatibility
        self.base_url = self.config.base_url
        self.mintlify_cleanup = self.config.mintlify_cleanup
        self.batch_size = self.config.batch_size
        self.web_connector_type = self.config.web_connector_type
        self.crawler_mode = self.config.crawler_mode
        self.skip_images = self.config.skip_images
        self.selector_config = self.config.selector_config
        self.selector_config_file = self.config.selector_config_file
        self.oauth_token = self.config.oauth_token

        # Legacy state management for backward compatibility
        self._documents: list[Document] = []
        self._documents_lock = asyncio.Lock()
        self._failed_urls: dict[str, str] = {}  # Simplified: URL -> error reason
        self._failed_urls_lock = asyncio.Lock()

        # HTTP client for fallback operations (Crawlee handles most HTTP operations)
        self.http_client = AsyncHttpClient(
            oauth_token=oauth_token,
            max_connections=5,  # Further reduced to prevent connection exhaustion
            max_keepalive_connections=3,
            max_retries=1,  # Reduced since Crawlee handles retries
            retry_backoff_factor=0.5,
            timeout=30.0,  # Add explicit timeout
            http2=False,  # Disable HTTP/2 to avoid connection pool issues
        )

        # Initialize request management with Crawlee's native loaders
        self.request_loader = None
        self.request_manager = None
        self.sitemap_loader = None
        self.recursive = False

        # Initialize SessionPool for better session management and anti-blocking
        from crawlee.sessions import SessionPool

        self.session_pool = SessionPool(
            max_pool_size=5,  # Reduced to prevent file descriptor exhaustion
        )

        # Initialize content processor
        self.content_processor = None
        self._setup_content_processor()

    async def __aenter__(self):
        """Async context manager entry."""
        await self.http_client._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with proper cleanup."""
        await self.http_client.close()

        # Persist session state for future runs
        if hasattr(self, "session_pool") and self.session_pool:
            logger.debug("Persisting session pool state")
            # Note: persist_state method may not be available in all versions
            if hasattr(self.session_pool, "persist_state"):
                await self.session_pool.persist_state()

    def _setup_content_processor(self) -> None:
        """Setup content processor with extractors and selectors."""
        # Initialize extractor factory for specialized content extraction
        extractor_factory = None
        if self.config.enable_specialized_extraction:
            try:
                from onyx.connectors.web.extractors import ExtractorFactory

                extractor_factory = ExtractorFactory()
            except ImportError:
                logger.warning("ExtractorFactory not available, using fallback extraction")

        # Initialize selector resolver for generic extraction
        self.selector_resolver = None
        try:
            from onyx.connectors.web.selectors import SelectorResolver

            self.selector_resolver = SelectorResolver()
            if self.config.selector_config or self.config.selector_config_file:
                self._setup_selector_resolver(self.selector_resolver)
        except ImportError:
            logger.warning("SelectorResolver not available, using fallback extraction")

        # Create content processor
        self.content_processor = ContentProcessor(
            mintlify_cleanup=self.config.mintlify_cleanup,
            selector_resolver=self.selector_resolver,
            extractor_factory=extractor_factory,
        )

    def _setup_selector_resolver(self, selector_resolver) -> None:
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
            exclude_patterns = self.config_manager.get_exclude_patterns()

            # Convert string patterns to regex patterns for SitemapRequestLoader
            import re
            from crawlee.http_clients import HttpxHttpClient

            exclude_regexes = (
                [re.compile(pattern.replace("*", ".*")) for pattern in exclude_patterns] if exclude_patterns else None
            )

            # Create HTTP client for sitemap loader with reduced connection limits
            crawlee_http_client = HttpxHttpClient(
                # Configure to prevent connection pool exhaustion
                persist_cookies_per_session=True,
                http2=False,  # Disable HTTP/2 to avoid connection pool issues
                # Pass httpx client kwargs through the async_client_kwargs parameter
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=45.0,  # Match the 45s timeout from logs
                    write=10.0,
                    pool=5.0,
                ),
                limits=httpx.Limits(
                    max_connections=5,  # Further reduced to prevent file descriptor exhaustion
                    max_keepalive_connections=2,
                    keepalive_expiry=15,  # Shorter keepalive to free up resources faster
                ),
            )

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
            if MULTI_TENANT:
                raise ValueError("Upload input for web connector is not supported in cloud environments")
            logger.warning("This is not a UI supported Web Connector flow, are you sure you want to do this?")
            # Read URLs from file and create RequestList
            urls = _read_urls_file(self.base_url)
            self.request_loader = RequestList(requests=urls)
            self.recursive = False

        else:
            raise ValueError(
                f"Invalid Web Connector Config, must choose a valid type: {[e.value for e in WEB_CONNECTOR_VALID_SETTINGS]}"
            )

    def _get_exclude_patterns(self) -> list[str]:
        """Generate URL exclude patterns based on connector configuration."""
        exclude_patterns = []

        # Skip images if configured
        if self.skip_images:
            exclude_patterns.extend(
                [
                    "**/*.png",
                    "**/*.jpg",
                    "**/*.jpeg",
                    "**/*.gif",
                    "**/*.bmp",
                    "**/*.tiff",
                    "**/*.ico",
                    "**/*.svg",
                    "**/*.webp",
                ]
            )

        # Skip unsupported file types
        exclude_patterns.extend(
            [
                "**/*.zip",
                "**/*.rar",
                "**/*.7z",
                "**/*.tar",
                "**/*.gz",
                "**/*.bz2",
                "**/*.mp4",
                "**/*.avi",
                "**/*.mov",
                "**/*.wmv",
                "**/*.flv",
                "**/*.webm",
                "**/*.mp3",
                "**/*.wav",
                "**/*.flac",
                "**/*.ogg",
                "**/*.m4a",
                "**/*.exe",
                "**/*.msi",
                "**/*.deb",
                "**/*.rpm",
                "**/*.dmg",
                "**/*.bin",
                "**/*.iso",
                "**/*.img",
            ]
        )

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
        hash_result = await asyncio.to_thread(lambda: hashlib.sha256(fingerprint_data.encode("utf-8")).hexdigest()[:16])

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
        normalized = " ".join(content.lower().split())

        # Remove common variations that don't affect content meaning
        # Remove extra punctuation and normalize quotes
        normalized = re.sub(r'[""' "‚„‹›«»]", '"', normalized)
        normalized = re.sub(r"[–—−]", "-", normalized)
        normalized = re.sub(r"\s+", " ", normalized)

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

    async def _update_metrics(self, metric_name: str, value: Any = 1, operation: str = "increment") -> None:
        """
        Update crawler metrics in thread-safe manner.

        Args:
            metric_name: Name of the metric to update
            value: Value to add/set (default: 1)
            operation: 'increment', 'set', or 'average'
        """
        async with self._metrics_lock:
            if operation == "increment":
                self._metrics[metric_name] = self._metrics.get(metric_name, 0) + value
            elif operation == "set":
                self._metrics[metric_name] = value
            elif operation == "average":
                # Simple moving average for processing time
                current_avg = self._metrics.get(metric_name, 0.0)
                count = self._metrics.get("pages_processed", 1)
                self._metrics[metric_name] = (current_avg * (count - 1) + value) / count

    async def _increment_error_category(self, category: str) -> None:
        """Increment error count for a specific category."""
        async with self._metrics_lock:
            if "error_categories" not in self._metrics:
                self._metrics["error_categories"] = {}
            self._metrics["error_categories"][category] = self._metrics["error_categories"].get(category, 0) + 1

    async def _get_metrics_snapshot(self) -> dict[str, Any]:
        """Get current metrics snapshot."""
        async with self._metrics_lock:
            snapshot = self._metrics.copy()

            # Calculate derived metrics
            if snapshot["crawl_start_time"]:
                elapsed_time = time.time() - snapshot["crawl_start_time"]
                snapshot["elapsed_time"] = elapsed_time
                if elapsed_time > 0:
                    snapshot["pages_per_second"] = snapshot["pages_processed"] / elapsed_time
                    snapshot["documents_per_second"] = snapshot["documents_created"] / elapsed_time
                else:
                    snapshot["pages_per_second"] = 0
                    snapshot["documents_per_second"] = 0
            else:
                snapshot["elapsed_time"] = 0
                snapshot["pages_per_second"] = 0
                snapshot["documents_per_second"] = 0

            # Calculate success rate
            total_pages = snapshot["pages_processed"]
            if total_pages > 0:
                snapshot["success_rate"] = snapshot["pages_successful"] / total_pages
                snapshot["failure_rate"] = snapshot["pages_failed"] / total_pages
                snapshot["skip_rate"] = snapshot["pages_skipped"] / total_pages
            else:
                snapshot["success_rate"] = 0
                snapshot["failure_rate"] = 0
                snapshot["skip_rate"] = 0

            return snapshot

    async def _log_progress_metrics(self, force: bool = False) -> None:
        """Log current progress metrics."""
        metrics = await self._get_metrics_snapshot()

        # Only log if significant progress or forced
        if force or metrics["pages_processed"] % 50 == 0:
            logger.info(
                f"Crawl Progress - "
                f"Processed: {metrics['pages_processed']} pages, "
                f"Created: {metrics['documents_created']} documents, "
                f"Failed: {metrics['pages_failed']}, "
                f"Skipped: {metrics['pages_skipped']}, "
                f"Rate: {metrics['pages_per_second']:.1f} pages/sec, "
                f"Success: {metrics['success_rate']:.1%}"
            )

            if metrics["error_categories"]:
                error_summary = ", ".join([f"{cat}:{count}" for cat, count in metrics["error_categories"].items()])
                logger.info(f"Error breakdown: {error_summary}")

    async def _add_document(self, document: Document) -> None:
        """Add document to collection in thread-safe manner."""
        async with self._documents_lock:
            self._documents.append(document)

    async def _add_failed_url(self, url: str, error: str) -> None:
        """Add failed URL info in thread-safe manner."""
        async with self._failed_urls_lock:
            self._failed_urls[url] = error

    def _extract_content(self, url: str, soup: BeautifulSoup, raw_html: str) -> tuple[str | None, str, dict[str, Any]]:
        """Extract content using specialized extractors or fallback methods."""
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

    async def _handle_file_extraction_async(self, context) -> Optional[Document]:
        """Unified async handler for PDF and supported binary files."""
        url = context.request.url
        file_content = None
        headers = {}
        try:
            # Get file content and headers
            if hasattr(context, "response") and context.response:
                file_content = context.response.body
                headers = context.response.headers
            else:
                file_content = await self.http_client.get_content(url)
                headers = {}
            if not file_content:
                logger.warning(f"No content retrieved for {url}")
                return None
            # Detect content type
            content_type = detect_content_type(url, headers)
            # PDF special handling
            if content_type == "pdf":
                if not file_content.startswith(b"%PDF"):
                    logger.warning(f"Invalid PDF content for {url} (missing PDF header)")
                    return None
                text_content, metadata, images = await self._extract_pdf_text_async(file_content, url)
            elif content_type in [
                "docx",
                "doc",
                "xlsx",
                "xls",
                "pptx",
                "ppt",
                "txt",
                "csv",
            ]:
                text_content, metadata, images = await self._extract_file_text_async(file_content, content_type, url)
            else:
                logger.warning(f"Unsupported file type {content_type} for {url}")
                return None
            if not text_content or len(text_content.strip()) < 50:
                logger.warning(f"Insufficient content extracted from {content_type} file {url}")
                return None
            # Check for duplicates
            is_duplicate, content_hash = await self.content_processor.is_content_duplicate(None, text_content, url)
            if is_duplicate:
                logger.info(f"Skipping duplicate {content_type} content for {url}")
                return None
            # Get last modified header if available
            doc_updated_at = None
            last_modified = headers.get("Last-Modified")
            if last_modified:
                try:
                    doc_updated_at = (
                        datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc).isoformat()
                    )
                except (ValueError, TypeError):
                    pass
            # Push structured data to Crawlee dataset
            await context.push_data(
                {
                    "id": url,
                    "url": url,
                    "title": url.split("/")[-1],
                    "content": text_content,
                    "source": "web",
                    "content_type": content_type,
                    "metadata": metadata or {},
                    "doc_updated_at": doc_updated_at,
                    "timestamp": time.time(),
                    "content_hash": content_hash,
                    "file_size": len(file_content),
                }
            )
            # Update metrics
            await self.metrics_collector.record_page_processed(success=True, bytes_processed=len(text_content))
            await self.metrics_collector.record_document_created()
            if content_type == "pdf":
                await self._update_metrics("pdfs_processed")
        except Exception as e:
            logger.error(f"Error processing file {url}: {e}")
            await self.metrics_collector.record_page_processed(success=False)
        finally:
            # Aggressive cleanup to prevent file descriptor leaks
            if file_content:
                del file_content

            # Force garbage collection for large files
            if hasattr(context, "response") and context.response:
                try:
                    await context.response.aclose()
                except:
                    pass

            # Explicitly run garbage collection for very large files
            import gc

            gc.collect()

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

        return b"".join(chunks)

    async def _extract_pdf_text_async(self, pdf_content: bytes, url: str) -> tuple[str, dict, list]:
        """
        Extract text from PDF content asynchronously to avoid blocking the event loop.
        """
        import asyncio
        import io

        def extract_text():
            """Run PDF extraction in thread pool to avoid blocking."""
            try:
                return read_pdf_file(file=io.BytesIO(pdf_content))
            except Exception as e:
                logger.error(f"PDF text extraction failed for {url}: {e}")
                return "", {}, []

        # Run PDF extraction in thread pool to avoid blocking event loop
        try:
            # Add timeout to prevent hanging on corrupted PDFs
            page_text, metadata, images = await asyncio.wait_for(
                asyncio.to_thread(extract_text),
                timeout=60.0,  # 60 second timeout
            )
            return page_text, metadata, images
        except asyncio.TimeoutError:
            logger.error(f"PDF text extraction timed out for {url}")
            return "", {}, []

    async def _handle_static_page_async(self, context) -> None:
        """Handle page extraction for static/BeautifulSoup crawler context."""
        url = context.request.url

        try:
            # Validate URL
            protected_url_check(url)

            # Get content from static context (BeautifulSoup provides soup directly)
            soup = context.soup
            raw_html = None

            # Get raw HTML for text extraction
            try:
                if hasattr(context, "response"):
                    raw_html = await context.response.text()
                else:
                    raw_html = str(soup) if soup else None
            except Exception as e:
                logger.warning(f"Could not get raw HTML for {url}: {e}")
                raw_html = str(soup) if soup else None

            if not soup or not raw_html:
                logger.warning(f"No valid content available for {url}")
                return

            # Extract content using the content processor
            title, text_content, metadata = await asyncio.to_thread(self.content_processor.extract_content, url, soup, raw_html)

            # Handle recursive crawling with Crawlee's native automatic discovery
            # Do this BEFORE content validation so links are discovered even if content is insufficient
            if self.recursive:
                # Get exclude patterns based on configuration (e.g., --skip-images)
                exclude_patterns = []
                if self.config.skip_images:
                    image_exts = ("png", "jpg", "jpeg", "gif", "bmp", "tiff", "ico", "svg", "webp")
                    pattern = re.compile(r".*\.(%s)$" % "|".join(image_exts), re.IGNORECASE)
                    exclude_patterns.append(pattern)

                await context.enqueue_links(
                    strategy="same-hostname",
                    limit=100,  # Use Crawlee's native limit instead of manual slicing
                    exclude=exclude_patterns if exclude_patterns else None,
                )

            # Validate content after link discovery
            if not text_content or len(text_content.strip()) < 50:
                logger.warning(f"Insufficient content extracted from {url}")
                return

            # Check for content duplicates
            is_duplicate, content_hash = await self.content_processor.is_content_duplicate(title, text_content, url)
            if is_duplicate:
                logger.info(f"Skipping duplicate content: {url}")
                return

            # Push structured data to Crawlee dataset (must be dict, not object)
            await context.push_data(
                {
                    "id": url,
                    "url": url,
                    "title": title or url,
                    "content": text_content,
                    "source": "web",
                    "content_type": "html",
                    "metadata": metadata or {},
                    "doc_updated_at": None,
                    "timestamp": time.time(),
                    "content_hash": content_hash,
                }
            )
            await self.metrics_collector.record_page_processed(success=True, bytes_processed=len(text_content))
            await self.metrics_collector.record_document_created()

        except Exception as e:
            error_category = self._classify_crawl_error(e, url)
            logger.error(f"Error processing {url}: {e}")
            await self.metrics_collector.increment_error_category(error_category)

    async def _handle_adaptive_page_async(self, context) -> None:
        """Crawlee-native page handler - processes content and pushes to dataset."""
        url = context.request.url

        try:
            # Validate URL
            protected_url_check(url)

            # Get content from adaptive context
            soup = None
            raw_html = None

            # Get raw HTML first, then parse
            try:
                # Get raw HTML for text extraction
                if hasattr(context, "page"):
                    raw_html = await context.page.content()
                elif hasattr(context, "response"):
                    raw_html = await context.response.text()
                else:
                    raw_html = None

                # Parse content
                if hasattr(context, "soup"):
                    # Direct soup access for BeautifulSoup contexts
                    soup = context.soup
                elif raw_html:
                    # Parse HTML manually for adaptive contexts
                    from bs4 import BeautifulSoup

                    soup = BeautifulSoup(raw_html, "html.parser")
                else:
                    # Try adaptive parser as last resort
                    try:
                        if hasattr(context, "parse_with_static_parser"):
                            parsed_result = await context.parse_with_static_parser()
                            if hasattr(parsed_result, "soup"):
                                soup = parsed_result.soup
                            else:
                                soup = parsed_result
                    except Exception as parse_e:
                        logger.debug(f"Static parser failed for {url}: {parse_e}")
                        soup = None
            except Exception as e:
                logger.warning(f"Could not get content for {url}: {e}")
                raw_html = None
                soup = None

            if not soup or not raw_html:
                logger.warning(f"No valid content available for {url}")
                return

            # Process content immediately using content processor
            title, text_content, metadata = await asyncio.to_thread(self.content_processor.extract_content, url, soup, raw_html)

            # Handle recursive crawling with Crawlee's native automatic discovery
            # Do this BEFORE content validation so links are discovered even if content is insufficient
            if self.recursive:
                # Get exclude patterns based on configuration (e.g., --skip-images)
                exclude_patterns = []
                if self.config.skip_images:
                    exclude_patterns.extend(
                        [
                            re.compile(r".*\.png$", re.IGNORECASE),
                            re.compile(r".*\.jpg$", re.IGNORECASE),
                            re.compile(r".*\.jpeg$", re.IGNORECASE),
                            re.compile(r".*\.gif$", re.IGNORECASE),
                            re.compile(r".*\.bmp$", re.IGNORECASE),
                            re.compile(r".*\.tiff$", re.IGNORECASE),
                            re.compile(r".*\.ico$", re.IGNORECASE),
                            re.compile(r".*\.svg$", re.IGNORECASE),
                            re.compile(r".*\.webp$", re.IGNORECASE),
                        ]
                    )

                await context.enqueue_links(
                    strategy="same-hostname",
                    limit=100,  # Use Crawlee's native limit instead of manual slicing
                    exclude=exclude_patterns if exclude_patterns else None,
                )

            # Validate content after link discovery
            if not text_content or len(text_content.strip()) < 50:
                logger.warning(f"Insufficient content extracted from {url}")
                return

            # Check for content duplicates
            is_duplicate, content_hash = await self.content_processor.is_content_duplicate(title, text_content, url)
            if is_duplicate:
                logger.info(f"Skipping duplicate content for {url}")
                return

            # Get response metadata
            doc_updated_at = None
            if hasattr(context, "response") and context.response:
                headers = context.response.headers
                last_modified = headers.get("Last-Modified")
                if last_modified:
                    try:
                        doc_updated_at = (
                            datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc).isoformat()
                        )
                    except (ValueError, TypeError):
                        pass

            # Push structured data to Crawlee dataset
            await context.push_data(
                {
                    "id": url,
                    "url": url,
                    "title": title or url,
                    "content": text_content,
                    "source": "web",
                    "metadata": metadata or {},
                    "doc_updated_at": doc_updated_at,
                    "timestamp": time.time(),
                    "content_hash": content_hash,
                }
            )

            # Update metrics
            await self.metrics_collector.record_page_processed(success=True, bytes_processed=len(text_content))
            await self.metrics_collector.record_document_created()

        except Exception as e:
            if "Page was not crawled with PlaywrightCrawler" in str(e):
                logger.debug(f"AdaptivePlaywrightCrawler internal error for {url}: {e}")
            else:
                logger.error(f"Error processing {url}: {e}")
                await self.metrics_collector.record_page_processed(success=False)

    async def _handle_binary_file_async(self, context) -> None:
        """Deprecated: Use _handle_file_extraction_async instead."""
        await self._handle_file_extraction_async(context)

    async def _monitor_resources(self) -> None:
        """Monitor system resources and log warnings if limits are approached."""
        import resource
        import psutil

        # Get current process
        process = psutil.Process()

        # Get file descriptor limits
        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

        while True:
            try:
                # Check open files
                num_fds = process.num_fds() if hasattr(process, "num_fds") else len(process.open_files())

                # Warn if approaching limit
                if num_fds > soft_limit * 0.8:
                    logger.warning(
                        f"High file descriptor usage: {num_fds}/{soft_limit} ({(num_fds / soft_limit) * 100:.1f}% of limit)"
                    )

                # Log memory usage
                memory_info = process.memory_info()
                logger.debug(f"Resource usage - FDs: {num_fds}/{soft_limit}, Memory: {memory_info.rss / 1024 / 1024:.1f}MB")

                await asyncio.sleep(10)  # Check every 10 seconds
            except Exception as e:
                logger.debug(f"Resource monitoring error: {e}")
                break

    async def _crawl_async(self) -> AsyncGenerator[List[Document], None]:
        """
        Simplified async crawling using Crawlee's native dataset flow.
        Crawlee handles all queuing, deduplication, and batching internally.
        """
        # Start resource monitoring
        monitor_task = asyncio.create_task(self._monitor_resources())

        try:
            # Initialize metrics tracking
            await self.metrics_collector.start_crawl()

            # Initialize request loader using Crawlee's native capabilities
            await self._initialize_request_loader()

            # Create storage directory
            storage_dir = tempfile.mkdtemp(prefix="async_crawlee_storage_")

            # Set storage directory
            import os

            os.environ["CRAWLEE_STORAGE_DIR"] = storage_dir

            # Open dataset - Crawlee will handle all data storage
            dataset = await Dataset.open(name="results")

            # Create crawler based on mode
            crawler = await self._create_crawler()

            # Setup handlers - now they push directly to dataset instead of queue
            await self._setup_crawler_handlers(crawler, None)

            # Start crawling - let Crawlee handle request management
            logger.info("Starting Crawlee crawl...")
            if self.request_manager and hasattr(crawler, "_request_manager") and crawler._request_manager:
                # Using RequestManager (from SitemapRequestLoader.to_tandem())
                logger.info("Using Crawlee RequestManager")
                await crawler.run()
            elif self.request_loader:
                # Use request loader
                if hasattr(self.request_loader, "get_total_count"):
                    # RequestList
                    total_count = await self.request_loader.get_total_count()
                    logger.info(f"Using RequestList with {total_count} requests")
                    requests_to_crawl = []
                    while not await self.request_loader.is_empty():
                        request = await self.request_loader.fetch_next_request()
                        if request:
                            requests_to_crawl.append(request)
                    await crawler.run(requests_to_crawl)
                elif hasattr(self.request_loader, "__iter__"):
                    # Iterable
                    await crawler.run(list(self.request_loader))
                else:
                    # Single request
                    await crawler.run([self.request_loader])
            else:
                # Fallback - single URL
                await crawler.run([self.base_url])

            # Process dataset using Crawlee's native async iteration
            logger.info("Crawling complete, processing dataset...")

            # Debug: Check dataset size
            dataset_info = await dataset.get_info()
            logger.info(f"Dataset info: {dataset_info}")

            documents_processed = 0
            batches_yielded = 0
            batch_documents = []

            # Use Crawlee's native async iteration for optimal memory efficiency
            # Note: FileSystemDatasetClient doesn't support 'clean' parameter
            async for item in dataset.iterate_items():
                # Convert dataset item to Document
                doc = await self._convert_dataset_item_to_document(item)
                if doc:
                    batch_documents.append(doc)
                    documents_processed += 1

                    # Yield batch when full
                    if len(batch_documents) >= self.batch_size:
                        batches_yielded += 1
                        logger.debug(f"Yielding batch {batches_yielded} with {len(batch_documents)} documents")
                        yield batch_documents
                        batch_documents = []

            # Yield final batch if any documents remain
            if batch_documents:
                batches_yielded += 1
                logger.debug(f"Yielding final batch {batches_yielded} with {len(batch_documents)} documents")
                yield batch_documents

            # Final statistics
            logger.info(f"Crawl completed: {documents_processed} total documents processed, {batches_yielded} batches yielded")

            # Log final metrics
            await self.metrics_collector.log_final_metrics()

        finally:
            # Cancel resource monitor
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

            # Clean up storage directory
            shutil.rmtree(storage_dir, ignore_errors=True)

    async def _convert_dataset_item_to_document(self, item: Dict[str, Any]) -> Optional[Document]:
        """Convert a dataset item back to a Document object."""
        try:
            # Extract fields from dataset item
            url = item.get("url") or item.get("id", "")
            title = item.get("title", url)
            content = item.get("content", "")
            metadata = item.get("metadata", {})
            doc_updated_at_str = item.get("doc_updated_at")

            if not content or len(content.strip()) < 50:
                return None

            # Parse doc_updated_at if available
            doc_updated_at = None
            if doc_updated_at_str:
                try:
                    doc_updated_at = datetime.fromisoformat(doc_updated_at_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass

            # Create Document object
            return Document(
                id=url,
                sections=[TextSection(link=url, text=content)],
                source=DocumentSource.WEB,
                semantic_identifier=title,
                metadata=metadata,
                doc_updated_at=doc_updated_at,
            )
        except Exception as e:
            logger.error(f"Error converting dataset item to document: {e}")
            return None

    async def _create_crawler(self):
        """Create crawler based on configured mode with enhanced error handling."""
        # Use reduced concurrency to prevent connection pool exhaustion and file descriptor limits
        concurrency_settings = ConcurrencySettings(
            # Further reduce concurrent requests to prevent file descriptor exhaustion
            desired_concurrency=5,  # Target 5 concurrent requests (further reduced)
            max_concurrency=10,  # Hard limit at 10 (further reduced)
            min_concurrency=1,  # Keep at least 1 active
            max_tasks_per_minute=60,  # Limit rate to prevent overwhelming server
        )

        # Configure statistics with error snapshots for debugging
        from crawlee.statistics import Statistics, StatisticsState

        statistics = Statistics(
            save_error_snapshots=True,  # Save page snapshots on errors for debugging
            state_model=StatisticsState,
        )

        # Use request_manager if available (for sitemap), otherwise use default
        crawler_kwargs = {
            "concurrency_settings": concurrency_settings,
            "session_pool": self.session_pool,  # Add SessionPool for better session management
            "statistics": statistics,  # Add custom statistics with error snapshots
        }
        if self.request_manager:
            crawler_kwargs["request_manager"] = self.request_manager

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
                    from crawlee.crawlers import (
                        RenderingTypePredictor,
                        RenderingTypePrediction,
                    )

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
                                        detection_selectors = getattr(config.detection, "required", [])
                                        if not detection_selectors:
                                            # Handle old format where detection is a list
                                            detection_selectors = config.detection if isinstance(config.detection, list) else []

                                        requires_js = any(
                                            "script" in selector.lower()
                                            or "dynamic" in selector.lower()
                                            or "javascript" in selector.lower()
                                            or "js-" in selector.lower()
                                            for selector in detection_selectors
                                        )
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
                            url = request.url if hasattr(request, "url") else str(request)

                            # If we know this domain needs Playwright, use it directly
                            domain = url.split("/")[2] if "://" in url else ""
                            if domain in self._failed_static_domains:
                                return RenderingTypePrediction(
                                    rendering_type="client_only",
                                    detection_probability_recommendation=0.95,
                                )

                            # Check if framework definitions indicate JS requirement
                            if self._requires_javascript_rendering(url):
                                return RenderingTypePrediction(
                                    rendering_type="client_only",
                                    detection_probability_recommendation=0.8,
                                )

                            # Default to trying static first
                            return RenderingTypePrediction(
                                rendering_type="static",
                                detection_probability_recommendation=0.3,
                            )

                        def store_result(self, request, result):
                            """Learn from failed static parsing attempts"""
                            url = request.url if hasattr(request, "url") else str(request)
                            domain = url.split("/")[2] if "://" in url else ""

                            # If static parsing failed or produced poor results, remember this domain
                            if (hasattr(result, "failed") and result.failed) or (
                                hasattr(result, "content") and len(result.content or "") < 100
                            ):
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
                        crawler_kwargs_no_manager = {k: v for k, v in crawler_kwargs.items() if k != "request_manager"}
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
                logger.info("Creating static crawler with reduced concurrency settings")
                return BeautifulSoupCrawler(
                    request_handler_timeout=timedelta(seconds=45),
                    max_request_retries=2,  # Reduce retries to fail faster
                    # Configure HTTP client to prevent connection issues
                    http_client=HttpxHttpClient(
                        http2=False,  # Disable HTTP/2
                        # Pass httpx settings through async_client_kwargs
                        timeout=httpx.Timeout(
                            connect=10.0,
                            read=45.0,
                            write=10.0,
                            pool=5.0,
                        ),
                        limits=httpx.Limits(
                            max_connections=5,  # Further reduced to prevent file descriptor exhaustion
                            max_keepalive_connections=2,
                            keepalive_expiry=15,  # Shorter keepalive to free up resources faster
                        ),
                    ),
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

    async def _setup_crawler_handlers(self, crawler, raw_items_queue=None):
        """Setup crawler handlers for different content types using Crawlee's dataset flow."""

        @crawler.router.default_handler
        async def async_handler(context) -> None:
            url = context.request.url

            try:
                # Basic URL validation (Crawlee handles most filtering through patterns)
                protected_url_check(url)

                # Handle different content types using Crawlee's native data flow
                if url.lower().endswith(".pdf") or self.config_manager.is_binary_file_supported(url):
                    await self._handle_file_extraction_async(context)
                else:
                    # Route to appropriate handler based on context type
                    if hasattr(context, "soup"):
                        # BeautifulSoup/static crawler context
                        await self._handle_static_page_async(context)
                    else:
                        # Adaptive crawler context
                        await self._handle_adaptive_page_async(context)

            except Exception as e:
                logger.error(f"Handler error for {url}: {e}")

        # Enhanced failed request handler with SessionPool integration
        async def handle_failed_request(context) -> None:
            url = context.request.url
            error_info = getattr(context, "error", None) or getattr(context, "exception", None)
            retry_count = getattr(context.request, "retry_count", 0)

            # Simple error classification for reporting
            error_category, error_reason = self._classify_crawl_error(error_info, url)

            # Enhanced session handling for specific error types
            if hasattr(context, "request") and hasattr(context.request, "session"):
                session = context.request.session
                if session:
                    # Don't retire sessions for authentication errors (allow retry)
                    if error_category == "http_error" and any(code in error_reason for code in ["401", "403"]):
                        session.mark_good()
                        logger.debug(f"Preserving session for auth error: {url}")
                    # Don't retire sessions for rate limiting (temporary issue)
                    elif error_category == "http_error" and "429" in error_reason:
                        session.mark_good()
                        logger.debug(f"Preserving session for rate limit: {url}")

            # Log error (Crawlee handles retry decisions)
            if retry_count > 0:
                logger.warning(f"Failed after retries: {url} - {error_reason}")
            else:
                logger.debug(f"Request failed: {url} - {error_reason}")

            # Track failed URL for user reporting (simplified)
            await self._add_failed_url(url, f"{error_category}: {error_reason}")

            # Update metrics
            await self.metrics_collector.increment_error_category(error_category)

        crawler.failed_request_handler = handle_failed_request

    def _classify_crawl_error(self, error_info: Any, url: str) -> tuple[str, str]:
        """
        Simplified error classification - let Crawlee handle retries.

        Returns:
            Tuple of (error_category, error_reason)
        """
        if not error_info:
            return "unknown", "Unknown error"

        error_msg = str(error_info)

        # Simple classification for reporting purposes
        if "status code:" in error_msg.lower():
            return "http_error", error_msg
        elif any(keyword in error_msg.lower() for keyword in ["timeout", "timed out"]):
            return "timeout", "Connection timeout"
        elif any(keyword in error_msg.lower() for keyword in ["connection", "connect", "network"]):
            return "network_error", "Network error"
        elif "playwright" in error_msg.lower():
            return "browser_error", "Browser error"
        elif "cancelled" in error_msg.lower():
            return "cancelled", "Request cancelled"
        else:
            return "unknown", error_msg[:200]  # Truncate long error messages

    def _is_binary_file(self, url: str) -> bool:
        """Check if URL points to a binary file we can process."""
        supported_extensions = {
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
        }
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in supported_extensions)

    async def _extract_file_text_async(self, content: bytes, content_type: str, url: str) -> Tuple[str, Dict[str, Any], List]:
        """Extract text from various file types asynchronously using existing parsers."""
        try:
            content_io = io.BytesIO(content)

            if content_type in ["docx", "doc"]:
                text, images = await asyncio.to_thread(docx_to_text_and_images, content_io)
                return text, {}, images or []
            elif content_type in ["xlsx", "xls"]:
                text = await asyncio.to_thread(xlsx_to_text, content_io)
                return text, {}, []
            elif content_type in ["pptx", "ppt"]:
                text = await asyncio.to_thread(pptx_to_text, content_io)
                return text, {}, []
            elif content_type in ["txt", "csv"]:
                # Simple text files
                text = content.decode("utf-8", errors="ignore")
                return text, {}, []
            else:
                logger.warning(f"No extraction method for content type: {content_type}")
                return "", {}, []

        except Exception as e:
            logger.error(f"Failed to extract text from {content_type}: {e}")
            return "", {}, []

    def _parse_last_modified(self, headers: Optional[Dict[str, str]]) -> Optional[datetime]:
        """Parse Last-Modified header from response."""
        if not headers:
            return None

        last_modified = headers.get("Last-Modified")
        if not last_modified:
            return None

        try:
            return datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse Last-Modified header: {last_modified}")
            return None

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

        # Always create a new event loop to avoid conflicts
        # This is safer than trying to reuse existing loops
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

        # Use configuration manager for validation
        self.config_manager.validate_or_raise()

    def _validate_basic_settings(self) -> None:
        """Validate basic configuration settings synchronously."""
        from onyx.connectors.exceptions import ConnectorValidationError

        # Validate connector type
        valid_types = [e.value for e in WEB_CONNECTOR_VALID_SETTINGS]
        if self.web_connector_type not in valid_types:
            raise ConnectorValidationError(f"Invalid connector type '{self.web_connector_type}'. Must be one of: {valid_types}")

        # Validate crawler mode
        valid_modes = ["adaptive", "static", "dynamic"]
        if self.crawler_mode not in valid_modes:
            raise ConnectorValidationError(f"Invalid crawler mode '{self.crawler_mode}'. Must be one of: {valid_modes}")

        # Validate batch size
        if self.batch_size <= 0:
            raise ConnectorValidationError("Batch size must be positive")
        if self.batch_size > 1000:
            logger.warning(f"Large batch size ({self.batch_size}) may cause memory issues")

        # Validate selector configuration
        if self.selector_config and self.selector_config_file:
            raise ConnectorValidationError("Cannot specify both selector_config and selector_config_file")

        # Validate file upload constraints
        if self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            if MULTI_TENANT:
                raise ConnectorValidationError("Upload input for web connector is not supported in cloud environments")

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
    def failed_urls(self) -> dict[str, str]:
        """Get failed URLs with error messages."""
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
