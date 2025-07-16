"""
Web Connector implementation using Crawlee library.
This module replaces the Playwright-based web scraping with Crawlee's more robust framework.
"""

import fcntl
import hashlib
import io
import ipaddress
import socket
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncGenerator, List, Optional, cast
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from crawlee.crawlers import AdaptivePlaywrightCrawler
from crawlee import ConcurrencySettings
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.app_configs import WEB_CONNECTOR_OAUTH_CLIENT_ID
from onyx.configs.app_configs import WEB_CONNECTOR_OAUTH_CLIENT_SECRET
from onyx.configs.app_configs import WEB_CONNECTOR_OAUTH_TOKEN_URL
from onyx.configs.app_configs import WEB_CONNECTOR_VALIDATE_URLS
from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import ConnectorValidationError
from onyx.connectors.exceptions import CredentialExpiredError
from onyx.connectors.exceptions import InsufficientPermissionsError
from onyx.connectors.exceptions import UnexpectedValidationError
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.models import Document
from onyx.connectors.models import TextSection
from onyx.file_processing.extract_file_text import read_pdf_file
from onyx.file_processing.html_utils import web_html_cleanup, convert_html_to_markdown
from onyx.utils.logger import setup_logger
from onyx.connectors.web.extractors import ExtractorFactory
from shared_configs.configs import MULTI_TENANT

logger = setup_logger()

class WEB_CONNECTOR_VALID_SETTINGS(str, Enum):
    RECURSIVE = "recursive"
    SINGLE = "single"
    SITEMAP = "sitemap"
    UPLOAD = "upload"

IFRAME_TEXT_LENGTH_THRESHOLD = 700

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


def is_valid_url(url: str) -> bool:
    """Check if a URL is valid."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def _read_urls_file(location: str) -> list[str]:
    """Read URLs from a file."""
    with open(location, "r") as f:
        urls = [line.strip() if "://" in line.strip() else f"https://{line.strip()}" 
                for line in f if line.strip()]
    return urls


def _get_datetime_from_last_modified_header(last_modified: str) -> datetime | None:
    """Parse Last-Modified header into datetime."""
    try:
        return datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return None


class WebConnectorCrawlee(LoadConnector):
    """Web connector implementation using Crawlee library."""
    
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
        
        # Initialize URLs based on connector type
        if web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value:
            self.initial_urls = [self.base_url]
            self.recursive = True
        elif web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SINGLE.value:
            self.initial_urls = [self.base_url]
            self.recursive = False
        elif web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value:
            self.initial_urls = self._extract_sitemap_urls(self.base_url)
            self.recursive = False
        elif web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            if MULTI_TENANT:
                raise ValueError(
                    "Upload input for web connector is not supported in cloud environments"
                )
            logger.warning(
                "This is not a UI supported Web Connector flow, "
                "are you sure you want to do this?"
            )
            self.initial_urls = _read_urls_file(base_url)
            self.recursive = False
        else:
            raise ValueError(
                f"Invalid Web Connector Config, must choose a valid type: "
                f"{[e.value for e in WEB_CONNECTOR_VALID_SETTINGS]}"
            )
        
        # OAuth setup if configured
        self.oauth_token = None
        if (
            WEB_CONNECTOR_OAUTH_CLIENT_ID
            and WEB_CONNECTOR_OAUTH_CLIENT_SECRET
            and WEB_CONNECTOR_OAUTH_TOKEN_URL
        ):
            client = BackendApplicationClient(client_id=WEB_CONNECTOR_OAUTH_CLIENT_ID)
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(
                token_url=WEB_CONNECTOR_OAUTH_TOKEN_URL,
                client_id=WEB_CONNECTOR_OAUTH_CLIENT_ID,
                client_secret=WEB_CONNECTOR_OAUTH_CLIENT_SECRET,
            )
            self.oauth_token = token["access_token"]
        
        # State tracking
        self.content_hashes: set[str] = set()  # Using stable MD5 hashes for content deduplication
        self.documents: list[Document] = []
        self.failed_urls: dict[str, dict[str, Any]] = {}  # Map of failed_url -> {error, source_pages}
        self.link_references: dict[str, set[str]] = {}  # Map of link -> set of source_pages that reference it
        
        # Thread synchronization for file access
        self.file_access_lock = threading.Lock()  # Prevent filesystem contention deadlocks
        
        # Initialize extractor factory for specialized content extraction
        self.enable_specialized_extraction = enable_specialized_extraction
        self.extractor_factory = ExtractorFactory() if enable_specialized_extraction else None
        
        # Initialize selector resolver for generic extraction (always initialize to use framework configs)
        from onyx.connectors.web.selectors import SelectorResolver
        self.selector_resolver = SelectorResolver()
        if selector_config or selector_config_file:
            self._setup_selector_resolver()

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

    def _extract_sitemap_urls(self, sitemap_url: str) -> list[str]:
        """Extract URLs from a sitemap using simple BeautifulSoup parsing."""
        # If URL doesn't end with .xml, try to find the sitemap automatically
        if not sitemap_url.endswith('.xml'):
            sitemap_url = self._discover_sitemap_url(sitemap_url)
        
        try:
            response = requests.get(sitemap_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, "xml")
            loc_tags = soup.find_all("loc")
            
            if not loc_tags:
                raise ValueError(
                    f"No URLs found in sitemap {sitemap_url}. Try using 'single' or 'recursive' scraping instead."
                )
            
            # Check if this is a sitemap index (contains other sitemaps)
            all_urls = []
            for loc_tag in loc_tags:
                url = urljoin(sitemap_url, loc_tag.text) if not urlparse(loc_tag.text).netloc else loc_tag.text
                
                # If URL ends with .xml, it might be a nested sitemap
                if url.endswith('.xml') and url != sitemap_url:
                    logger.info(f"Processing nested sitemap: {url}")
                    try:
                        nested_urls = self._extract_sitemap_urls(url)
                        all_urls.extend(nested_urls)
                    except Exception as e:
                        logger.warning(f"Failed to process nested sitemap {url}: {e}")
                        # Add the sitemap URL itself as fallback
                        all_urls.append(url)
                else:
                    all_urls.append(url)
            
            return all_urls
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch sitemap from {sitemap_url}: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error processing sitemap {sitemap_url}: {e}")

    def _discover_sitemap_url(self, base_url: str) -> str:
        """Discover sitemap URL from base URL by trying common locations."""
        # Ensure base URL ends with /
        if not base_url.endswith('/'):
            base_url += '/'
        
        # Common sitemap locations to try
        sitemap_paths = [
            'sitemap.xml',
            'sitemap_index.xml', 
            'sitemaps.xml',
            'sitemap/sitemap.xml',
            'wp-sitemap.xml',  # WordPress
        ]
        
        for path in sitemap_paths:
            sitemap_url = urljoin(base_url, path)
            try:
                response = requests.head(sitemap_url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"Found sitemap at: {sitemap_url}")
                    return sitemap_url
            except requests.RequestException:
                continue
        
        # If no sitemap found, check robots.txt
        try:
            robots_url = urljoin(base_url, 'robots.txt')
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                for line in response.text.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                        return sitemap_url
        except requests.RequestException:
            pass
        
        # Default fallback
        default_sitemap = urljoin(base_url, 'sitemap.xml')
        logger.info(f"No sitemap found, defaulting to: {default_sitemap}")
        return default_sitemap

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        if credentials:
            logger.warning("Unexpected credentials provided for Web Connector")
        return None
    
    async def _handle_pdf_from_context(self, context) -> Document | None:
        """Handle PDF text extraction from Crawlee context."""
        url = context.request.url
        try:
            # Get PDF content from Crawlee's response
            if hasattr(context, 'response') and context.response:
                pdf_content = context.response.body
            else:
                # Fallback: download PDF directly
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                pdf_content = response.content
            
            # Extract text using our PDF processing function
            page_text, metadata, images = read_pdf_file(file=io.BytesIO(pdf_content))
            
            # Get last modified header if available
            last_modified = None
            if hasattr(context, 'response') and context.response and hasattr(context.response, 'headers'):
                last_modified = context.response.headers.get("Last-Modified")
            
            return Document(
                id=url,
                sections=[TextSection(link=url, text=page_text)],
                source=DocumentSource.WEB,
                semantic_identifier=url.split("/")[-1],
                metadata=metadata,
                doc_updated_at=_get_datetime_from_last_modified_header(last_modified) if last_modified else None,
            )
        except Exception as e:
            logger.error(f"Failed to process PDF {url}: {e}")
            return None



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



    async def _extract_iframe_content(self, page: Any, parsed_html: Any) -> None:
        """Extract content from iframes."""
        try:
            iframe_count = await page.locator("iframe").count()
            if iframe_count > 0:
                iframe_texts = []
                for i in range(iframe_count):
                    try:
                        frame = page.frame_locator("iframe").nth(i)
                        text = await frame.locator("body").inner_text()
                        iframe_texts.append(text)
                    except Exception as e:
                        logger.debug(f"Failed to extract iframe {i}: {e}")
                
                if iframe_texts:
                    combined_iframe_text = "\n".join(iframe_texts)
                    if len(parsed_html.cleaned_text) < IFRAME_TEXT_LENGTH_THRESHOLD:
                        parsed_html.cleaned_text = combined_iframe_text
                    else:
                        parsed_html.cleaned_text += "\n" + combined_iframe_text
        except Exception as e:
            logger.debug(f"Error extracting iframe content: {e}")

    def load_from_state(self) -> GenerateDocumentsOutput:
        """Main entry point for document generation using Crawlee with incremental batching."""
        import asyncio
        from crawlee.storages import Dataset
        import tempfile
        import shutil
        import threading
        import time
        
        # Create a unique storage directory for this run
        storage_dir = tempfile.mkdtemp(prefix="crawlee_storage_")
        
        # Track yielded documents to avoid duplicates
        yielded_files = set()
        crawl_complete = threading.Event()
        shutdown_requested = threading.Event()
        # Accumulate documents for proper batching
        accumulated_batch = []
        # Track last yield time for time-based yielding
        last_yield_time = time.time()
        # Track last known file count for efficient new file discovery
        last_file_count = 0
        
        async def crawl() -> None:
            # Set storage directory using environment variable
            import os
            os.environ['CRAWLEE_STORAGE_DIR'] = storage_dir
            
            # Open the dataset for storing results
            dataset = await Dataset.open(name='results')
            
            # Create crawler based on mode
            if self.crawler_mode == "adaptive":
                # Use AdaptivePlaywrightCrawler with BeautifulSoup static parser
                # This automatically determines when to use JavaScript vs static parsing
                crawler = AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
                    playwright_crawler_specific_kwargs={
                        "browser_launch_options": {
                            "args": [
                                "--no-sandbox", 
                                "--disable-setuid-sandbox",
                                "--disable-dev-shm-usage"
                            ]
                        }},
                    concurrency_settings=ConcurrencySettings(
                        max_concurrency=5,  # Reduced from 10 to minimize deadlock risk
                        desired_concurrency=3,  # Reduced from 5 to minimize deadlock risk
                        min_concurrency=1
                    ),
                )
            elif self.crawler_mode == "static":
                # Use BeautifulSoupCrawler for static content only
                from crawlee.crawlers import BeautifulSoupCrawler
                crawler = BeautifulSoupCrawler(
                    concurrency_settings=ConcurrencySettings(
                        max_concurrency=5,  # Reduced from 10 to minimize deadlock risk
                        desired_concurrency=3,  # Reduced from 5 to minimize deadlock risk
                        min_concurrency=1
                    ),
                    # Configure Crawlee's built-in timeout and error handling
                    request_handler_timeout=45,  # 45 second timeout per request
                    max_requests_per_crawl=None,  # No artificial limits
                    max_request_retries=2,  # Limit retries to prevent infinite loops
                )
            else:  # dynamic
                # Use PlaywrightCrawler for dynamic content only
                from crawlee.crawlers import PlaywrightCrawler
                crawler = PlaywrightCrawler(
                    browser_launch_options={
                        "args": [
                            "--no-sandbox", 
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage"
                        ]
                    },
                    concurrency_settings=ConcurrencySettings(
                        max_concurrency=5,  # Reduced from 10 to minimize deadlock risk
                        desired_concurrency=3,  # Reduced from 5 to minimize deadlock risk
                        min_concurrency=1
                    ),
                )
            
            # Helper functions for file type detection
            def is_supported_document_type(url: str) -> bool:
                """Check if URL points to a supported document type that can be processed."""
                supported_extensions = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'}
                url_lower = url.lower()
                return any(url_lower.endswith(ext) for ext in supported_extensions)
            
            def should_store_as_base64(url: str) -> bool:
                """Check if URL should be stored as base64 for later processing."""
                return is_supported_document_type(url)
            
            # URL filtering to skip non-HTTP URLs and binary files
            def should_skip_url(url: str) -> bool:
                """Check if URL should be skipped based on scheme or file extension."""
                if not url.startswith(('http://', 'https://')):
                    return True
                if any(url.startswith(prefix) for prefix in ['mailto:', 'tel:', 'javascript:', 'data:']):
                    return True
                
                url_lower = url.lower()
                
                # Skip images if requested
                if self.skip_images:
                    image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.ico', '.svg', '.webp'}
                    if any(url_lower.endswith(ext) for ext in image_extensions):
                        return True
                
                # Skip truly unsupported binary file extensions
                # Note: PDF, DOCX, XLS, PPT will be handled as base64 and processed during yielding
                unsupported_extensions = {
                    # Archives (would need special handling)
                    '.zip', '.rar', '.7z', '.tar', '.gz', '.bz2',
                    # Videos (no text content)
                    '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm',
                    # Audio (no text content)
                    '.mp3', '.wav', '.flac', '.ogg', '.m4a',
                    # Executables (no text content)
                    '.exe', '.msi', '.deb', '.rpm', '.dmg',
                    # Other binary (no text content)
                    '.bin', '.iso', '.img'
                }
                
                # Always skip these unsupported extensions
                if any(url_lower.endswith(ext) for ext in unsupported_extensions):
                    return True
                
                # If skip_images is False, images will be processed (but not extracted as text)
                if not self.skip_images:
                    image_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.tiff', '.ico', '.svg', '.webp'}
                    if any(url_lower.endswith(ext) for ext in image_extensions):
                        # Images will be downloaded but not processed for text content
                        return False
                        
                return False
            
            # Override the request handler to filter URLs
            original_run_request_handler = crawler._run_request_handler
            
            async def filtered_run_request_handler(context):
                if should_skip_url(context.request.url):
                    logger.debug(f"Skipping non-HTTP/binary URL: {context.request.url}")
                    return
                return await original_run_request_handler(context)
            
            crawler._run_request_handler = filtered_run_request_handler
            
            # Optional stealth hook (Crawlee provides good defaults)
            @crawler.pre_navigation_hook
            async def pre_navigation_hook(context) -> None:
                if hasattr(context, 'page') and context.page:
                    # Minimal stealth - Crawlee handles most anti-bot measures automatically
                    await context.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            
            # Add failure handler to track failed requests
            async def handle_failed_request(context) -> None:
                """Handle failed requests and track them."""
                url = context.request.url
                logger.info(f"BROKEN_LINK_TRACKER: Processing failed request for {url}")
                
                error_info = getattr(context, 'error', None) or getattr(context, 'exception', None)
                
                if error_info:
                    error_msg = str(error_info)
                    # Extract status code from error message if available
                    if "status code:" in error_msg:
                        status_code = error_msg.split("status code:")[1].strip().rstrip(")")
                        error_reason = f"HTTP {status_code}"
                    elif "ConnectTimeout" in error_msg:
                        error_reason = "Connection timeout"
                    elif "ConnectError" in error_msg:
                        error_reason = "Connection error"
                    elif "Network is unreachable" in error_msg:
                        error_reason = "Network unreachable"
                    else:
                        error_reason = error_msg
                else:
                    error_reason = "Unknown error"
                
                # Get source pages that referenced this failed URL
                source_pages = list(self.link_references.get(url, set()))
                
                # Store failed URL with error reason and source pages
                self.failed_urls[url] = {
                    "error": error_reason,
                    "source_pages": source_pages
                }
                
                logger.warning(f"Failed to process {url}: {error_reason}")
                if source_pages:
                    logger.info(f"Referenced by {len(source_pages)} page(s): {', '.join(source_pages[:3])}{'...' if len(source_pages) > 3 else ''}")
                else:
                    logger.info(f"No source pages tracked for {url} (may be initial URL or external reference)")
            
            # Set the error handler on the crawler
            crawler.failed_request_handler = handle_failed_request
            
            # Define the handler based on crawler mode
            if self.crawler_mode == "static":
                @crawler.router.default_handler
                async def static_handler(context) -> None:
                    url = context.request.url
                    
                    # Check if URL is PDF - static crawler can't handle PDFs properly
                    if url.lower().endswith(".pdf"):
                        logger.warning(f"Skipping PDF {url} in static mode - use adaptive or dynamic mode for PDFs")
                        return
                    
                    # Check content type if available
                    if hasattr(context, 'response') and context.response:
                        content_type = context.response.headers.get('content-type', '').lower()
                        if any(ct in content_type for ct in ['image/', 'video/', 'audio/', 'application/octet-stream']):
                            logger.debug(f"Skipping binary content type {content_type} for {url}")
                            return
                    
                    # Check if this is a supported document type that should be stored as base64
                    if should_store_as_base64(url):
                        await self._handle_binary_file_with_dataset(context, dataset)
                    else:
                        # Handle regular web pages with BeautifulSoup
                        await self._handle_adaptive_page_with_dataset(context, dataset)
            
            elif self.crawler_mode == "dynamic":
                @crawler.router.default_handler
                async def dynamic_handler(context) -> None:
                    url = context.request.url
                    
                    try:
                        # Add timeout protection to prevent deadlocks
                        async with asyncio.timeout(45):  # 45 second timeout to prevent 60s deadlock
                            # Check if URL is PDF and handle text extraction
                            if url.lower().endswith(".pdf"):
                                doc = await self._handle_pdf_from_context(context)
                                if doc:
                                    # Store document data in dataset
                                    await dataset.push_data({
                                        'id': doc.id,
                                        'sections': [{'link': s.link, 'text': s.text} for s in doc.sections],
                                        'source': doc.source.value if hasattr(doc.source, 'value') else str(doc.source),
                                        'semantic_identifier': doc.semantic_identifier,
                                        'metadata': doc.metadata,
                                        'doc_updated_at': doc.doc_updated_at.isoformat() if doc.doc_updated_at else None,
                                    })
                                return
                            
                            # Check if this is a supported document type that should be stored as base64
                            if should_store_as_base64(url):
                                await self._handle_binary_file_with_dataset(context, dataset)
                            else:
                                # Handle regular web pages with Playwright
                                await self._handle_adaptive_page_with_dataset(context, dataset)
                    except asyncio.TimeoutError:
                        logger.warning(f"Handler timeout for {url} - preventing deadlock")
                        return
                    except Exception as e:
                        logger.error(f"Handler error for {url}: {e}")
                        return
            
            else:  # adaptive
                @crawler.router.default_handler
                async def adaptive_handler(context) -> None:
                    url = context.request.url
                    
                    try:
                        # Add timeout protection to prevent deadlocks
                        async with asyncio.timeout(45):  # 45 second timeout to prevent 60s deadlock
                            # Check if URL is PDF and handle text extraction
                            if url.lower().endswith(".pdf"):
                                doc = await self._handle_pdf_from_context(context)
                                if doc:
                                    # Store document data in dataset
                                    await dataset.push_data({
                                        'id': doc.id,
                                        'sections': [{'link': s.link, 'text': s.text} for s in doc.sections],
                                        'source': doc.source.value if hasattr(doc.source, 'value') else str(doc.source),
                                        'semantic_identifier': doc.semantic_identifier,
                                        'metadata': doc.metadata,
                                        'doc_updated_at': doc.doc_updated_at.isoformat() if doc.doc_updated_at else None,
                                    })
                                return
                            
                            # Check if this is a supported document type that should be stored as base64
                            if should_store_as_base64(url):
                                await self._handle_binary_file_with_dataset(context, dataset)
                            else:
                                # Handle regular web pages
                                await self._handle_adaptive_page_with_dataset(context, dataset)
                    except asyncio.TimeoutError:
                        logger.warning(f"Handler timeout for {url} - preventing deadlock")
                        return
                    except Exception as e:
                        logger.error(f"Handler error for {url}: {e}")
                        return
            
            # Run the crawler with initial URLs
            await crawler.run(self.initial_urls)
        
        def run_crawler():
            """Run the crawler in a separate thread."""
            try:
                asyncio.run(crawl())
            except Exception as e:
                logger.error(f"Crawler thread encountered error: {e}")
                # Don't re-raise to prevent thread from hanging
            finally:
                crawl_complete.set()
        
        # Start crawling in background thread
        crawler_thread = threading.Thread(target=run_crawler)
        crawler_thread.start()
        
        # Yield batches incrementally as they become available
        try:
            while not crawl_complete.is_set() or self._has_new_data(storage_dir, last_file_count):
                try:
                    # Check for new data and accumulate into batch
                    new_docs, last_file_count = self._get_new_documents(storage_dir, yielded_files, last_file_count)
                    accumulated_batch.extend(new_docs)
                    
                    current_time = time.time()
                    time_since_last_yield = current_time - last_yield_time
                    
                    # Yield conditions:
                    # 1. When we have enough documents for a full batch
                    # 2. When 60 seconds have passed and we have at least 1 document
                    should_yield_full_batch = len(accumulated_batch) >= self.batch_size
                    should_yield_time_based = time_since_last_yield >= 60.0 and len(accumulated_batch) >= 1
                    
                    if should_yield_full_batch:
                        # Yield full batches when we have enough documents
                        while len(accumulated_batch) >= self.batch_size:
                            batch_to_yield = accumulated_batch[:self.batch_size]
                            accumulated_batch = accumulated_batch[self.batch_size:]
                            last_yield_time = time.time()
                            logger.info(f"Yielding incremental batch of {len(batch_to_yield)} documents (batch size reached)")
                            yield batch_to_yield
                    elif should_yield_time_based:
                        # Yield partial batch after 60 seconds if we have documents
                        batch_to_yield = accumulated_batch[:]
                        accumulated_batch = []
                        last_yield_time = time.time()
                        logger.info(f"Yielding time-based batch of {len(batch_to_yield)} documents (60s timeout)")
                        yield batch_to_yield
                    
                    # If crawling is still active, wait briefly before checking again
                    # Increased sleep time to reduce filesystem contention and prevent deadlocks
                    if not crawl_complete.is_set():
                        time.sleep(3.0)  # Check every 3 seconds instead of 1 second
                
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt received, starting graceful shutdown...")
                    shutdown_requested.set()
                    break
                except Exception as e:
                    logger.error(f"Error in yielding loop: {e}")
                    # Continue processing despite errors
                    time.sleep(3.0)  # Increased sleep time to reduce contention
            
            # Yield any remaining documents as a final partial batch
            if accumulated_batch:
                logger.info(f"Yielding final partial batch of {len(accumulated_batch)} documents")
                yield accumulated_batch
                    
        finally:
            # Ensure crawler thread completes with timeout to prevent deadlocks
            logger.info("Waiting for crawler thread to complete...")
            crawler_thread.join(timeout=30.0)  # 30 second timeout
            if crawler_thread.is_alive():
                logger.warning("Crawler thread did not complete within timeout, proceeding with cleanup")
            else:
                logger.info("Crawler thread completed successfully")
            
            # Clean up storage directory
            shutil.rmtree(storage_dir, ignore_errors=True)
    
    def _has_new_data(self, storage_dir: str, last_file_count: int) -> bool:
        """Check if there are new document files since last check (efficient version)."""
        from pathlib import Path
        
        # Use file access lock to prevent deadlocks with crawler thread
        with self.file_access_lock:
            dataset_path = self._find_dataset_path(storage_dir)
            if not dataset_path or not dataset_path.exists():
                return False
                
            # Count JSON files (excluding metadata) - more efficient than creating sets
            current_file_count = len([f for f in dataset_path.glob("*.json") if not f.name.startswith("__")])
            
            # Check if we have more files than before
            return current_file_count > last_file_count
    
    def _get_new_documents(self, storage_dir: str, yielded_files: set, last_file_count: int) -> tuple[list[Document], int]:
        """Get new documents from storage directory, only processing files added since last check."""
        import json
        from pathlib import Path
        
        # Use file access lock to prevent deadlocks with crawler thread
        with self.file_access_lock:
            dataset_path = self._find_dataset_path(storage_dir)
            if not dataset_path or not dataset_path.exists():
                return [], last_file_count
            
            # Get all JSON files sorted by name (Crawlee creates them with sequential numbers)
            all_json_files = sorted([f for f in dataset_path.glob("*.json") if not f.name.startswith("__")])
            current_file_count = len(all_json_files)
            
            # Only process files beyond what we've seen before
            if current_file_count <= last_file_count:
                return [], current_file_count
            
            # Get only the new files (files beyond the last known count)
            new_files = all_json_files[last_file_count:]
            
            new_documents = []
            
            for json_file in new_files:
                # Skip if already processed (safety check)
                if json_file.name in yielded_files:
                    continue
                    
                # Retry logic for file operations
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            # Add OS-level file locking to prevent corruption
                            try:
                                fcntl.flock(f.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)  # Non-blocking shared lock
                            except OSError:
                                # File is being written, retry after brief delay
                                if attempt < max_retries - 1:
                                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                                    continue
                                else:
                                    logger.warning(f"Could not acquire lock for {json_file}, skipping")
                                    break
                            
                            content = f.read().strip()
                            if not content:
                                logger.warning(f"Skipping empty JSON file: {json_file}")
                                yielded_files.add(json_file.name)  # Mark as processed
                                break
                            
                            doc_data = json.loads(content)
                            # Lock automatically released when file closes
                            break
                    except (IOError, OSError) as e:
                        if attempt < max_retries - 1:
                            logger.debug(f"File access error for {json_file}, retrying: {e}")
                            time.sleep(0.1 * (attempt + 1))
                            continue
                        else:
                            logger.error(f"Failed to read {json_file} after {max_retries} attempts: {e}")
                            continue
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in {json_file}: {e}")
                        yielded_files.add(json_file.name)  # Mark as processed to avoid retry
                        break
                else:
                    # If we exhausted retries, skip this file
                    continue
                
                # Process the successfully read document
                try:
                    # Check if this is a base64 file that needs processing
                    if doc_data.get('metadata', {}).get('is_base64_file', False):
                        processed_doc = self._process_base64_file(doc_data)
                        if processed_doc:
                            new_documents.append(processed_doc)
                            self.documents.append(processed_doc)
                            yielded_files.add(json_file.name)
                        continue
                    
                    # Validate and reconstruct sections with proper text handling
                    sections = []
                    for s in doc_data['sections']:
                        text = s.get('text')
                        if text is None or text == "":
                            logger.warning(f"Skipping section with None/empty text for document {doc_data['id']}")
                            continue
                        sections.append(TextSection(link=s['link'], text=str(text)))
                    
                    # Skip document if no valid sections
                    if not sections:
                        logger.warning(f"Skipping document {doc_data['id']} - no valid sections with text content")
                        yielded_files.add(json_file.name)  # Mark as processed to avoid re-processing
                        continue
                    
                    # Reconstruct Document object from stored data
                    doc = Document(
                        id=doc_data['id'],
                        sections=sections,
                        source=DocumentSource.WEB,
                        semantic_identifier=doc_data['semantic_identifier'],
                        metadata=doc_data.get('metadata', {}),
                        doc_updated_at=datetime.fromisoformat(doc_data['doc_updated_at']) if doc_data.get('doc_updated_at') else None,
                    )
                    
                    new_documents.append(doc)
                    self.documents.append(doc)  # Keep for compatibility
                    yielded_files.add(json_file.name)  # Mark as processed
                        
                except Exception as e:
                    logger.error(f"Error processing document from {json_file}: {e}")
                    yielded_files.add(json_file.name)  # Mark as processed even if failed
                    continue
            
            return new_documents, current_file_count
    
    def _find_dataset_path(self, storage_dir: str):
        """Find the actual dataset path in the storage directory."""
        from pathlib import Path
        
        # Try the named dataset first
        dataset_path = Path(storage_dir) / "datasets" / "results"
        if dataset_path.exists():
            return dataset_path
            
        # Check if datasets directory exists but with different name (e.g., default)
        datasets_dir = Path(storage_dir) / "datasets"
        if datasets_dir.exists():
            available_datasets = list(datasets_dir.glob("*"))
            # Check the default dataset first
            default_dataset_path = datasets_dir / "default"
            if default_dataset_path.exists():
                return default_dataset_path
            else:
                # Use the first available dataset
                if available_datasets:
                    return available_datasets[0]
        
        return None
    
    async def _handle_binary_file_with_dataset(self, context, dataset) -> None:
        """Handle binary file by storing as base64 for later processing."""
        import base64
        import httpx
        
        url = context.request.url
        
        try:
            # Download the file content
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                
                # Encode as base64
                base64_content = base64.b64encode(response.content).decode('utf-8')
                
                # Store with special metadata to indicate it's base64
                await dataset.push_data({
                    'id': url,
                    'sections': [{'link': url, 'text': base64_content}],
                    'source': 'web',
                    'semantic_identifier': url,
                    'metadata': {
                        'is_base64_file': True,
                        'content_type': response.headers.get('content-type', ''),
                        'file_size': len(response.content)
                    },
                    'doc_updated_at': None,
                })
                
                logger.info(f"Stored binary file as base64: {url}")
                
        except Exception as e:
            logger.error(f"Error handling binary file {url}: {e}")

    def _process_base64_file(self, doc_data: dict) -> Optional[Document]:
        """Process a base64 file and convert to text using existing file processors."""
        import base64
        import tempfile
        import os
        from onyx.file_processing.extract_file_text import extract_file_text
        
        url = doc_data['id']
        base64_content = doc_data['sections'][0]['text']
        
        try:
            # Decode base64 content
            file_content = base64.b64decode(base64_content)
            
            # Determine file extension from URL
            file_extension = None
            for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']:
                if url.lower().endswith(ext):
                    file_extension = ext
                    break
            
            if not file_extension:
                logger.warning(f"Could not determine file extension for {url}")
                return None
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp_file:
                tmp_file.write(file_content)
                tmp_file_path = tmp_file.name
            
            try:
                # Extract text using existing file processing
                text_content = extract_file_text(tmp_file_path)
                
                if not text_content or text_content.strip() == "":
                    logger.warning(f"No text content extracted from {url}")
                    return None
                
                # Create document with extracted text
                doc = Document(
                    id=url,
                    sections=[TextSection(link=url, text=text_content.strip())],
                    source=DocumentSource.WEB,
                    semantic_identifier=url,
                    metadata={
                        'extracted_from_file': True,
                        'original_content_type': doc_data.get('metadata', {}).get('content_type', ''),
                        'file_size': doc_data.get('metadata', {}).get('file_size', 0)
                    },
                    doc_updated_at=None,
                )
                
                logger.info(f"Successfully processed {file_extension} file: {url}")
                return doc
                
            finally:
                # Clean up temporary file
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
                    
        except Exception as e:
            logger.error(f"Error processing base64 file {url}: {e}")
            return None

    async def _handle_adaptive_page_with_dataset(self, context, dataset) -> None:
        """Handle page processing and store results in dataset."""
        url = context.request.url
        
        try:
            # Validate URL
            protected_url_check(url)
            
            # Get content from adaptive context (Crawlee handles Playwright vs static automatically)
            if hasattr(context, 'page') and context.page is not None:
                logger.info(f"Processing with Playwright: {url}")
                raw_html = await context.page.content()
                soup = BeautifulSoup(raw_html, "html.parser")
            elif hasattr(context, 'soup') and context.soup is not None:
                logger.info(f"Processing with static parser: {url}")
                soup = context.soup
                raw_html = str(soup)
            else:
                logger.warning(f"No page or soup available for {url}")
                return
            
            # Extract content using specialized extractors or fallback
            title, text_content, metadata, extracted_links = self._extract_content(url, soup, raw_html)
            
            # Track link references for broken link reporting (thread-safe)
            if extracted_links:
                def update_link_references():
                    for link in extracted_links:
                        if link not in self.link_references:
                            self.link_references[link] = set()
                        self.link_references[link].add(url)
                
                # Run in executor to prevent async lock contention
                import asyncio
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, update_link_references)
            
            # Add discovered links for recursive crawling (even if content extraction fails)
            if self.recursive:
                # Use extracted links if available, otherwise fall back to Crawlee's automatic discovery
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
            
            # Ensure text content is a string
            text_content = str(text_content)
            
            # Check for duplicate content using stable hash
            content_key = f"{title}:{text_content}"
            content_hash = hashlib.md5(content_key.encode("utf-8")).hexdigest()
            
            if content_hash in self.content_hashes:
                logger.info(f"Skipping duplicate content for {url}")
                return
            
            self.content_hashes.add(content_hash)
            
            # Get last modified header
            last_modified = getattr(context.response, 'headers', {}).get("Last-Modified") if hasattr(context, 'response') else None
            
            # Store document data in dataset
            await dataset.push_data({
                'id': url,
                'sections': [{'link': url, 'text': text_content}],
                'source': DocumentSource.WEB.value,
                'semantic_identifier': title or url,
                'metadata': metadata,
                'doc_updated_at': _get_datetime_from_last_modified_header(last_modified).isoformat() if last_modified else None,
            })
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
    

    def validate_connector_settings(self) -> None:
        """Validate connector configuration and test connectivity."""
        # Make sure we have URLs configured
        if not self.initial_urls:
            raise ConnectorValidationError(
                "No URLs configured. Please provide at least one valid URL."
            )

        if (
            self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value
            or self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value
        ):
            return None

        # For validation, test the base URL
        test_url = self.base_url

        # Check that the URL is allowed and well-formed
        try:
            protected_url_check(test_url)
        except ValueError as e:
            raise ConnectorValidationError(
                f"Protected URL check failed for '{test_url}': {e}"
            )
        except ConnectionError as e:
            # Typically DNS or other network issues
            raise ConnectorValidationError(str(e))

        # Make a quick request to see if we get a valid response (let Crawlee handle detailed validation)
        try:
            response = requests.get(test_url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else 0
            if status_code == 401:
                raise CredentialExpiredError(
                    f"Unauthorized access to '{test_url}': {e}"
                )
            elif status_code == 403:
                raise InsufficientPermissionsError(
                    f"Forbidden access to '{test_url}': {e}"
                )
            elif status_code == 404:
                raise ConnectorValidationError(f"Page not found for '{test_url}': {e}")
            else:
                raise UnexpectedValidationError(
                    f"HTTP error validating '{test_url}': {e}"
                )
        except requests.exceptions.RequestException as e:
            if "NameResolutionError" in str(e):
                raise ConnectorValidationError(
                    f"Unable to resolve hostname for '{test_url}'. Please check the URL and your internet connection."
                )
            else:
                raise UnexpectedValidationError(
                    f"Network error validating '{test_url}': {e}"
                )


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Adaptive Web Connector - Extract content from websites with automatic JavaScript detection and specialized framework support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single page extraction
  python connector.py --url https://example.com --type single
  
  # Recursive crawling (automatically detects JS requirements)
  python connector.py --url https://docs.example.com --type recursive
  
  # Sitemap-based crawling
  python connector.py --url https://example.com/sitemap.xml --type sitemap
  
  # Zoomin documentation with custom batch size
  python connector.py --url https://docs-cybersec.thalesgroup.com/bundle/guide/page/overview.htm --type single --batch-size 5
  
  # Save batches for ingestion API
  python connector.py --url https://docs.example.com --type recursive --save-batches --batch-dir ./ingestion_batches
  
  # Force static crawling (no JavaScript execution)
  python connector.py --url https://simple-site.com --type recursive --crawler-mode static
  
  # Force dynamic crawling (always use Playwright)
  python connector.py --url https://spa-app.com --type single --crawler-mode dynamic
  
  # Skip image files during crawling
  python connector.py --url https://docs.example.com --type recursive --skip-images
        """
    )
    
    parser.add_argument(
        "--url", "-u",
        required=True,
        help="URL to crawl (base URL for recursive, sitemap URL for sitemap, specific page for single)"
    )
    
    parser.add_argument(
        "--type", "-t",
        choices=["single", "recursive", "sitemap", "upload"],
        default="single",
        help="Crawling type (default: single)"
    )
    
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=INDEX_BATCH_SIZE,
        help=f"Number of documents per batch (default: {INDEX_BATCH_SIZE})"
    )
    
    
    parser.add_argument(
        "--no-mintlify",
        action="store_true",
        help="Disable Mintlify-specific cleanup"
    )
    
    parser.add_argument(
        "--no-specialized",
        action="store_true",
        help="Disable specialized extractors (Zoomin, etc.)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file to save extracted content (JSON format)"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)"
    )
    
    parser.add_argument(
        "--save-batches",
        action="store_true",
        help="Save each batch as a separate JSON file for ingestion API"
    )
    
    parser.add_argument(
        "--batch-dir",
        default="./batches",
        help="Directory to save batch files (default: ./batches)"
    )
    
    parser.add_argument(
        "--crawler-mode",
        choices=["adaptive", "static", "dynamic"],
        default="adaptive",
        help="Crawler mode: adaptive (auto-detect JS needs), static (BeautifulSoup only), dynamic (Playwright only). Default: adaptive"
    )
    
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip image files (PNG, JPG, GIF, etc.) during crawling"
    )
    
    # Generic selector configuration options
    parser.add_argument(
        "--selector-config",
        help="Path to JSON/YAML file with selector configuration"
    )
    
    parser.add_argument(
        "--content-selector",
        help="CSS selector for main content extraction"
    )
    
    parser.add_argument(
        "--title-selector",
        help="CSS selector for page title extraction"
    )
    
    parser.add_argument(
        "--links-selector",
        help="CSS selector for navigation links extraction"
    )
    
    parser.add_argument(
        "--exclude-selectors",
        nargs="+",
        help="CSS selectors for elements to exclude from extraction"
    )
    
    parser.add_argument(
        "--detection-selectors",
        nargs="+",
        help="CSS selectors that must be present for generic extraction to activate"
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Configure global timeout for requests
    NETWORK_TIMEOUT = args.timeout
    
    # Setup batch saving if requested
    if args.save_batches:
        import os
        import json
        from datetime import datetime
        
        # Create batch directory
        os.makedirs(args.batch_dir, exist_ok=True)
        print(f"Batch files will be saved to: {args.batch_dir}")
    
    def save_batch_for_ingestion(batch_docs: list, batch_num: int, source_url: str) -> str:
        """Save a batch of documents in ingestion API format."""
        if not args.save_batches:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_{batch_num:03d}_{timestamp}.json"
        filepath = os.path.join(args.batch_dir, filename)
        
        # Convert documents to ingestion API format
        ingestion_docs = []
        for doc in batch_docs:
            # Extract first text section
            text_content = ""
            source_link = None
            if doc.sections:
                text_content = doc.sections[0].text
                source_link = doc.sections[0].link
            
            # Prepare metadata in the expected format
            metadata = doc.metadata.copy() if doc.metadata else {}
            metadata["document_id"] = doc.id
            metadata["source_url"] = source_url
            metadata["batch_number"] = batch_num
            metadata["extraction_timestamp"] = timestamp
            
            # Add extractor info if available
            if "extractor" in metadata:
                metadata["extraction_method"] = metadata["extractor"]
            
            # Create ingestion document
            ingestion_doc = {
                "document": {
                    "id": doc.id,
                    "sections": [
                        {
                            "text": text_content,
                            "link": source_link or doc.id
                        }
                    ],
                    "source": "web",  # DocumentSource.WEB
                    "semantic_identifier": doc.semantic_identifier or doc.id,
                    "metadata": metadata,
                    "from_ingestion_api": True
                }
            }
            
            # Add optional fields if present
            if doc.doc_updated_at:
                ingestion_doc["document"]["doc_updated_at"] = doc.doc_updated_at.isoformat()
            
            ingestion_docs.append(ingestion_doc)
        
        # Save to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(ingestion_docs, f, indent=2, ensure_ascii=False)
            return filepath
        except Exception as e:
            print(f"Error saving batch {batch_num}: {e}")
            return None
    
    # Handle selector configuration from CLI
    selector_config = None
    selector_config_file = None
    
    # Process CLI selector options
    if any([args.selector_config, args.content_selector, args.title_selector, 
            args.links_selector, args.exclude_selectors, args.detection_selectors]):
        
        if args.selector_config:
            selector_config_file = args.selector_config
        
        # Create quick config from individual CLI options
        if any([args.content_selector, args.title_selector, args.links_selector, 
                args.exclude_selectors, args.detection_selectors]):
            
            from onyx.connectors.web.selectors import SelectorResolver
            resolver = SelectorResolver()
            
            quick_config = resolver.create_quick_config(
                name="cli_custom",
                title_selector=args.title_selector,
                content_selector=args.content_selector,
                links_selector=args.links_selector,
                exclude_selectors=args.exclude_selectors,
                detection_selectors=args.detection_selectors
            )
            
            selector_config = quick_config.to_json()
    
    # Create connector with specified parameters
    try:
        connector = WebConnectorCrawlee(
            base_url=args.url,
            web_connector_type=args.type,
            mintlify_cleanup=not args.no_mintlify,
            batch_size=args.batch_size,
            enable_specialized_extraction=not args.no_specialized,
            crawler_mode=args.crawler_mode,
            skip_images=args.skip_images,
            selector_config=selector_config,
            selector_config_file=selector_config_file,
        )
        
        print(f"Starting {args.type} crawl of: {args.url}")
        print(f"Crawler mode: {args.crawler_mode} {'(automatically detects JS requirements)' if args.crawler_mode == 'adaptive' else ''}")
        print(f"Specialized extraction: {'enabled' if not args.no_specialized else 'disabled'}")
        print(f"Skip images: {'enabled' if args.skip_images else 'disabled'}")
        print(f"Batch size: {args.batch_size}")
        print(f"Network timeout: {args.timeout}s")
        print("-" * 60)
        
        # Run the connector and collect results
        all_documents = []
        batch_count = 0
        
        for batch in connector.load_from_state():
            batch_count += 1
            batch_size = len(batch)
            all_documents.extend(batch)
            
            print(f"Batch {batch_count}: {batch_size} documents")
            
            # Save batch for ingestion API if requested
            if args.save_batches:
                saved_file = save_batch_for_ingestion(batch, batch_count, args.url)
                if saved_file:
                    print(f"  Saved to: {saved_file}")
            
            # Show sample from first batch
            if batch_count == 1 and batch:
                sample_doc = batch[0]
                print(f"  Sample title: {sample_doc.semantic_identifier}")
                print(f"  Sample content length: {len(sample_doc.sections[0].text) if sample_doc.sections else 0} chars")
                if hasattr(sample_doc, 'metadata') and sample_doc.metadata:
                    print(f"  Sample metadata: {list(sample_doc.metadata.keys())}")
        
        print("-" * 60)
        print(f"Crawling complete! Total: {len(all_documents)} documents in {batch_count} batches")
        
        # Report failed URLs with source pages
        if connector.failed_urls:
            print(f"\nFailed URLs ({len(connector.failed_urls)}):")
            for failed_url, failure_info in connector.failed_urls.items():
                error_reason = failure_info["error"]
                source_pages = failure_info["source_pages"]
                
                print(f"  {error_reason}: {failed_url}")
                if source_pages:
                    print(f"    Referenced by {len(source_pages)} page(s):")
                    for source_page in source_pages:
                        print(f"      - {source_page}")
                else:
                    print(f"    No source pages tracked (may be initial URL or external reference)")
                print()  # Empty line for readability
        
        if args.save_batches:
            print(f"Batch files saved to: {args.batch_dir}/")
            print(f"To ingest via API, POST each batch file to: /onyx-api/ingestion")
        
        # Save to file if requested
        if args.output:
            import json
            output_data = []
            for doc in all_documents:
                doc_data = {
                    "id": doc.id,
                    "title": doc.semantic_identifier,
                    "content": doc.sections[0].text if doc.sections else "",
                    "source": doc.source.value if hasattr(doc.source, 'value') else str(doc.source),
                    "metadata": doc.metadata or {},
                    "updated_at": doc.doc_updated_at.isoformat() if doc.doc_updated_at else None
                }
                output_data.append(doc_data)
            
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"Results saved to: {args.output}")
        
        # Show summary of extracted content
        if all_documents:
            total_chars = sum(len(doc.sections[0].text) if doc.sections else 0 for doc in all_documents)
            avg_chars = total_chars // len(all_documents) if all_documents else 0
            print(f"Content summary: {total_chars:,} total chars, {avg_chars:,} avg per document")
            
            # Show extractor usage if specialized extraction was enabled
            if not args.no_specialized:
                extractor_used = any(
                    doc.metadata and doc.metadata.get('extractor') 
                    for doc in all_documents 
                    if hasattr(doc, 'metadata')
                )
                if extractor_used:
                    extractors = set(
                        doc.metadata.get('extractor', 'standard') 
                        for doc in all_documents 
                        if hasattr(doc, 'metadata') and doc.metadata
                    )
                    print(f"Extractors used: {', '.join(extractors)}")
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)