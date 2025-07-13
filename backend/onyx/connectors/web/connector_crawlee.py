"""
Web Connector implementation using Crawlee library.
This module replaces the Playwright-based web scraping with Crawlee's more robust framework.
"""

import hashlib
import io
import ipaddress
import socket
from datetime import datetime, timezone
from enum import Enum
from typing import Any, AsyncGenerator, Optional, cast
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from crawlee import Request
from crawlee.crawlers import BeautifulSoupCrawler, PlaywrightCrawler
from crawlee.crawlers._beautifulsoup import BeautifulSoupCrawlingContext
from crawlee.crawlers._playwright import PlaywrightCrawlingContext
from crawlee.storages import RequestQueue
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
from onyx.file_processing.html_utils import web_html_cleanup, parse_html_with_trafilatura_markdown
from onyx.utils.logger import setup_logger
from onyx.utils.sitemap import list_pages_for_site
from onyx.connectors.web.extractors import ExtractorFactory
from shared_configs.configs import MULTI_TENANT

logger = setup_logger()


class WEB_CONNECTOR_VALID_SETTINGS(str, Enum):
    RECURSIVE = "recursive"
    SINGLE = "single"
    SITEMAP = "sitemap"
    UPLOAD = "upload"


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,"
        "image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-CH-UA": '"Google Chrome";v="123", "Not:A-Brand";v="8"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
}

PDF_MIME_TYPES = [
    "application/pdf",
    "application/x-pdf",
    "application/acrobat",
    "application/vnd.pdf",
    "text/pdf",
    "text/x-pdf",
]

WEB_CONNECTOR_MAX_SCROLL_ATTEMPTS = 20
IFRAME_TEXT_LENGTH_THRESHOLD = 700
JAVASCRIPT_DISABLED_MESSAGE = "You have JavaScript disabled in your browser"


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


def check_internet_connection(url: str) -> None:
    """Verify internet connectivity by making a test request."""
    try:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        response = session.get(url, timeout=30, allow_redirects=True)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response and e.response.status_code == 403:
            logger.warning(
                f"Received 403 Forbidden for {url}, will retry with browser automation"
            )
            return
        
        status_code = e.response.status_code if e.response else -1
        error_msgs = {
            400: "Bad Request",
            401: "Unauthorized",
            404: "Not Found",
            500: "Internal Server Error",
            502: "Bad Gateway",
            503: "Service Unavailable",
            504: "Gateway Timeout",
        }
        
        error_msg = error_msgs.get(status_code, "HTTP Error")
        if status_code == 403:
            raise RuntimeError(
                f"Access denied. Received {status_code} {error_msg}. "
                f"This might be due to bot detection on the website."
            )
        else:
            raise RuntimeError(f"HTTP {status_code} {error_msg}")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Failed to connect to {url}. Please check your internet connection: {e}")
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Connection to {url} timed out")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred while connecting to {url}: {e}")


def is_pdf_content(response: requests.Response) -> bool:
    """Check if response content is PDF based on headers."""
    content_type = response.headers.get("Content-Type", "").lower()
    return any(pdf_type in content_type for pdf_type in PDF_MIME_TYPES)


def is_valid_url(url: str) -> bool:
    """Check if a URL is valid."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def get_internal_links(base_url: str, current_url: str, soup: BeautifulSoup) -> list[str]:
    """Extract internal links from the page."""
    links = set()
    
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href:
            continue
        
        # Account for malformed backslashes in URLs
        href = href.replace("\\", "/")
        
        # "#!" indicates the page is using a hashbang URL, which is a client-side routing technique
        if "#" in href and "#!" not in href:
            href = href.split("#")[0]
        
        if not is_valid_url(href):
            # Relative path handling
            href = urljoin(current_url, href)
        
        if urlparse(href).netloc == urlparse(current_url).netloc and base_url in href:
            links.add(href)
    
    return list(links)


def extract_urls_from_sitemap(sitemap_url: str) -> list[str]:
    """Extract URLs from a sitemap."""
    try:
        response = requests.get(sitemap_url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        urls = [
            _ensure_absolute_url(sitemap_url, loc_tag.text)
            for loc_tag in soup.find_all("loc")
        ]

        if len(urls) == 0 and len(soup.find_all("urlset")) == 0:
            urls = list_pages_for_site(sitemap_url)

        if len(urls) == 0:
            raise ValueError(
                f"No URLs found in sitemap {sitemap_url}. Try using the 'single' or 'recursive' scraping options instead."
            )

        return urls
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch sitemap from {sitemap_url}: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error while processing sitemap {sitemap_url}: {e}")


def _ensure_absolute_url(source_url: str, maybe_relative_url: str) -> str:
    """Convert relative URLs to absolute URLs."""
    if not urlparse(maybe_relative_url).netloc:
        return urljoin(source_url, maybe_relative_url)
    return maybe_relative_url


def _ensure_valid_url(url: str) -> str:
    """Ensure URL has a scheme."""
    if "://" not in url:
        return "https://" + url
    return url


def _read_urls_file(location: str) -> list[str]:
    """Read URLs from a file."""
    with open(location, "r") as f:
        urls = [_ensure_valid_url(line.strip()) for line in f if line.strip()]
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
        scroll_before_scraping: bool = False,
        enable_specialized_extraction: bool = True,
        **kwargs: Any,
    ) -> None:
        self.base_url = _ensure_valid_url(base_url)
        self.mintlify_cleanup = mintlify_cleanup
        self.batch_size = batch_size
        self.scroll_before_scraping = scroll_before_scraping
        self.web_connector_type = web_connector_type
        
        # Initialize URL list based on connector type
        if web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value:
            self.initial_urls = [self.base_url]
            self.recursive = True
        elif web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SINGLE.value:
            self.initial_urls = [self.base_url]
            self.recursive = False
        elif web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value:
            self.initial_urls = extract_urls_from_sitemap(self.base_url)
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
        self.content_hashes: set[int] = set()
        self.documents: list[Document] = []
        self.visited_urls: set[str] = set()
        
        # Initialize extractor factory for specialized content extraction
        self.enable_specialized_extraction = enable_specialized_extraction
        self.extractor_factory = ExtractorFactory() if enable_specialized_extraction else None
        
        # Incremental batching support
        self._batch_queue: list[list[Document]] = []
        self._current_batch: list[Document] = []

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        if credentials:
            logger.warning("Unexpected credentials provided for Web Connector")
        return None
    
    def _add_document_to_batch(self, doc: Document) -> None:
        """Add document to current batch and queue completed batches."""
        self._current_batch.append(doc)
        self.documents.append(doc)  # Keep for compatibility and final processing
        
        # Check if we've reached batch size
        if len(self._current_batch) >= self.batch_size:
            # Queue the completed batch
            self._batch_queue.append(self._current_batch.copy())
            logger.info(f"Queued batch of {len(self._current_batch)} documents")
            self._current_batch.clear()
    
    def _get_next_batch(self) -> Optional[list[Document]]:
        """Get the next available batch from the queue."""
        if self._batch_queue:
            return self._batch_queue.pop(0)
        return None
    
    def _finalize_batches(self) -> None:
        """Add any remaining documents as a final batch."""
        if self._current_batch:
            self._batch_queue.append(self._current_batch.copy())
            logger.info(f"Queued final batch of {len(self._current_batch)} documents")
            self._current_batch.clear()

    async def _handle_pdf(self, url: str) -> Document | None:
        """Handle PDF file processing."""
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()
            
            page_text, metadata, images = read_pdf_file(
                file=io.BytesIO(response.content)
            )
            
            last_modified = response.headers.get("Last-Modified")
            
            return Document(
                id=url,
                sections=[TextSection(link=url, text=page_text)],
                source=DocumentSource.WEB,
                semantic_identifier=url.split("/")[-1],
                metadata=metadata,
                doc_updated_at=(
                    _get_datetime_from_last_modified_header(last_modified)
                    if last_modified
                    else None
                ),
            )
        except Exception as e:
            logger.error(f"Failed to process PDF {url}: {e}")
            return None

    async def _handle_playwright_page(
        self, context: PlaywrightCrawlingContext
    ) -> None:
        """Handle page processing with Playwright crawler."""
        request = context.request
        page = context.page
        url = request.url
        
        logger.info(f"Processing: {url}")
        
        # Skip if already visited
        if url in self.visited_urls:
            logger.info(f"Already visited: {url}")
            return
        
        self.visited_urls.add(url)
        
        try:
            # Validate URL
            protected_url_check(url)
            
            # Handle scrolling if enabled
            if self.scroll_before_scraping:
                await self._scroll_page(page)
            
            # Get page content
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Try specialized extractor first
            extractor = None
            if self.extractor_factory:
                extractor = self.extractor_factory.get_extractor(soup, url)
            
            if extractor:
                # Use specialized extraction
                extracted = extractor.extract_content(soup, url)
                
                # Add discovered links for recursive crawling
                if self.recursive and extracted.links:
                    for link in extracted.links:
                        if link not in self.visited_urls:
                            await context.add_requests([Request.from_url(link)])
                
                # Use extracted content
                title = extracted.title
                text_content = extracted.content
                doc_metadata = extracted.metadata or {}
                
            else:
                # Fall back to standard extraction
                # Extract internal links if recursive mode
                if self.recursive:
                    internal_links = get_internal_links(self.base_url, url, soup)
                    for link in internal_links:
                        if link not in self.visited_urls:
                            await context.add_requests([Request.from_url(link)])
                
                # Extract content as Markdown using trafilatura
                title = soup.title.string if soup.title else None
                content_html = await page.content()
                text_content = parse_html_with_trafilatura_markdown(content_html)
                
                # Fallback to traditional cleanup if trafilatura doesn't work
                if not text_content or len(text_content) < 50:
                    parsed_html = web_html_cleanup(soup, self.mintlify_cleanup)
                    title = parsed_html.title
                    text_content = parsed_html.cleaned_text
                    
                    # Handle iframe content if needed
                    if JAVASCRIPT_DISABLED_MESSAGE in text_content:
                        await self._extract_iframe_content(page, parsed_html)
                doc_metadata = {}
            
            # Check for duplicate content
            content_hash = hash((title, text_content))
            if content_hash in self.content_hashes:
                logger.info(f"Skipping duplicate content for {url}")
                return
            
            self.content_hashes.add(content_hash)
            
            # Create document
            last_modified = None
            if hasattr(context, "response") and context.response:
                last_modified = context.response.headers.get("Last-Modified")
            
            doc = Document(
                id=url,
                sections=[TextSection(link=url, text=text_content)],
                source=DocumentSource.WEB,
                semantic_identifier=title or url,
                metadata=doc_metadata,
                doc_updated_at=(
                    _get_datetime_from_last_modified_header(last_modified)
                    if last_modified
                    else None
                ),
            )
            
            self._add_document_to_batch(doc)
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    async def _handle_beautifulsoup_page(
        self, context: BeautifulSoupCrawlingContext
    ) -> None:
        """Handle page processing with BeautifulSoup crawler (non-JS pages)."""
        request = context.request
        soup = context.soup
        url = request.url
        
        logger.info(f"Processing (no JS): {url}")
        
        # Skip if already visited
        if url in self.visited_urls:
            logger.info(f"Already visited: {url}")
            return
        
        self.visited_urls.add(url)
        
        try:
            # Validate URL
            protected_url_check(url)
            
            # Try specialized extractor first
            extractor = None
            if self.extractor_factory:
                extractor = self.extractor_factory.get_extractor(soup, url)
            
            if extractor:
                # Use specialized extraction
                extracted = extractor.extract_content(soup, url)
                
                # Add discovered links for recursive crawling
                if self.recursive and extracted.links:
                    for link in extracted.links:
                        if link not in self.visited_urls:
                            await context.add_requests([Request.from_url(link)])
                
                # Use extracted content
                title = extracted.title
                text_content = extracted.content
                doc_metadata = extracted.metadata or {}
                
            else:
                # Fall back to standard extraction
                # Extract internal links if recursive mode
                if self.recursive:
                    internal_links = get_internal_links(self.base_url, url, soup)
                    for link in internal_links:
                        if link not in self.visited_urls:
                            await context.add_requests([Request.from_url(link)])
                
                # Extract content as Markdown using trafilatura
                title = soup.title.string if soup.title else None
                text_content = parse_html_with_trafilatura_markdown(str(soup))
                
                # Fallback to traditional cleanup if trafilatura doesn't work
                if not text_content or len(text_content) < 50:
                    parsed_html = web_html_cleanup(soup, self.mintlify_cleanup)
                    title = parsed_html.title
                    text_content = parsed_html.cleaned_text
                doc_metadata = {}
            
            # Check for duplicate content
            content_hash = hash((title, text_content))
            if content_hash in self.content_hashes:
                logger.info(f"Skipping duplicate content for {url}")
                return
            
            self.content_hashes.add(content_hash)
            
            # Create document
            last_modified = None
            if hasattr(context, "response") and context.response:
                last_modified = context.response.headers.get("Last-Modified")
            
            doc = Document(
                id=url,
                sections=[TextSection(link=url, text=text_content)],
                source=DocumentSource.WEB,
                semantic_identifier=title or url,
                metadata=doc_metadata,
                doc_updated_at=(
                    _get_datetime_from_last_modified_header(last_modified)
                    if last_modified
                    else None
                ),
            )
            
            self._add_document_to_batch(doc)
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    async def _scroll_page(self, page: Any) -> None:
        """Scroll page to load lazy content."""
        scroll_attempts = 0
        previous_height = await page.evaluate("document.body.scrollHeight")
        
        while scroll_attempts < WEB_CONNECTOR_MAX_SCROLL_ATTEMPTS:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_load_state("networkidle", timeout=60000)
            await page.wait_for_timeout(500)  # Let JavaScript run
            
            new_height = await page.evaluate("document.body.scrollHeight")
            if new_height == previous_height:
                break
            
            previous_height = new_height
            scroll_attempts += 1

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
        import threading
        import time
        
        async def crawl() -> None:
            # Check internet connection
            check_internet_connection(self.base_url)
            
            # Determine if we need JavaScript rendering
            needs_js = self.scroll_before_scraping or self.oauth_token is not None
            
            if needs_js:
                # Use PlaywrightCrawler for JavaScript rendering
                crawler = PlaywrightCrawler(
                    browser_launch_options={
                        "args": ["--no-sandbox", "--disable-setuid-sandbox"]
                    }
                )
                
                # Set up pre-navigation hook for anti-bot measures
                @crawler.pre_navigation_hook
                async def pre_navigation_hook(context) -> None:
                    # Add script to modify navigator properties to avoid detection
                    await context.page.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                        Object.defineProperty(navigator, 'plugins', {
                            get: () => [1, 2, 3, 4, 5]
                        });
                        Object.defineProperty(navigator, 'languages', {
                            get: () => ['en-US', 'en']
                        });
                    """)
                
                # Handle PDF files separately
                @crawler.router.default_handler
                async def playwright_handler(context: PlaywrightCrawlingContext) -> None:
                    url = context.request.url
                    
                    # Check if URL is PDF
                    if url.lower().endswith(".pdf"):
                        doc = await self._handle_pdf(url)
                        if doc:
                            self._add_document_to_batch(doc)
                        return
                    
                    # Check content type with HEAD request
                    try:
                        head_response = requests.head(url, headers=DEFAULT_HEADERS, allow_redirects=True, timeout=30)
                        if is_pdf_content(head_response):
                            doc = await self._handle_pdf(url)
                            if doc:
                                self._add_document_to_batch(doc)
                            return
                    except Exception:
                        pass  # Continue with regular processing
                    
                    await self._handle_playwright_page(context)
                
            else:
                # Use BeautifulSoupCrawler for static content
                crawler = BeautifulSoupCrawler()
                
                @crawler.router.default_handler
                async def beautifulsoup_handler(context: BeautifulSoupCrawlingContext) -> None:
                    url = context.request.url
                    
                    # Check if URL is PDF
                    if url.lower().endswith(".pdf"):
                        doc = await self._handle_pdf(url)
                        if doc:
                            self._add_document_to_batch(doc)
                        return
                    
                    await self._handle_beautifulsoup_page(context)
            
            # Run the crawler
            await crawler.run(self.initial_urls)
        
        # Flag to track when crawling is complete
        crawl_complete = threading.Event()
        
        def run_crawler():
            """Run the crawler in a separate thread."""
            try:
                asyncio.run(crawl())
            finally:
                crawl_complete.set()
        
        # Start crawling in background thread
        crawler_thread = threading.Thread(target=run_crawler)
        crawler_thread.start()
        
        # Yield batches as they become available during crawling
        try:
            while not crawl_complete.is_set() or self._batch_queue:
                # Check for completed batches
                batch = self._get_next_batch()
                if batch:
                    yield batch
                else:
                    # No batch ready, wait briefly if crawling is still active
                    if not crawl_complete.is_set():
                        time.sleep(0.1)
                    elif not self._batch_queue:
                        # Crawling done and no more batches
                        break
        finally:
            # Ensure crawler thread completes
            crawler_thread.join()
        
        # Finalize any remaining documents and yield final batches
        self._finalize_batches()
        
        # Yield any remaining batches
        while True:
            batch = self._get_next_batch()
            if not batch:
                break
            yield batch
        
        # Check if we found any documents at all
        if not self.documents:
            raise RuntimeError("No valid pages found.")

    def validate_connector_settings(self) -> None:
        """Validate connector configuration and test connectivity."""
        # Make sure we have at least one valid URL to check
        if not self.initial_urls:
            raise ConnectorValidationError(
                "No URL configured. Please provide at least one valid URL."
            )

        if (
            self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value
            or self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value
        ):
            return None

        # We'll just test the first URL for connectivity and correctness
        test_url = self.initial_urls[0]

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

        # Make a quick request to see if we get a valid response
        try:
            check_internet_connection(test_url)
        except Exception as e:
            err_str = str(e)
            if "401" in err_str:
                raise CredentialExpiredError(
                    f"Unauthorized access to '{test_url}': {e}"
                )
            elif "403" in err_str:
                raise InsufficientPermissionsError(
                    f"Forbidden access to '{test_url}': {e}"
                )
            elif "404" in err_str:
                raise ConnectorValidationError(f"Page not found for '{test_url}': {e}")
            elif "Max retries exceeded" in err_str and "NameResolutionError" in err_str:
                raise ConnectorValidationError(
                    f"Unable to resolve hostname for '{test_url}'. Please check the URL and your internet connection."
                )
            else:
                # Could be a 5xx or another error, treat as unexpected
                raise UnexpectedValidationError(
                    f"Unexpected error validating '{test_url}': {e}"
                )


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Crawlee Web Connector - Extract content from websites with specialized framework support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single page extraction
  python connector_crawlee.py --url https://example.com --type single
  
  # Recursive crawling with JavaScript
  python connector_crawlee.py --url https://docs.example.com --type recursive --js
  
  # Sitemap-based crawling
  python connector_crawlee.py --url https://example.com/sitemap.xml --type sitemap
  
  # Zoomin documentation with custom batch size
  python connector_crawlee.py --url https://docs-cybersec.thalesgroup.com/bundle/guide/page/overview.htm --type single --js --batch-size 5
  
  # Save batches for ingestion API
  python connector_crawlee.py --url https://docs.example.com --type recursive --save-batches --batch-dir ./ingestion_batches
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
        "--js", "--javascript",
        action="store_true",
        help="Enable JavaScript rendering (slower but handles dynamic content)"
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
    
    # Create connector with specified parameters
    try:
        connector = WebConnectorCrawlee(
            base_url=args.url,
            web_connector_type=args.type,
            mintlify_cleanup=not args.no_mintlify,
            batch_size=args.batch_size,
            scroll_before_scraping=args.js,
            enable_specialized_extraction=not args.no_specialized,
        )
        
        print(f"Starting {args.type} crawl of: {args.url}")
        print(f"JavaScript rendering: {'enabled' if args.js else 'disabled'}")
        print(f"Specialized extraction: {'enabled' if not args.no_specialized else 'disabled'}")
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