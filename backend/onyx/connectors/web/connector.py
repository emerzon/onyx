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
        **kwargs: Any,
    ) -> None:
        self.base_url = base_url if "://" in base_url else f"https://{base_url}"
        self.mintlify_cleanup = mintlify_cleanup
        self.batch_size = batch_size
        self.web_connector_type = web_connector_type
        
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
        
        # Initialize extractor factory for specialized content extraction
        self.enable_specialized_extraction = enable_specialized_extraction
        self.extractor_factory = ExtractorFactory() if enable_specialized_extraction else None
        
        # Incremental batching support
        self._batch_queue: list[list[Document]] = []
        self._current_batch: list[Document] = []

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


    def _extract_content(self, url: str, soup: BeautifulSoup, raw_html: str) -> tuple[str | None, str, dict[str, Any]]:
        """Extract content using specialized extractors or fallback methods."""
        # Try specialized extractor first
        if self.extractor_factory:
            extractor = self.extractor_factory.get_extractor(soup, url)
            if extractor:
                extracted = extractor.extract_content(soup, url)
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

    async def _handle_adaptive_page(self, context) -> None:
        """Handle page processing with AdaptivePlaywrightCrawler."""
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
            title, text_content, metadata = self._extract_content(url, soup, raw_html)
            
            # Add discovered links for recursive crawling (Crawlee handles deduplication)
            if self.recursive:
                await context.enqueue_links(strategy="same-hostname")
            
            # Check for duplicate content using stable hash
            content_key = f"{title}:{text_content}"
            content_hash = hashlib.md5(content_key.encode("utf-8")).hexdigest()
            
            if content_hash in self.content_hashes:
                logger.info(f"Skipping duplicate content for {url}")
                return
            
            self.content_hashes.add(content_hash)
            
            # Create and batch document
            last_modified = getattr(context.response, 'headers', {}).get("Last-Modified") if hasattr(context, 'response') else None
            
            doc = Document(
                id=url,
                sections=[TextSection(link=url, text=text_content)],
                source=DocumentSource.WEB,
                semantic_identifier=title or url,
                metadata=metadata,
                doc_updated_at=_get_datetime_from_last_modified_header(last_modified) if last_modified else None,
            )
            
            self._add_document_to_batch(doc)
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")


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
                    max_concurrency=10,
                    desired_concurrency=5,
                    min_concurrency=1
                ),
            )
            
            # Optional stealth hook (Crawlee provides good defaults)
            @crawler.pre_navigation_hook
            async def pre_navigation_hook(context) -> None:
                if hasattr(context, 'page') and context.page:
                    # Minimal stealth - Crawlee handles most anti-bot measures automatically
                    await context.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            
            @crawler.router.default_handler
            async def adaptive_handler(context) -> None:
                url = context.request.url
                
                # Check if URL is PDF and handle text extraction
                if url.lower().endswith(".pdf"):
                    doc = await self._handle_pdf_from_context(context)
                    if doc:
                        self._add_document_to_batch(doc)
                    return
                
                # Handle regular web pages
                await self._handle_adaptive_page(context)
            
            # Run the crawler with initial URLs
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
            enable_specialized_extraction=not args.no_specialized,
        )
        
        print(f"Starting {args.type} crawl of: {args.url}")
        print(f"Adaptive rendering: enabled (automatically detects JS requirements)")
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