"""
Async Web Connector implementation using proper async patterns.
This replaces the sync-over-async anti-pattern in the original connector.
"""

import asyncio
import hashlib
import io
import ipaddress
import re
import shutil
import socket
import tempfile
import time
import threading
import traceback
import sys
import signal
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, List, Optional, Dict, Tuple
from urllib.parse import urlparse, urljoin

import httpx

from bs4 import BeautifulSoup
from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    BeautifulSoupCrawler,
    PlaywrightCrawler,
)
from crawlee import ConcurrencySettings
from crawlee.storages import Dataset
from crawlee.request_loaders import SitemapRequestLoader, RequestList
from crawlee.http_clients import HttpxHttpClient

from onyx.configs.app_configs import INDEX_BATCH_SIZE, WEB_CONNECTOR_VALIDATE_URLS
from onyx.configs.constants import DocumentSource
from shared_configs.configs import MULTI_TENANT
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


class ThreadDumper:
    """Utility for capturing thread dumps and async task states."""
    
    @staticmethod
    def get_thread_dump() -> str:
        """Get a comprehensive thread dump including stack traces."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"THREAD DUMP - {datetime.now(timezone.utc).isoformat()}")
        lines.append("=" * 80)
        
        # Get all threads
        threads = threading.enumerate()
        lines.append(f"\nTotal threads: {len(threads)}")
        lines.append("-" * 80)
        
        for thread in threads:
            lines.append(f"\nThread: {thread.name} (ID: {thread.ident}, Daemon: {thread.daemon})")
            lines.append(f"  Alive: {thread.is_alive()}")
            
            # Get stack trace for this thread
            if thread.ident:
                frame = sys._current_frames().get(thread.ident)
                if frame:
                    lines.append("  Stack trace:")
                    for filename, lineno, name, line in traceback.extract_stack(frame):
                        lines.append(f"    File \"{filename}\", line {lineno}, in {name}")
                        if line:
                            lines.append(f"      {line.strip()}")
        
        return "\n".join(lines)
    
    @staticmethod
    def get_async_task_dump() -> str:
        """Get information about all async tasks."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"ASYNC TASK DUMP - {datetime.now(timezone.utc).isoformat()}")
        lines.append("=" * 80)
        
        # Get all tasks
        tasks = asyncio.all_tasks()
        lines.append(f"\nTotal tasks: {len(tasks)}")
        lines.append("-" * 80)
        
        # Group tasks by state
        running = []
        pending = []
        done = []
        cancelled = []
        
        for task in tasks:
            if task.done():
                if task.cancelled():
                    cancelled.append(task)
                else:
                    done.append(task)
            else:
                if task == asyncio.current_task():
                    running.append(task)
                else:
                    pending.append(task)
        
        lines.append(f"\nTask states:")
        lines.append(f"  Running: {len(running)}")
        lines.append(f"  Pending: {len(pending)}")
        lines.append(f"  Done: {len(done)}")
        lines.append(f"  Cancelled: {len(cancelled)}")
        
        # Detail each task
        for category, tasks_list in [("RUNNING", running), ("PENDING", pending), ("DONE", done[:10]), ("CANCELLED", cancelled[:10])]:
            if tasks_list:
                lines.append(f"\n{category} TASKS:")
                for i, task in enumerate(tasks_list):
                    lines.append(f"\n  Task {i+1}:")
                    lines.append(f"    Name: {task.get_name()}")
                    lines.append(f"    Coro: {task.get_coro()}")
                    
                    # Get stack if available
                    if not task.done():
                        stack = task.get_stack()
                        if stack:
                            lines.append("    Stack:")
                            for frame in stack[:5]:  # Limit to 5 frames
                                lines.append(f"      {frame}")
                    
                    # Get exception if task failed
                    if task.done() and not task.cancelled():
                        try:
                            exception = task.exception()
                            if exception:
                                lines.append(f"    Exception: {type(exception).__name__}: {exception}")
                        except:
                            pass
        
        return "\n".join(lines)
    
    @staticmethod
    def get_event_loop_info() -> str:
        """Get information about the event loop."""
        lines = []
        lines.append("=" * 80)
        lines.append("EVENT LOOP INFO")
        lines.append("=" * 80)
        
        try:
            loop = asyncio.get_running_loop()
            lines.append(f"Loop running: {loop.is_running()}")
            lines.append(f"Loop closed: {loop.is_closed()}")
            lines.append(f"Debug mode: {loop.get_debug()}")
            
            # Get pending callbacks
            # Note: This is implementation-specific and might not work on all Python versions
            if hasattr(loop, '_ready'):
                lines.append(f"Ready callbacks: {len(loop._ready)}")
            if hasattr(loop, '_scheduled'):
                lines.append(f"Scheduled callbacks: {len(loop._scheduled)}")
                
        except RuntimeError:
            lines.append("No running event loop")
        
        return "\n".join(lines)
    
    @staticmethod
    def dump_all() -> str:
        """Get complete diagnostic dump."""
        sections = []
        
        # Thread dump
        sections.append(ThreadDumper.get_thread_dump())
        
        # Async task dump
        try:
            sections.append(ThreadDumper.get_async_task_dump())
        except RuntimeError:
            sections.append("No async task dump available (no running loop)")
        
        # Event loop info
        sections.append(ThreadDumper.get_event_loop_info())
        
        return "\n\n".join(sections)


class PerformanceMonitor:
    """Monitor performance metrics and detect potential deadlocks."""
    
    def __init__(self, deadlock_threshold: float = 300.0):
        self.deadlock_threshold = deadlock_threshold
        self.operation_times: Dict[str, List[float]] = defaultdict(list)
        self.active_operations: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self._deadlock_check_task: Optional[asyncio.Task] = None
        
    async def start_operation(self, operation_id: str, operation_type: str, metadata: Dict[str, Any] = None) -> None:
        """Start tracking an operation."""
        with self.lock:
            self.active_operations[operation_id] = {
                "type": operation_type,
                "start_time": time.perf_counter(),
                "metadata": metadata or {},
                "thread_id": threading.current_thread().ident,
                "task_id": id(asyncio.current_task()),
            }
        logger.debug(f"Started operation {operation_id} ({operation_type})")
    
    async def end_operation(self, operation_id: str) -> None:
        """End tracking an operation and record its duration."""
        with self.lock:
            if operation_id in self.active_operations:
                op = self.active_operations.pop(operation_id)
                duration = time.perf_counter() - op["start_time"]
                self.operation_times[op["type"]].append(duration)
                
                if duration > 10.0:  # Log slow operations
                    logger.warning(
                        f"Slow operation detected: {operation_id} ({op['type']}) "
                        f"took {duration:.2f}s - metadata: {op['metadata']}"
                    )
                else:
                    logger.debug(f"Completed operation {operation_id} in {duration:.2f}s")
    
    async def check_deadlocks(self) -> None:
        """Check for potential deadlocks in active operations."""
        deadlock_count = 0
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                current_time = time.perf_counter()
                
                detected_deadlocks = []
                with self.lock:
                    for op_id, op_info in list(self.active_operations.items()):
                        duration = current_time - op_info["start_time"]
                        if duration > self.deadlock_threshold:
                            detected_deadlocks.append((op_id, op_info, duration))
                
                if detected_deadlocks:
                    deadlock_count += 1
                    logger.error(f"DEADLOCK DETECTION #{deadlock_count} - Found {len(detected_deadlocks)} stuck operations")
                    
                    # Log each deadlock
                    for op_id, op_info, duration in detected_deadlocks:
                        logger.error(
                            f"Potential deadlock: Operation {op_id} ({op_info['type']}) "
                            f"has been running for {duration:.2f}s\n"
                            f"Thread: {op_info['thread_id']}, Task: {op_info['task_id']}\n"
                            f"Metadata: {op_info['metadata']}"
                        )
                    
                    # Capture and log thread dump
                    logger.error("Capturing diagnostic information...")
                    dump = ThreadDumper.dump_all()
                    
                    # Write to file for analysis
                    dump_file = f"/tmp/crawler_deadlock_{deadlock_count}_{int(time.time())}.txt"
                    try:
                        with open(dump_file, 'w') as f:
                            f.write(dump)
                        logger.error(f"Thread dump written to: {dump_file}")
                    except Exception as e:
                        logger.error(f"Failed to write thread dump: {e}")
                    
                    # Also log key parts to logger
                    logger.error("Thread dump summary:\n" + dump[:2000] + "\n... (truncated)")
                    
            except Exception as e:
                logger.error(f"Error in deadlock detection: {e}")
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        with self.lock:
            stats = {}
            for op_type, times in self.operation_times.items():
                if times:
                    stats[op_type] = {
                        "count": len(times),
                        "avg": sum(times) / len(times),
                        "min": min(times),
                        "max": max(times),
                        "p95": sorted(times)[int(len(times) * 0.95)] if len(times) > 20 else max(times),
                    }
            
            stats["active_operations"] = len(self.active_operations)
            stats["active_operation_details"] = [
                {
                    "id": op_id,
                    "type": op["type"],
                    "duration": time.perf_counter() - op["start_time"],
                    "metadata": op["metadata"]
                }
                for op_id, op in self.active_operations.items()
            ]
            
            return stats


class HostLimitedTransport(httpx.AsyncHTTPTransport):
    """Custom transport that limits concurrent requests per host to prevent HTTP/2 stream exhaustion."""

    def __init__(self, *args, max_streams_per_host: int = 5, **kwargs):
        super().__init__(*args, **kwargs)
        self._host_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._max_streams_per_host = max_streams_per_host
        self._lock = asyncio.Lock()

    async def handle_async_request(self, request):
        # Extract host from request
        host = request.url.host

        # Get or create semaphore for this host
        async with self._lock:
            if host not in self._host_semaphores:
                self._host_semaphores[host] = asyncio.Semaphore(self._max_streams_per_host)
            semaphore = self._host_semaphores[host]

        # Limit concurrent requests to this host
        async with semaphore:
            return await super().handle_async_request(request)


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
    
    # Class variable to track instances for signal handling
    _instances = []

    def __init__(
        self,
        base_url: str,
        web_connector_type: str = WEB_CONNECTOR_VALID_SETTINGS.RECURSIVE.value,
        mintlify_cleanup: bool = True,
        batch_size: int = INDEX_BATCH_SIZE,
        enable_specialized_extraction: bool = True,
        crawler_mode: str = "static",  # Default to static to avoid adaptive crawler issues
        skip_images: bool = False,
        selector_config: Optional[str] = None,
        selector_config_file: Optional[str] = None,
        oauth_token: Optional[str] = None,
        sitemap_url_limit: Optional[int] = None,  # Limit URLs from sitemap
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
        
        # Initialize performance monitor
        self.performance_monitor = PerformanceMonitor(deadlock_threshold=300.0)
        self._perf_monitor_task: Optional[asyncio.Task] = None
        
        # Progress tracking
        self.processed_count = 0
        self.total_urls = 0
        self._last_progress_time = time.time()
        self._last_progress_count = 0
        
        # Queue for real-time batch yielding
        self._raw_items_queue = None
        
        # Register instance for signal handling
        AsyncWebConnectorCrawlee._instances.append(self)
        
        # Setup signal handler for thread dumps (SIGUSR1)
        if not hasattr(AsyncWebConnectorCrawlee, '_signal_handler_installed'):
            try:
                signal.signal(signal.SIGUSR1, AsyncWebConnectorCrawlee._handle_dump_signal)
                AsyncWebConnectorCrawlee._signal_handler_installed = True
                logger.info("Thread dump signal handler installed (kill -USR1 <pid>)")
            except Exception as e:
                logger.warning(f"Could not install signal handler: {e}")

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
        self.sitemap_url_limit = sitemap_url_limit

        # Legacy state management for backward compatibility
        self._documents: list[Document] = []
        self._documents_lock = asyncio.Lock()
        self._failed_urls: dict[str, str] = {}  # Simplified: URL -> error reason
        self._failed_urls_lock = asyncio.Lock()
        
        # Content deduplication
        self._content_hashes: set[str] = set()
        self._content_hashes_lock = asyncio.Lock()
        
        # Metrics
        self._metrics = {
            "pages_crawled": 0,
            "documents_generated": 0,
            "errors": 0,
            "start_time": datetime.now(timezone.utc),
        }

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
        self.extractor_factory = None  # Will be set in _setup_content_processor
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
        
        # Clean up temporary storage directory
        try:
            import shutil
            import tempfile
            import os
            storage_dir = os.path.join(tempfile.gettempdir(), f"crawlee_{os.getpid()}")
            if os.path.exists(storage_dir):
                shutil.rmtree(storage_dir)
                logger.info(f"Cleaned up temporary storage directory: {storage_dir}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary storage: {e}")

    def _setup_content_processor(self) -> None:
        """Setup content processor with extractors and selectors."""
        # Initialize extractor factory for specialized content extraction
        extractor_factory = None
        if self.config.enable_specialized_extraction:
            try:
                from onyx.connectors.web.extractors import ExtractorFactory

                extractor_factory = ExtractorFactory()
                self.extractor_factory = extractor_factory
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
            # For recursive crawling, use just the URL list - crawlers handle RequestList internally
            self.request_loader = [self.base_url]  # Just pass URLs, not RequestList
            self.request_manager = None
            self.recursive = True
            logger.info(f"Initialized recursive crawler with URL: {self.base_url}")

        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SINGLE.value:
            # For single page, use just the URL
            self.request_loader = [self.base_url]
            self.request_manager = None
            self.recursive = False

        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value:
            # Use Crawlee's native SitemapRequestLoader for efficient sitemap processing
            # Auto-detect sitemap URL if not explicitly provided
            sitemap_url = self.base_url
            parsed_url = urlparse(self.base_url)
            
            sitemap_indicators = [
                "sitemap" in parsed_url.path.lower(),
                parsed_url.path.endswith(".xml"),
                "sitemap_index" in parsed_url.path.lower(),
            ]
            
            if not any(sitemap_indicators):
                # Auto-append sitemap.xml if not present
                from urllib.parse import urljoin
                if parsed_url.path.endswith('/'):
                    sitemap_url = urljoin(self.base_url, 'sitemap.xml')
                else:
                    sitemap_url = urljoin(self.base_url + '/', 'sitemap.xml')
                logger.info(f"Auto-appended sitemap.xml to URL: {sitemap_url}")
            
            logger.info(f"Using Crawlee SitemapRequestLoader for sitemap: {sitemap_url}")

            # Create URL filtering patterns for better control
            exclude_patterns = self.config_manager.get_exclude_patterns()

            # Convert string patterns to regex patterns for SitemapRequestLoader
            import re
            from crawlee.http_clients import HttpxHttpClient
            import httpx

            exclude_regexes = (
                [re.compile(pattern.replace("*", ".*")) for pattern in exclude_patterns] if exclude_patterns else None
            )

            # Create HTTP client for sitemap loader with proper limits
            crawlee_http_client = HttpxHttpClient(
                # Configure for better performance with HTTP/2
                persist_cookies_per_session=True,
                http2=True,  # Enable HTTP/2 for better performance
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=30.0,
                    write=10.0,
                    pool=10.0,
                ),
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=10,
                    keepalive_expiry=30,
                ),
                # Use custom transport to limit streams per host
                transport=HostLimitedTransport(
                    http2=True,
                    max_streams_per_host=5,
                ),
            )

            # Create SitemapRequestLoader and convert to RequestManager following Crawlee docs
            try:
                logger.info("Creating SitemapRequestLoader...")
                sitemap_loader = SitemapRequestLoader(
                    sitemap_urls=[sitemap_url],  # Use the auto-detected sitemap URL
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
                # Fallback: use URL list with the sitemap URL
                logger.info(f"Falling back to treating {sitemap_url} as a single page")
                self.request_loader = [sitemap_url]
                self.request_manager = None
                self.sitemap_loader = None

            self.recursive = False

        elif self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.UPLOAD.value:
            if MULTI_TENANT:
                raise ValueError("Upload input for web connector is not supported in cloud environments")
            logger.warning("This is not a UI supported Web Connector flow, are you sure you want to do this?")
            # Read URLs from file
            urls = _read_urls_file(self.base_url)
            self.request_loader = urls  # Just pass the URL list
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
                try:
                    extracted = generic_extractor.extract_content(soup, url)
                    # Ignore links - let Crawlee handle link discovery automatically
                    if extracted.content and len(extracted.content.strip()) > 10:
                        return extracted.title, extracted.content, extracted.metadata or {}
                    else:
                        logger.debug(f"Generic extractor returned insufficient content for {url}")
                except Exception as e:
                    logger.warning(f"Generic extractor failed for {url}: {e}")

        # Try specialized extractor second
        if self.extractor_factory:
            extractor = self.extractor_factory.get_extractor(soup, url)
            if extractor:
                try:
                    extracted = extractor.extract_content(soup, url)
                    # Ignore links - let Crawlee handle link discovery automatically
                    if extracted.content and len(extracted.content.strip()) > 10:
                        return extracted.title, extracted.content, extracted.metadata or {}
                    else:
                        logger.debug(f"Specialized extractor returned insufficient content for {url}")
                except Exception as e:
                    logger.warning(f"Specialized extractor failed for {url}: {e}")

        # Fallback to standard extraction
        logger.info(f"Using fallback extraction for {url}")
        title = soup.title.string if soup.title else None
        text_content = convert_html_to_markdown(raw_html)

        # Final fallback to traditional cleanup if needed
        if not text_content or len(text_content) < 50:
            parsed_html = web_html_cleanup(soup, self.mintlify_cleanup)
            title = parsed_html.title
            text_content = parsed_html.cleaned_text

        return title, text_content, {}

    async def _handle_file_extraction_async(self, context) -> None:
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
                return
            # Detect content type
            content_type = detect_content_type(url, headers)
            # PDF special handling
            if content_type == "pdf":
                if not file_content.startswith(b"%PDF"):
                    logger.warning(f"Invalid PDF content for {url} (missing PDF header)")
                    return
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
                return
            if not text_content or len(text_content.strip()) < 50:
                logger.warning(
                    f"Insufficient content extracted from {content_type} file {url}. Extracted length: {len(text_content.strip()) if text_content else 0}"
                )
                return

            # Get last modified date from headers if available
            doc_updated_at = self._parse_last_modified(headers)

            # Push to dataset
            dataset_metadata = {
                **metadata,
                "content_type": content_type,
            }
            if doc_updated_at:
                dataset_metadata["doc_updated_at"] = doc_updated_at.isoformat()
            
            await context.push_data({
                "url": url,
                "title": metadata.get("title") or f"{content_type.upper()} Document",
                "content": text_content,
                "metadata": dataset_metadata,
            })
        except Exception as e:
            logger.error(f"Error handling file extraction for {url}: {e}")
            raise

    async def _create_crawler(self):
        # Configure crawlee to use /tmp for storage (much faster, especially on tmpfs)
        import tempfile
        import os
        storage_dir = os.path.join(tempfile.gettempdir(), f"crawlee_{os.getpid()}")
        # Set the CRAWLEE_STORAGE_DIR environment variable before importing crawlee
        os.environ['CRAWLEE_STORAGE_DIR'] = storage_dir
        logger.info(f"Using temporary storage directory: {storage_dir}")
        """Create crawler based on configured mode with enhanced error handling."""
        # Import httpx at method level to ensure it's available
        import httpx

        # Optimize concurrency based on connector type
        if self.web_connector_type == WEB_CONNECTOR_VALID_SETTINGS.SITEMAP.value:
            # For sitemap crawling, use higher concurrency
            concurrency_settings = ConcurrencySettings(
                desired_concurrency=20,  # Start with 20 concurrent requests
                max_concurrency=50,      # Allow up to 50 concurrent requests
                min_concurrency=10,      # Keep at least 10 active
                max_tasks_per_minute=600,  # Allow 600 requests per minute
            )
            logger.info("Using optimized concurrency for sitemap crawling")
            
            # Force static mode for sitemap crawling for better performance
            if self.crawler_mode == "adaptive":
                logger.info("Switching from adaptive to static mode for sitemap crawling performance")
                self.crawler_mode = "static"
        else:
            # Use default for other types
            concurrency_settings = ConcurrencySettings()

        # Configure statistics with error snapshots for debugging
        from crawlee.statistics import Statistics, StatisticsState

        statistics = Statistics(
            save_error_snapshots=True,  # Save page snapshots on errors for debugging
            state_model=StatisticsState,
        )

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
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        ]

        # Use request_manager if available (for sitemap), otherwise use default
        crawler_kwargs = {
            "concurrency_settings": concurrency_settings,
            "session_pool": self.session_pool,  # Add SessionPool for better session management
            "statistics": statistics,  # Add custom statistics with error snapshots
        }
        if self.request_manager:
            crawler_kwargs["request_manager"] = self.request_manager

        try:
            if self.crawler_mode == "adaptive":
                # Try with RequestManager first, fallback without it if SSL context issues
                try:
                    logger.info(f"Creating adaptive crawler (request_manager: {self.request_manager is not None}, request_loader: {self.request_loader is not None})")

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

                    # Create base kwargs without request_manager for now
                    base_crawler_kwargs = {
                        "concurrency_settings": concurrency_settings,
                        "session_pool": self.session_pool,
                        "statistics": statistics,
                    }
                    
                    return AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
                        rendering_type_predictor=FrameworkAwareRenderingTypePredictor(self.selector_resolver) if self.selector_resolver else None,
                        playwright_crawler_specific_kwargs={
                            "browser_launch_options": {
                                "args": browser_args,
                                "timeout": 30000,  # 30 second timeout
                                "ignore_default_args": ["--enable-automation"],
                            },
                        },
                        **base_crawler_kwargs,
                    )
                except Exception as e:
                    if "cannot pickle 'SSLContext' object" in str(e) and self.request_manager:
                        logger.warning("SSL context pickling issue detected, creating adaptive crawler without RequestManager")
                        # Create crawler without RequestManager to avoid SSL context issues
                        crawler_kwargs_no_manager = {k: v for k, v in crawler_kwargs.items() if k != "request_manager"}
                        return AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(
                            rendering_type_predictor=FrameworkAwareRenderingTypePredictor(self.selector_resolver) if self.selector_resolver else None,
                            playwright_crawler_specific_kwargs={
                                "browser_launch_options": {
                                    "args": browser_args,
                                    "timeout": 30000,  # 30 second timeout
                                    "ignore_default_args": ["--enable-automation"],
                                },
                            },
                            **crawler_kwargs_no_manager,
                        )
                    else:
                        raise
            elif self.crawler_mode == "static":
                logger.info("Creating static crawler")
                return BeautifulSoupCrawler(
                    # Configure HTTP client with custom transport for per-host stream limiting
                    http_client=HttpxHttpClient(
                        # Allow HTTP/2 for better performance
                        http2=True,
                        # Configure timeouts to prevent hanging
                        timeout=httpx.Timeout(
                            connect=10.0,
                            read=30.0,  # Match the handler timeout
                            write=10.0,
                            pool=10.0,  # Allow reasonable pool wait time
                        ),
                        limits=httpx.Limits(
                            # HTTP/2 allows many streams per connection
                            max_connections=10,  # Per-host connection limit
                            max_keepalive_connections=10,
                            keepalive_expiry=30,  # Standard keepalive
                        ),
                        # Use custom transport to limit streams per host
                        transport=HostLimitedTransport(
                            http2=True,
                            max_streams_per_host=5,  # Limit to 5 concurrent streams per host
                        ),
                    ),
                    request_handler_timeout=timedelta(seconds=30),  # 30 second timeout per request
                    max_request_retries=2,  # Retry failed requests up to 2 times
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
                    **crawler_kwargs,
                )
        except Exception as e:
            logger.error(f"Failed to create {self.crawler_mode} crawler: {e}")
            # Fallback to static crawler with default configuration
            logger.warning("Falling back to static crawler with default configuration")
            # Fallback crawler - use only basic settings without request_manager
            # Configure HTTP client to force HTTP/1.1 and prevent deadlocks
            try:
                fallback_http_client = HttpxHttpClient(
                    # Allow HTTP/2 for better performance
                    http2=True,
                    timeout=httpx.Timeout(
                        connect=10.0,
                        read=30.0,
                        write=10.0,
                        pool=10.0,
                    ),
                    limits=httpx.Limits(
                        max_connections=10,
                        max_keepalive_connections=10,
                        keepalive_expiry=30,
                    ),
                    # Use custom transport to limit streams per host
                    transport=HostLimitedTransport(
                        http2=True,
                        max_streams_per_host=5,
                    ),
                )
                return BeautifulSoupCrawler(
                    http_client=fallback_http_client,
                )
            except Exception as e2:
                logger.error(f"Failed to create fallback crawler with custom HTTP client: {e2}")
                # Last resort - minimal crawler
                return BeautifulSoupCrawler()

    async def _setup_crawler_handlers(self, crawler, raw_items_queue=None):
        """Setup crawler handlers for different content types using Crawlee's dataset flow."""
        
        # Store queue reference for use in handler
        self._raw_items_queue = raw_items_queue
        logger.info(f"_setup_crawler_handlers: Queue {'provided' if raw_items_queue else 'NOT provided'}, storing as instance variable")

        @crawler.router.default_handler
        async def async_handler(context) -> None:
            url = context.request.url
            logger.info(f"Handler invoked for URL: {url}, queue={'available' if self._raw_items_queue else 'NOT available'}")
            operation_id = f"page_{id(context)}_{url[:50]}"
            
            await self.performance_monitor.start_operation(operation_id, "page_processing", {"url": url, "context_type": type(context).__name__})
            
            # Update progress tracking
            self.processed_count += 1
            current_time = time.time()
            
            # Yield control periodically to allow batch processor to run
            # Only yield every batch_size URLs to minimize overhead
            if self.processed_count % self.batch_size == 0:
                await asyncio.sleep(0)
            
            # Log progress every 5 seconds or every 10 URLs (more frequent for debugging)
            if (current_time - self._last_progress_time > 5) or (self.processed_count % 10 == 0):
                rate = (self.processed_count - self._last_progress_count) / (current_time - self._last_progress_time) if current_time > self._last_progress_time else 0
                if self.total_urls > 0:
                    logger.info(f"Progress: {self.processed_count}/{self.total_urls} URLs processed ({self.processed_count/self.total_urls*100:.1f}%) - Rate: {rate:.1f} URLs/sec")
                else:
                    logger.info(f"Progress: {self.processed_count} URLs processed - Rate: {rate:.1f} URLs/sec")
                self._last_progress_time = current_time
                self._last_progress_count = self.processed_count
            
            # Debug logging to understand context type
            logger.debug(f"Handler received context type: {type(context).__name__}")
            logger.debug(f"Context module: {type(context).__module__}")
            
            # List key attributes for debugging
            attrs = [attr for attr in dir(context) if not attr.startswith('_')]
            logger.debug(f"Context attributes (first 20): {attrs[:20]}")

            try:
                # Check for error status codes first - be careful with adaptive context
                if type(context).__name__ != "AdaptivePlaywrightCrawlingContext":
                    # Safe to check response on non-adaptive contexts
                    if hasattr(context, "response") and hasattr(context.response, "status_code"):
                        status_code = context.response.status_code
                        if status_code >= 400:
                            logger.debug(f"Skipping {url} due to HTTP {status_code}")
                            return

                # Basic URL validation (Crawlee handles most filtering through patterns)
                protected_url_check(url)

                # Handle different content types using Crawlee's native data flow
                if url.lower().endswith(".pdf") or self.config_manager.is_binary_file_supported(url):
                    await self._handle_file_extraction_async(context)
                elif hasattr(context, "soup"):
                    # BeautifulSoupCrawler context (our original static crawler)
                    logger.debug(f"Using static handler for {url}")
                    await self._handle_static_page_async(context)
                elif hasattr(context, "body") or hasattr(context, "$"):
                    # CheerioCrawler context (from AdaptivePlaywrightCrawler in static mode)
                    logger.debug(f"Using static handler for {url} (adaptive crawler in static mode)")
                    await self._handle_static_page_async(context)
                elif type(context).__name__ == "AdaptivePlaywrightCrawlingContext":
                    # AdaptivePlaywrightCrawler context - check if it has parsed content (static mode)
                    try:
                        # First check if parsed_content is available
                        parsed_content = context.parsed_content
                        if parsed_content is not None:
                            logger.debug(f"Using static handler for {url} (adaptive crawler with parsed content)")
                            await self._handle_static_page_async(context)
                        else:
                            # Try to access page to confirm dynamic mode
                            _ = context.page
                            logger.debug(f"Using adaptive/dynamic handler for {url}")
                            await self._handle_adaptive_page_async(context)
                    except (AttributeError, RuntimeError) as e:
                        if "PlaywrightCrawler" in str(e):
                            # This means static mode
                            logger.debug(f"Adaptive crawler in static mode for {url}")
                            await self._handle_static_page_async(context)
                        else:
                            # Re-raise if it's a different error
                            raise
                elif type(context).__name__ == "PlaywrightCrawlingContext":
                    # Regular PlaywrightCrawler context (used by AdaptivePlaywrightCrawler in dynamic mode)
                    logger.debug(f"Using adaptive handler for {url} (PlaywrightCrawlingContext)")
                    await self._handle_adaptive_page_async(context)
                else:
                    # Unknown context type
                    logger.error(f"Unknown context type for {url}: {type(context).__name__}")
                    logger.error(f"Context attributes: {[attr for attr in dir(context) if not attr.startswith('_')][:10]}")
                    raise ValueError(f"Cannot determine handler for context type: {type(context).__name__}")

            except Exception as e:
                if "Client error status code" in str(e):
                    logger.debug(f"Skipping {url} due to client error: {e}")
                else:
                    logger.error(f"Handler error for {url}: {e}", exc_info=True)
            finally:
                await self.performance_monitor.end_operation(operation_id)

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
            
            # Log failure details for debugging
            logger.warning(f"Request failed - URL: {url}, Category: {error_category}, Reason: {error_reason}, Retries: {retry_count}")

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

    async def _extract_pdf_text_async(self, content: bytes, url: str) -> Tuple[str, Dict[str, Any], List]:
        """Extract text from PDF files asynchronously."""
        try:
            content_io = io.BytesIO(content)
            text, images = await asyncio.to_thread(read_pdf_file, content_io, f"pdf_from_{url}")
            return text, {"title": f"PDF Document from {url}"}, images or []
        except Exception as e:
            logger.error(f"Failed to extract text from PDF at {url}: {e}")
            return "", {}, []

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
    
    def get_performance_summary(self) -> str:
        """Get a summary of current performance metrics."""
        stats = self.performance_monitor.get_performance_stats()
        return self._format_performance_report(stats)
    
    @staticmethod
    def _handle_dump_signal(signum, frame):
        """Handle SIGUSR1 signal to dump thread state."""
        logger.info(f"Received signal {signum} - generating thread dump")
        try:
            dump = ThreadDumper.dump_all()
            
            # Write to file
            dump_file = f"/tmp/crawler_manual_dump_{int(time.time())}.txt"
            with open(dump_file, 'w') as f:
                f.write(dump)
            logger.info(f"Thread dump written to: {dump_file}")
            
            # Log summary
            logger.info("Thread dump summary:\n" + dump[:1000] + "\n... (truncated)")
            
            # Also trigger performance report for all instances
            for instance in AsyncWebConnectorCrawlee._instances:
                if instance.performance_monitor:
                    perf_report = instance.get_performance_summary()
                    logger.info(f"Performance report for instance:\n{perf_report}")
                    
        except Exception as e:
            logger.error(f"Error generating thread dump: {e}")
    
    def get_thread_dump(self) -> str:
        """Get current thread dump."""
        return ThreadDumper.dump_all()
    
    def save_thread_dump(self, filename: Optional[str] = None) -> str:
        """Save thread dump to file and return filename."""
        if not filename:
            filename = f"/tmp/crawler_dump_{int(time.time())}.txt"
        
        dump = self.get_thread_dump()
        with open(filename, 'w') as f:
            f.write(dump)
            f.write("\n\n" + "=" * 80 + "\n")
            f.write("PERFORMANCE REPORT\n")
            f.write("=" * 80 + "\n")
            f.write(self.get_performance_summary())
        
        logger.info(f"Thread dump saved to: {filename}")
        return filename

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

    async def _crawl_async(self) -> AsyncGenerator[List[Document], None]:
        """
        Main async crawl implementation that yields document batches.
        """
        # Start deadlock detection if not already running
        if not self._perf_monitor_task:
            self._perf_monitor_task = asyncio.create_task(self.performance_monitor.check_deadlocks())
        
        # Initialize request loader
        await self.performance_monitor.start_operation("init_loader", "request_loader_init")
        await self._initialize_request_loader()
        await self.performance_monitor.end_operation("init_loader")
        
        # Create crawler instance
        await self.performance_monitor.start_operation("create_crawler", "crawler_creation")
        crawler = await self._create_crawler()
        await self.performance_monitor.end_operation("create_crawler")
        
        # Create a queue for passing documents from crawler to batch yielder
        doc_queue = asyncio.Queue(maxsize=self.batch_size * 2)
        
        # Setup handlers with the queue
        await self.performance_monitor.start_operation("setup_handlers", "handler_setup")
        await self._setup_crawler_handlers(crawler, raw_items_queue=doc_queue)
        await self.performance_monitor.end_operation("setup_handlers")
        
        # Create dataset for results
        dataset = await Dataset.open()
        
        # Create a stop event for the batch processor
        stop_processing = asyncio.Event()
        
        # Create a task to run the crawler
        async def run_crawler():
            """Run the crawler and signal completion."""
            try:
                await self._run_crawler_internal(crawler, dataset)
            finally:
                stop_processing.set()
        
        # Start crawler task
        crawler_task = asyncio.create_task(run_crawler())
        
        # Process batches from queue while crawler runs
        items = []
        batch_start_time = time.perf_counter()
        last_yield_time = time.perf_counter()
        
        logger.info("Starting batch processor loop...")
        
        try:
            loop_iterations = 0
            while not crawler_task.done() or not doc_queue.empty():
                loop_iterations += 1
                # Log status periodically
                if loop_iterations % 10 == 0:
                    logger.info(f"Batch processor loop iteration {loop_iterations}: crawler_done={crawler_task.done()}, queue_size={doc_queue.qsize()}, items_buffered={len(items)}")
                try:
                    # Wait for documents with timeout
                    doc = await asyncio.wait_for(doc_queue.get(), timeout=1.0)
                    items.append(doc)
                    logger.info(f"Retrieved document from queue, buffer size: {len(items)}")
                    
                    # Yield batch when size is reached
                    if len(items) >= self.batch_size:
                        batch_duration = time.perf_counter() - batch_start_time
                        logger.info(f"Yielding batch of {len(items)} documents during crawl (processed in {batch_duration:.2f}s)")
                        yield items
                        items = []
                        batch_start_time = time.perf_counter()
                        last_yield_time = time.perf_counter()
                        
                except asyncio.TimeoutError:
                    # Check if we should yield partial batch
                    if items and (time.perf_counter() - last_yield_time) > 30:  # 30 second timeout
                        logger.info(f"Yielding partial batch of {len(items)} documents due to timeout")
                        yield items
                        items = []
                        batch_start_time = time.perf_counter()
                        last_yield_time = time.perf_counter()
                except Exception as e:
                    logger.error(f"Error processing document from queue: {e}")
            
            # Wait for crawler to complete
            logger.info(f"Waiting for crawler task to complete. Current buffer: {len(items)} items")
            await crawler_task
            logger.info(f"Crawler task completed. Buffer has {len(items)} items")
            
            # Yield remaining items from queue
            if items:
                logger.info(f"Yielding final batch of {len(items)} documents from queue")
                yield items
                
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            # Cancel crawler if still running
            if not crawler_task.done():
                crawler_task.cancel()
            raise
        
        # Also process any remaining items from the dataset
        # This handles documents that weren't pushed to the queue
        logger.info(f"Checking dataset for any remaining items...")
        remaining_count = 0
        items = []
        batch_start_time = time.perf_counter()
        
        async for item in dataset.iterate_items():
            remaining_count += 1
            doc = await self._convert_dataset_item_to_document(item)
            if doc:
                items.append(doc)
                
                # Yield batch when size is reached
                if len(items) >= self.batch_size:
                    logger.info(f"Yielding batch of {len(items)} documents from dataset")
                    yield items
                    items = []
                    batch_start_time = time.perf_counter()
        
        # Yield final batch
        if items:
            logger.info(f"Yielding final batch of {len(items)} documents from dataset")
            yield items
            
        if remaining_count > 0:
            logger.info(f"Processed {remaining_count} additional items from dataset")
        
        # Log final statistics
        perf_stats = self.performance_monitor.get_performance_stats()
        logger.info(f"Performance statistics: {perf_stats}")
        
        # Print performance report
        logger.info(f"Final performance report:\n{self._format_performance_report(perf_stats)}")
        
        logger.info(f"Crawl complete. Processed {self.processed_count} total items.")
        
    async def _run_crawler_internal(self, crawler, dataset):
        """Internal method to run the crawler."""
        try:
            crawl_start_time = time.perf_counter()
            
            if self.request_manager and hasattr(crawler, 'run_tandem'):
                # Use run_tandem for crawlers that support it (BeautifulSoup, Playwright)
                await self.performance_monitor.start_operation("crawl_tandem", "crawl_execution", {"mode": "tandem"})
                await crawler.run_tandem(self.request_manager)
                await self.performance_monitor.end_operation("crawl_tandem")
            elif self.request_manager:
                # For AdaptivePlaywrightCrawler, we can't use RequestManager directly
                # We need to extract URLs from the sitemap loader
                logger.info("Converting sitemap to URL list for adaptive crawler...")
                
                await self.performance_monitor.start_operation("extract_urls", "url_extraction", {"source": "sitemap"})
                urls = []
                
                # If we have a sitemap loader, get URLs from it
                if self.sitemap_loader:
                    # Extract URLs in batches for better performance
                    url_count = 0
                    last_progress_time = time.perf_counter()
                    extraction_start = time.perf_counter()
                    
                    # Batch extraction for efficiency
                    batch_size = 1000
                    tasks = []
                    
                    while True:
                        # Check if we've reached the URL limit
                        if self.sitemap_url_limit and url_count >= self.sitemap_url_limit:
                            logger.info(f"Reached sitemap URL limit of {self.sitemap_url_limit}")
                            break
                            
                        # Collect batch of requests
                        batch = []
                        remaining = batch_size
                        if self.sitemap_url_limit:
                            remaining = min(batch_size, self.sitemap_url_limit - url_count)
                            
                        for _ in range(remaining):
                            request = await self.sitemap_loader.fetch_next_request()
                            if request:
                                batch.append(request.url)
                            else:
                                break
                        
                        if not batch:
                            break
                            
                        urls.extend(batch)
                        url_count += len(batch)
                        
                        # Log progress
                        current_time = time.perf_counter()
                        if url_count % 10000 == 0 or (current_time - last_progress_time) > 30:
                            extraction_duration = current_time - extraction_start
                            rate = url_count / extraction_duration if extraction_duration > 0 else 0
                            logger.info(f"Extracted {url_count} URLs so far ({rate:.0f} URLs/sec)...")
                            last_progress_time = current_time
                            
                            # Check if extraction is taking too long
                            if extraction_duration > 300:  # 5 minutes
                                logger.warning("URL extraction taking very long - capturing diagnostic info")
                                self.save_thread_dump()
                
                await self.performance_monitor.end_operation("extract_urls")
                
                logger.info(f"Found {len(urls)} URLs from sitemap")
                if urls:
                    # Let Crawlee handle the URLs with its internal queueing system
                    # This is more efficient than manual chunking
                    self.total_urls = len(urls)
                    logger.info(f"Processing {self.total_urls} URLs from sitemap")
                    
                    await self.performance_monitor.start_operation("crawl_urls", "crawl_execution", 
                        {"mode": "sitemap_urls", "count": self.total_urls})
                    
                    try:
                        # For very large sitemaps, process in chunks to avoid file system overload
                        if len(urls) > 10000:
                            logger.warning(f"Large sitemap with {len(urls)} URLs detected. Processing in chunks...")
                            chunk_size = 5000  # Process 5000 URLs at a time
                            for i in range(0, len(urls), chunk_size):
                                chunk = urls[i:i + chunk_size]
                                logger.info(f"Processing chunk {i//chunk_size + 1}/{(len(urls) + chunk_size - 1)//chunk_size} with {len(chunk)} URLs")
                                
                                # Add timeout for each chunk
                                timeout_seconds = 600.0  # 10 minutes per chunk
                                crawl_task = asyncio.create_task(crawler.run(chunk))
                                try:
                                    await asyncio.wait_for(crawl_task, timeout=timeout_seconds)
                                except asyncio.TimeoutError:
                                    logger.warning(f"Chunk {i//chunk_size + 1} timed out after {timeout_seconds/60:.1f} minutes")
                                    # Continue with next chunk
                                
                                # Small delay between chunks to let file system catch up
                                await asyncio.sleep(1.0)
                        else:
                            # Pass all URLs to the crawler at once
                            # Crawlee will handle them efficiently with its internal queue
                            logger.info(f"Starting crawler.run() with {len(urls)} URLs")
                            logger.info(f"Queue status before crawler.run(): {'available' if self._raw_items_queue else 'NOT available'}")
                            
                            # Add timeout to prevent infinite hanging
                            # For large sitemaps, use a longer timeout
                            timeout_seconds = 3600.0 if len(urls) > 1000 else 300.0  # 1 hour for large, 5 min for small
                            logger.info(f"Using {timeout_seconds}s timeout for {len(urls)} URLs")
                            
                            crawl_task = asyncio.create_task(crawler.run(urls))
                            try:
                                await asyncio.wait_for(crawl_task, timeout=timeout_seconds)
                            except asyncio.TimeoutError:
                                logger.error(f"Crawler timed out after {timeout_seconds/60:.1f} minutes")
                            logger.error(f"Processed {self.processed_count} out of {len(urls)} URLs before timeout")
                            # Save diagnostic info
                            self.save_thread_dump()
                            # Don't raise - let it yield what was processed
                            logger.warning("Continuing with partial results after timeout")
                        
                        logger.info(f"Crawler completed. Processed {self.processed_count} out of {len(urls)} URLs")
                    except Exception as e:
                        logger.error(f"Error processing URLs: {e}")
                        raise
                    finally:
                        await self.performance_monitor.end_operation("crawl_urls")
                else:
                    logger.warning("No URLs found in sitemap")
                    # If no URLs found, try the original URL
                    if hasattr(self, 'base_url'):
                        logger.info(f"Falling back to crawling base URL: {self.base_url}")
                        await self.performance_monitor.start_operation("crawl_fallback", "crawl_execution", {"mode": "fallback"})
                        await crawler.run([self.base_url])
                        await self.performance_monitor.end_operation("crawl_fallback")
            elif self.request_loader:
                # RequestList can be passed directly to crawler.run()
                await self.performance_monitor.start_operation("crawl_loader", "crawl_execution", {"mode": "request_loader"})
                await crawler.run(self.request_loader)
                await self.performance_monitor.end_operation("crawl_loader")
            else:
                raise ValueError("No request loader or manager available")
            
            crawl_duration = time.perf_counter() - crawl_start_time
            logger.info(f"Crawl completed in {crawl_duration:.2f}s")
        except Exception as e:
            logger.error(f"Crawl failed: {e}")
            raise
        

    async def _convert_dataset_item_to_document(self, item: Dict[str, Any]) -> Optional[Document]:
        """Convert a dataset item to a Document object."""
        try:
            url = item.get("url", "")
            title = item.get("title", "")
            content = item.get("content", "")
            metadata = item.get("metadata", {})
            doc_updated_at_str = metadata.get("doc_updated_at")
            
            if not content or len(content.strip()) < 50:
                logger.debug(f"Skipping {url} due to insufficient content")
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

    async def _handle_static_page_async(self, context) -> None:
        """Handle static HTML pages using BeautifulSoup/Cheerio context."""
        url = context.request.url
        logger.info(f"_handle_static_page_async called for {url}")
        logger.info(f"Queue status: queue={'available' if self._raw_items_queue else 'NOT available'}")
        
        try:
            # Get soup - handle different context types
            if hasattr(context, 'soup'):
                # BeautifulSoupCrawler context
                soup = context.soup
                raw_html = str(soup)
            elif hasattr(context, 'body'):
                # CheerioCrawler context (from AdaptivePlaywrightCrawler in static mode)
                raw_html = context.body
                soup = BeautifulSoup(raw_html, 'html.parser')
            elif type(context).__name__ == "AdaptivePlaywrightCrawlingContext":
                # AdaptivePlaywrightCrawler context
                # According to docs, parsed_content is already a BeautifulSoup object when using with_beautifulsoup_static_parser
                try:
                    # Try to get parsed_content - it's already a BeautifulSoup object
                    soup = context.parsed_content
                    if soup:
                        logger.debug(f"Got parsed_content as BeautifulSoup: {type(soup)}")
                        raw_html = str(soup)
                    else:
                        logger.error(f"No parsed_content available for {url}")
                        return
                except Exception as e:
                    logger.debug(f"Error accessing parsed_content for {url}: {e}")
                    # Adaptive crawler might be in dynamic mode, not static
                    logger.error(f"Cannot access parsed_content - might be in dynamic mode")
                    return
            else:
                logger.error(f"No soup, body, or parsed_content available for {url}")
                return
            
            # Extract content using configured extractors
            title, text_content, metadata = self._extract_content(url, soup, raw_html)
            
            # Temporarily lower threshold to debug
            if not text_content:
                logger.warning(f"No content extracted from {url}")
                return
            
            logger.info(f"Extracted content from {url} - length: {len(text_content.strip())}")
            logger.debug(f"Content preview: {text_content[:500] if text_content else 'None'}")
            
            # Check for duplicates
            is_duplicate, content_hash = await self._is_content_duplicate(title, text_content, url)
            if is_duplicate:
                logger.debug(f"Skipping duplicate content at {url}")
                return
            
            # Get last modified date from headers if available
            doc_updated_at = None
            if hasattr(context, "response") and hasattr(context.response, "headers"):
                doc_updated_at = self._parse_last_modified(context.response.headers)
            
            # Push to dataset
            dataset_metadata = {
                **metadata,
                "content_hash": content_hash,
            }
            if doc_updated_at:
                dataset_metadata["doc_updated_at"] = doc_updated_at.isoformat()
            
            data_item = {
                "url": url,
                "title": title,
                "content": text_content,
                "metadata": dataset_metadata,
            }
            logger.debug(f"Pushing data to dataset for {url}")
            await context.push_data(data_item)
            logger.info(f"Successfully pushed {url} to dataset")
            
            # If we have a queue, also convert and push the document for immediate batch yielding
            if self._raw_items_queue:
                logger.info(f"Queue is available, pushing document for {url}")
                try:
                    doc = await self._convert_dataset_item_to_document(data_item)
                    if doc:
                        await self._raw_items_queue.put(doc)
                        logger.info(f"Pushed document to queue for {url} (queue size: {self._raw_items_queue.qsize()})")
                    else:
                        logger.warning(f"Document conversion returned None for {url}")
                except asyncio.QueueFull:
                    logger.warning(f"Document queue full, skipping immediate yield for {url}")
                except Exception as e:
                    logger.error(f"Error pushing to queue: {e}")
            else:
                logger.warning(f"Queue NOT available for {url} - documents will only be available after crawl completes")
            
            # Handle link discovery for recursive crawling
            if self.recursive:
                # Crawlee will automatically discover and enqueue links when we call enqueue_links
                await context.enqueue_links()
                
        except Exception as e:
            logger.error(f"Error handling static page {url}: {e}")
            raise

    async def _handle_adaptive_page_async(self, context) -> None:
        """Handle dynamic pages using Playwright context."""
        url = context.request.url
        
        try:
            # Get the page object (with error handling for adaptive crawler)
            try:
                page = context.page
            except (AttributeError, RuntimeError) as e:
                logger.error(f"Cannot access page for {url}: {e}")
                raise
            
            # Wait for content to load
            await page.wait_for_load_state("networkidle", timeout=30000)
            
            # Get page content
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            
            # Extract content using configured extractors
            title, text_content, metadata = self._extract_content(url, soup, content)
            
            if not text_content or len(text_content.strip()) < 50:
                logger.debug(f"Skipping {url} due to insufficient content")
                return
            
            # Check for duplicates
            is_duplicate, content_hash = await self._is_content_duplicate(title, text_content, url)
            if is_duplicate:
                logger.debug(f"Skipping duplicate content at {url}")
                return
            
            # Try to get last modified from page metadata
            doc_updated_at = None
            
            # Push to dataset
            dataset_metadata = {
                **metadata,
                "content_hash": content_hash,
            }
            if doc_updated_at:
                dataset_metadata["doc_updated_at"] = doc_updated_at.isoformat()
            
            data_item = {
                "url": url,
                "title": title,
                "content": text_content,
                "metadata": dataset_metadata,
            }
            
            await context.push_data(data_item)
            logger.info(f"Successfully pushed {url} to dataset")
            
            # If we have a queue, also convert and push the document for immediate batch yielding
            if self._raw_items_queue:
                logger.info(f"Queue is available, pushing document for {url}")
                try:
                    doc = await self._convert_dataset_item_to_document(data_item)
                    if doc:
                        await self._raw_items_queue.put(doc)
                        logger.info(f"Pushed document to queue for {url} (queue size: {self._raw_items_queue.qsize()})")
                    else:
                        logger.warning(f"Document conversion returned None for {url}")
                except asyncio.QueueFull:
                    logger.warning(f"Document queue full, skipping immediate yield for {url}")
                except Exception as e:
                    logger.error(f"Error pushing to queue: {e}")
            else:
                logger.warning(f"Queue NOT available for {url} - documents will only be available after crawl completes")
            
            # Handle link discovery for recursive crawling
            if self.recursive:
                # Crawlee will automatically discover and enqueue links when we call enqueue_links
                await context.enqueue_links()
                
        except Exception as e:
            logger.error(f"Error handling adaptive page {url}: {e}")
            raise

    def _format_performance_report(self, stats: Dict[str, Any]) -> str:
        """Format performance statistics into a readable report."""
        lines = ["=" * 60]
        lines.append("PERFORMANCE REPORT")
        lines.append("=" * 60)
        
        # Operation statistics
        for op_type, op_stats in stats.items():
            if isinstance(op_stats, dict) and 'count' in op_stats:
                lines.append(f"\n{op_type}:")
                lines.append(f"  Count: {op_stats['count']}")
                lines.append(f"  Avg: {op_stats['avg']:.2f}s")
                lines.append(f"  Min: {op_stats['min']:.2f}s")
                lines.append(f"  Max: {op_stats['max']:.2f}s")
                lines.append(f"  P95: {op_stats['p95']:.2f}s")
        
        # Active operations
        if 'active_operations' in stats:
            lines.append(f"\nActive Operations: {stats['active_operations']}")
            if stats['active_operations'] > 0 and 'active_operation_details' in stats:
                lines.append("Active Operation Details:")
                for op in stats['active_operation_details']:
                    lines.append(f"  - {op['type']} ({op['id'][:20]}...): {op['duration']:.2f}s")
                    if op['metadata']:
                        lines.append(f"    Metadata: {op['metadata']}")
        
        lines.append("=" * 60)
        return "\n".join(lines)
    
    async def _get_metrics_snapshot(self) -> dict[str, Any]:
        """Get a snapshot of current metrics."""
        # Initialize metrics if not exists
        if not hasattr(self, "_metrics"):
            self._metrics = {
                "pages_crawled": 0,
                "documents_generated": 0,
                "errors": 0,
                "start_time": datetime.now(timezone.utc),
            }
        
        # Calculate runtime
        runtime = datetime.now(timezone.utc) - self._metrics["start_time"]
        
        return {
            **self._metrics,
            "runtime_seconds": runtime.total_seconds(),
            "failed_urls": len(self._failed_urls),
        }
