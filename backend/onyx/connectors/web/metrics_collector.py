"""
Metrics collection module for web connector.
Handles async-safe metrics tracking and reporting.
"""
import asyncio
import time
from typing import Any, Dict, Optional
from dataclasses import dataclass, field

from onyx.utils.logger import setup_logger


logger = setup_logger()


@dataclass
class CrawlMetrics:
    """Data class for crawl metrics."""
    crawl_start_time: Optional[float] = None
    pages_processed: int = 0
    pages_successful: int = 0
    pages_failed: int = 0
    pages_skipped: int = 0
    documents_created: int = 0
    duplicates_found: int = 0
    bytes_processed: int = 0
    avg_processing_time: float = 0.0
    error_categories: Dict[str, int] = field(default_factory=dict)
    
    # Derived metrics
    elapsed_time: float = 0.0
    pages_per_second: float = 0.0
    documents_per_second: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    skip_rate: float = 0.0
    
    def calculate_derived_metrics(self) -> None:
        """Calculate derived metrics from base metrics."""
        if self.crawl_start_time:
            self.elapsed_time = time.time() - self.crawl_start_time
            if self.elapsed_time > 0:
                self.pages_per_second = self.pages_processed / self.elapsed_time
                self.documents_per_second = self.documents_created / self.elapsed_time
        
        if self.pages_processed > 0:
            self.success_rate = self.pages_successful / self.pages_processed
            self.failure_rate = self.pages_failed / self.pages_processed
            self.skip_rate = self.pages_skipped / self.pages_processed


class MetricsCollector:
    """
    Thread-safe metrics collection for web crawling.
    Extracted from AsyncWebConnectorCrawlee for better testability.
    """
    
    def __init__(self):
        self._metrics = CrawlMetrics()
        self._metrics_lock = asyncio.Lock()
    
    async def start_crawl(self) -> None:
        """Mark the start of a crawl."""
        async with self._metrics_lock:
            self._metrics.crawl_start_time = time.time()
    
    async def update_metric(
        self, 
        metric_name: str, 
        value: Any = 1, 
        operation: str = "increment"
    ) -> None:
        """
        Update crawler metrics in thread-safe manner.
        
        Args:
            metric_name: Name of the metric to update
            value: Value to add/set (default: 1)
            operation: 'increment', 'set', or 'average'
        """
        async with self._metrics_lock:
            if operation == "increment":
                current = getattr(self._metrics, metric_name, 0)
                setattr(self._metrics, metric_name, current + value)
            elif operation == "set":
                setattr(self._metrics, metric_name, value)
            elif operation == "average":
                # Simple moving average for processing time
                if metric_name == "avg_processing_time":
                    current_avg = self._metrics.avg_processing_time
                    count = self._metrics.pages_processed
                    if count > 0:
                        self._metrics.avg_processing_time = (
                            (current_avg * (count - 1) + value) / count
                        )
    
    async def increment_error_category(self, category: str) -> None:
        """Increment error count for a specific category."""
        async with self._metrics_lock:
            if category not in self._metrics.error_categories:
                self._metrics.error_categories[category] = 0
            self._metrics.error_categories[category] += 1
    
    async def record_page_processed(
        self, 
        success: bool = True, 
        bytes_processed: int = 0,
        processing_time: Optional[float] = None
    ) -> None:
        """Record that a page was processed."""
        async with self._metrics_lock:
            self._metrics.pages_processed += 1
            if success:
                self._metrics.pages_successful += 1
            else:
                self._metrics.pages_failed += 1
            
            if bytes_processed > 0:
                self._metrics.bytes_processed += bytes_processed
            
            if processing_time is not None:
                # Update average processing time
                count = self._metrics.pages_processed
                current_avg = self._metrics.avg_processing_time
                self._metrics.avg_processing_time = (
                    (current_avg * (count - 1) + processing_time) / count
                )
    
    async def record_document_created(self) -> None:
        """Record that a document was created."""
        async with self._metrics_lock:
            self._metrics.documents_created += 1
    
    async def record_duplicate_found(self) -> None:
        """Record that duplicate content was found."""
        async with self._metrics_lock:
            self._metrics.duplicates_found += 1
    
    async def record_page_skipped(self) -> None:
        """Record that a page was skipped."""
        async with self._metrics_lock:
            self._metrics.pages_skipped += 1
    
    async def get_metrics_snapshot(self) -> CrawlMetrics:
        """Get current metrics snapshot with calculated derived metrics."""
        async with self._metrics_lock:
            # Create a copy of metrics
            snapshot = CrawlMetrics(
                crawl_start_time=self._metrics.crawl_start_time,
                pages_processed=self._metrics.pages_processed,
                pages_successful=self._metrics.pages_successful,
                pages_failed=self._metrics.pages_failed,
                pages_skipped=self._metrics.pages_skipped,
                documents_created=self._metrics.documents_created,
                duplicates_found=self._metrics.duplicates_found,
                bytes_processed=self._metrics.bytes_processed,
                avg_processing_time=self._metrics.avg_processing_time,
                error_categories=self._metrics.error_categories.copy()
            )
            
            # Calculate derived metrics
            snapshot.calculate_derived_metrics()
            
            return snapshot
    
    async def log_progress(self, force: bool = False) -> None:
        """Log current progress metrics."""
        metrics = await self.get_metrics_snapshot()
        
        # Only log if significant progress or forced
        if force or metrics.pages_processed % 50 == 0:
            logger.info(
                f"Crawl Progress - "
                f"Processed: {metrics.pages_processed} pages, "
                f"Created: {metrics.documents_created} documents, "
                f"Failed: {metrics.pages_failed}, "
                f"Skipped: {metrics.pages_skipped}, "
                f"Rate: {metrics.pages_per_second:.1f} pages/sec, "
                f"Success: {metrics.success_rate:.1%}"
            )
            
            if metrics.error_categories:
                error_summary = ", ".join([
                    f"{cat}:{count}" 
                    for cat, count in metrics.error_categories.items()
                ])
                logger.info(f"Error breakdown: {error_summary}")
    
    async def log_final_metrics(self) -> None:
        """Log final crawl metrics."""
        metrics = await self.get_metrics_snapshot()
        
        logger.info(
            f"Final Crawl Metrics - "
            f"Duration: {metrics.elapsed_time:.1f}s, "
            f"Total Pages: {metrics.pages_processed}, "
            f"Documents: {metrics.documents_created}, "
            f"Success Rate: {metrics.success_rate:.1%}, "
            f"Avg Processing: {metrics.avg_processing_time:.2f}s/page, "
            f"Data Processed: {metrics.bytes_processed / (1024*1024):.1f}MB"
        )
        
        if metrics.error_categories:
            logger.info(
                f"Error Summary - " + ", ".join([
                    f"{cat}: {count}" 
                    for cat, count in metrics.error_categories.items()
                ])
            )