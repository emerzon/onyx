"""
Type definitions and data classes for web connector.
Provides type safety and structured data models.
"""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class ContentType(str, Enum):
    """Content types detected by the web connector."""
    HTML = "html"
    PDF = "pdf"
    DOCX = "docx"
    DOC = "doc"
    XLSX = "xlsx"
    XLS = "xls"
    PPTX = "pptx"
    PPT = "ppt"
    TXT = "txt"
    CSV = "csv"
    IMAGE = "image"


class CrawlStatus(str, Enum):
    """Status of a crawl operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ErrorCategory(str, Enum):
    """Categories of errors that can occur during crawling."""
    HTTP_ERROR = "http_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    BROWSER_ERROR = "browser_error"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CrawlRequest:
    """Represents a request to crawl a URL."""
    url: str
    crawl_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL format: {self.url}")


@dataclass
class ProcessedContent:
    """Represents processed content from a web page."""
    url: str
    title: Optional[str]
    content: str
    content_type: ContentType
    content_hash: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    file_size: Optional[int] = None
    doc_updated_at: Optional[str] = None
    
    @property
    def is_empty(self) -> bool:
        """Check if content is effectively empty."""
        return not self.content or len(self.content.strip()) < 50


@dataclass
class CrawlError:
    """Represents an error that occurred during crawling."""
    url: str
    error_category: ErrorCategory
    error_message: str
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())
    retry_count: int = 0
    
    @classmethod
    def from_exception(
        cls, 
        url: str, 
        error: Exception, 
        retry_count: int = 0
    ) -> 'CrawlError':
        """Create CrawlError from an exception."""
        error_msg = str(error).lower()
        
        # Define error patterns and their corresponding categories
        error_patterns = [
            (["status code:"], ErrorCategory.HTTP_ERROR),
            (["timeout", "timed out"], ErrorCategory.TIMEOUT),
            (["connection", "connect", "network"], ErrorCategory.NETWORK_ERROR),
            (["playwright"], ErrorCategory.BROWSER_ERROR),
            (["cancelled"], ErrorCategory.CANCELLED),
        ]
        
        # Find matching category
        category = ErrorCategory.UNKNOWN
        for keywords, error_category in error_patterns:
            if any(keyword in error_msg for keyword in keywords):
                category = error_category
                break
        
        return cls(
            url=url,
            error_category=category,
            error_message=str(error)[:200],  # Truncate long messages
            retry_count=retry_count
        )


@dataclass
class PageInfo:
    """Information about a web page."""
    url: str
    title: Optional[str] = None
    content_length: int = 0
    content_type: Optional[str] = None
    last_modified: Optional[datetime] = None
    status_code: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class ExtractionResult:
    """Result of content extraction operation."""
    success: bool
    content: Optional[ProcessedContent] = None
    error: Optional[CrawlError] = None
    processing_time: float = 0.0
    
    @classmethod
    def success_result(
        cls, 
        content: ProcessedContent, 
        processing_time: float = 0.0
    ) -> 'ExtractionResult':
        """Create a successful extraction result."""
        return cls(
            success=True,
            content=content,
            processing_time=processing_time
        )
    
    @classmethod
    def error_result(
        cls, 
        error: CrawlError, 
        processing_time: float = 0.0
    ) -> 'ExtractionResult':
        """Create a failed extraction result."""
        return cls(
            success=False,
            error=error,
            processing_time=processing_time
        )


@dataclass
class CrawlSession:
    """Represents a crawl session with state and configuration."""
    session_id: str
    start_time: datetime
    status: CrawlStatus = CrawlStatus.PENDING
    base_url: str = ""
    crawl_type: str = ""
    end_time: Optional[datetime] = None
    total_pages: int = 0
    processed_pages: int = 0
    failed_pages: int = 0
    
    @property
    def duration(self) -> Optional[float]:
        """Get session duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def is_active(self) -> bool:
        """Check if session is currently active."""
        return self.status == CrawlStatus.IN_PROGRESS


@dataclass
class ValidationResult:
    """Result of configuration validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
    
    @property
    def has_issues(self) -> bool:
        """Check if there are any errors or warnings."""
        return bool(self.errors or self.warnings)