"""
Configuration management module for web connector.
Handles configuration validation and settings management.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Any
from urllib.parse import urlparse

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from shared_configs.configs import MULTI_TENANT
from onyx.connectors.exceptions import ConnectorValidationError
from onyx.utils.logger import setup_logger


logger = setup_logger()


class WEB_CONNECTOR_TYPE(str, Enum):
    """Valid web connector types."""
    RECURSIVE = "recursive"
    SINGLE = "single"
    SITEMAP = "sitemap"
    UPLOAD = "upload"


class CRAWLER_MODE(str, Enum):
    """Valid crawler modes."""
    ADAPTIVE = "adaptive"
    STATIC = "static"
    DYNAMIC = "dynamic"


@dataclass
class WebConnectorConfig:
    """Configuration for web connector."""
    # Required settings
    base_url: str
    web_connector_type: str = WEB_CONNECTOR_TYPE.RECURSIVE.value
    
    # Crawling settings
    batch_size: int = INDEX_BATCH_SIZE
    crawler_mode: str = CRAWLER_MODE.ADAPTIVE.value
    recursive: bool = False
    
    # Content settings
    mintlify_cleanup: bool = True
    skip_images: bool = True
    enable_specialized_extraction: bool = True
    
    # Authentication
    oauth_token: Optional[str] = None
    
    # Selector configuration
    selector_config: Optional[str] = None
    selector_config_file: Optional[str] = None
    
    # Performance settings
    max_concurrency: int = 10
    max_requests_per_minute: int = 300
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._normalize_url()
    
    def _normalize_url(self) -> None:
        """Normalize the base URL."""
        if self.base_url and "://" not in self.base_url:
            self.base_url = f"https://{self.base_url}"


class ConfigurationManager:
    """
    Manages configuration validation and settings for web connector.
    Extracted from AsyncWebConnectorCrawlee for better organization.
    """
    
    def __init__(self, config: WebConnectorConfig):
        self.config = config
        self._validation_errors: List[str] = []
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return list of validation errors.
        
        Returns:
            List of validation error messages (empty if valid)
        """
        self._validation_errors = []
        
        # Run all validation checks
        self._validate_base_url()
        self._validate_connector_type()
        self._validate_crawler_mode()
        self._validate_batch_size()
        self._validate_selector_config()
        self._validate_upload_constraints()
        
        return self._validation_errors
    
    def validate_or_raise(self) -> None:
        """
        Validate configuration and raise exception if invalid.
        
        Raises:
            ConnectorValidationError: If validation fails
        """
        errors = self.validate()
        if errors:
            raise ConnectorValidationError(
                f"Configuration validation failed: {'; '.join(errors)}"
            )
    
    def _validate_base_url(self) -> None:
        """Validate base URL format."""
        if not self.config.base_url:
            self._validation_errors.append("Base URL is required")
            return
        
        # URL format validation
        parsed = urlparse(self.config.base_url)
        if not parsed.scheme or not parsed.netloc:
            self._validation_errors.append(
                f"Invalid URL format: {self.config.base_url}"
            )
    
    def _validate_connector_type(self) -> None:
        """Validate connector type."""
        valid_types = [e.value for e in WEB_CONNECTOR_TYPE]
        if self.config.web_connector_type not in valid_types:
            self._validation_errors.append(
                f"Invalid connector type '{self.config.web_connector_type}'. "
                f"Must be one of: {valid_types}"
            )
    
    def _validate_crawler_mode(self) -> None:
        """Validate crawler mode."""
        valid_modes = [e.value for e in CRAWLER_MODE]
        if self.config.crawler_mode not in valid_modes:
            self._validation_errors.append(
                f"Invalid crawler mode '{self.config.crawler_mode}'. "
                f"Must be one of: {valid_modes}"
            )
    
    def _validate_batch_size(self) -> None:
        """Validate batch size."""
        if self.config.batch_size <= 0:
            self._validation_errors.append("Batch size must be positive")
        elif self.config.batch_size > 1000:
            logger.warning(
                f"Large batch size ({self.config.batch_size}) may cause memory issues"
            )
    
    def _validate_selector_config(self) -> None:
        """Validate selector configuration."""
        if self.config.selector_config and self.config.selector_config_file:
            self._validation_errors.append(
                "Cannot specify both selector_config and selector_config_file"
            )
    
    def _validate_upload_constraints(self) -> None:
        """Validate file upload constraints."""
        if self.config.web_connector_type == WEB_CONNECTOR_TYPE.UPLOAD.value:
            if MULTI_TENANT:
                self._validation_errors.append(
                    "Upload input for web connector is not supported in cloud environments"
                )
    
    def get_exclude_patterns(self) -> List[str]:
        """
        Generate URL exclude patterns based on configuration.
        
        Returns:
            List of URL patterns to exclude
        """
        exclude_patterns = []
        
        # Skip images if configured
        if self.config.skip_images:
            exclude_patterns.extend([
                "**/*.png", "**/*.jpg", "**/*.jpeg", "**/*.gif",
                "**/*.bmp", "**/*.tiff", "**/*.ico", "**/*.svg", "**/*.webp"
            ])
        
        # Skip unsupported file types
        exclude_patterns.extend([
            # Archives
            "**/*.zip", "**/*.rar", "**/*.7z", "**/*.tar", "**/*.gz", "**/*.bz2",
            # Media files
            "**/*.mp4", "**/*.avi", "**/*.mov", "**/*.wmv", "**/*.flv", "**/*.webm",
            "**/*.mp3", "**/*.wav", "**/*.flac", "**/*.ogg", "**/*.m4a",
            # Executables
            "**/*.exe", "**/*.msi", "**/*.deb", "**/*.rpm", "**/*.dmg",
            # Disk images
            "**/*.bin", "**/*.iso", "**/*.img"
        ])
        
        return exclude_patterns
    
    def is_binary_file_supported(self, url: str) -> bool:
        """Check if URL points to a supported binary file."""
        supported_extensions = {
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'
        }
        url_lower = url.lower()
        return any(url_lower.endswith(ext) for ext in supported_extensions)
    
    @classmethod
    def from_dict(cls, settings: dict[str, Any]) -> 'ConfigurationManager':
        """
        Create ConfigurationManager from dictionary settings.
        
        Args:
            settings: Dictionary of configuration settings
            
        Returns:
            ConfigurationManager instance
        """
        config = WebConnectorConfig(**settings)
        return cls(config)