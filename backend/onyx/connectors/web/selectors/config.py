"""
Configuration classes for generic web content extraction selectors.
"""

import json
import yaml
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from pathlib import Path

from onyx.utils.logger import setup_logger

logger = setup_logger()


class SelectorType(Enum):
    """Types of selectors supported by the generic extractor."""
    CONTENT = "content"
    NAVIGATION = "navigation"
    METADATA = "metadata"
    DETECTION = "detection"
    EXCLUSION = "exclusion"
    WAIT = "wait"


@dataclass
class ContentSelectors:
    """Selectors for extracting page content."""
    title: Optional[str] = None
    body: Optional[str] = None
    text: Optional[str] = None
    description: Optional[str] = None
    
    # Fallback selectors if primary ones fail
    title_fallback: List[str] = field(default_factory=lambda: [
        'h1', '.title', '.page-title', 'title'
    ])
    body_fallback: List[str] = field(default_factory=lambda: [
        'main', 'article', '.content', '.main-content', '.post-content'
    ])


@dataclass
class NavigationSelectors:
    """Selectors for extracting navigation elements."""
    links: Optional[str] = None
    pagination: Optional[str] = None
    next_page: Optional[str] = None
    breadcrumbs: Optional[str] = None
    
    # Default navigation selectors
    links_fallback: List[str] = field(default_factory=lambda: [
        'a[href]'  # This should find all links on the page
    ])
    pagination_fallback: List[str] = field(default_factory=lambda: [
        '.pagination a', '.pager a', '.page-numbers a'
    ])


@dataclass
class MetadataSelectors:
    """Selectors for extracting page metadata."""
    date: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[str] = None
    
    # Default metadata selectors
    date_fallback: List[str] = field(default_factory=lambda: [
        '.date', '.published', 'time[datetime]', '[datetime]'
    ])
    author_fallback: List[str] = field(default_factory=lambda: [
        '.author', '.byline', '.writer', '[rel="author"]'
    ])


@dataclass
class DetectionSelectors:
    """Selectors for detecting if this configuration applies to a page."""
    required: List[str] = field(default_factory=list)
    optional: List[str] = field(default_factory=list)


@dataclass
class ExclusionSelectors:
    """Selectors for elements to exclude from extraction."""
    remove: List[str] = field(default_factory=lambda: [
        'nav', 'header', 'footer', '.navigation', '.sidebar', '.breadcrumb',
        '.ad', '.advertisement', '.social-share', 'script', 'style'
    ])
    skip_if_contains: List[str] = field(default_factory=list)


@dataclass
class WaitSelectors:
    """Selectors to wait for when loading dynamic content."""
    required: List[str] = field(default_factory=list)
    optional: List[str] = field(default_factory=list)
    timeout: float = 10.0


@dataclass
class SelectorConfig:
    """Complete configuration for generic web content extraction."""
    name: str = "generic"
    version: str = "1.0"
    
    # Core selector groups
    content: ContentSelectors = field(default_factory=ContentSelectors)
    navigation: NavigationSelectors = field(default_factory=NavigationSelectors)
    metadata: MetadataSelectors = field(default_factory=MetadataSelectors)
    detection: DetectionSelectors = field(default_factory=DetectionSelectors)
    exclusion: ExclusionSelectors = field(default_factory=ExclusionSelectors)
    wait: WaitSelectors = field(default_factory=WaitSelectors)
    
    # Additional configuration
    priority: int = 0  # Higher priority configs override lower ones
    enabled: bool = True
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SelectorConfig':
        """Create SelectorConfig from dictionary with simplified format support."""
        try:
            # Extract main fields
            name = data.get('name', 'generic')
            version = data.get('version', '1.0')
            priority = data.get('priority', 0)
            enabled = data.get('enabled', True)
            
            # Handle simplified format
            if 'title' in data and isinstance(data['title'], list):
                return cls._from_simplified_dict(data)
            
            # Parse legacy content selectors format
            content_data = data.get('content', {})
            content = ContentSelectors(
                title=content_data.get('title'),
                body=content_data.get('body'),
                text=content_data.get('text'),
                description=content_data.get('description'),
                title_fallback=content_data.get('title_fallback', ContentSelectors().title_fallback),
                body_fallback=content_data.get('body_fallback', ContentSelectors().body_fallback)
            )
            
            # Parse navigation selectors
            nav_data = data.get('navigation', {})
            navigation = NavigationSelectors(
                links=nav_data.get('links'),
                pagination=nav_data.get('pagination'),
                next_page=nav_data.get('next_page'),
                breadcrumbs=nav_data.get('breadcrumbs'),
                links_fallback=nav_data.get('links_fallback', NavigationSelectors().links_fallback),
                pagination_fallback=nav_data.get('pagination_fallback', NavigationSelectors().pagination_fallback)
            )
            
            # Parse metadata selectors
            meta_data = data.get('metadata', {})
            metadata = MetadataSelectors(
                date=meta_data.get('date'),
                author=meta_data.get('author'),
                category=meta_data.get('category'),
                tags=meta_data.get('tags'),
                date_fallback=meta_data.get('date_fallback', MetadataSelectors().date_fallback),
                author_fallback=meta_data.get('author_fallback', MetadataSelectors().author_fallback)
            )
            
            # Parse detection selectors
            detect_data = data.get('detection', {})
            detection = DetectionSelectors(
                required=detect_data.get('required', []),
                optional=detect_data.get('optional', [])
            )
            
            # Parse exclusion selectors
            exclude_data = data.get('exclusion', {})
            exclusion = ExclusionSelectors(
                remove=exclude_data.get('remove', ExclusionSelectors().remove),
                skip_if_contains=exclude_data.get('skip_if_contains', [])
            )
            
            # Parse wait selectors
            wait_data = data.get('wait', {})
            wait = WaitSelectors(
                required=wait_data.get('required', []),
                optional=wait_data.get('optional', []),
                timeout=wait_data.get('timeout', 10.0)
            )
            
            return cls(
                name=name,
                version=version,
                content=content,
                navigation=navigation,
                metadata=metadata,
                detection=detection,
                exclusion=exclusion,
                wait=wait,
                priority=priority,
                enabled=enabled
            )
            
        except Exception as e:
            logger.error(f"Error parsing selector config: {e}")
            raise ValueError(f"Invalid selector configuration: {e}")
    
    @classmethod
    def _from_simplified_dict(cls, data: Dict[str, Any]) -> 'SelectorConfig':
        """Create SelectorConfig from simplified dictionary format."""
        name = data.get('name', 'generic')
        version = data.get('version', '1.0')
        priority = data.get('priority', 0)
        enabled = data.get('enabled', True)
        
        # Title selectors - use first as primary, rest as fallback
        title_selectors = data.get('title', [])
        title_primary = title_selectors[0] if title_selectors else None
        title_fallback = title_selectors[1:] if len(title_selectors) > 1 else []
        
        # Body selectors - use first as primary, rest as fallback for multi-selector handling
        body_selectors = data.get('body', [])
        body_primary = body_selectors[0] if body_selectors else None
        body_fallback = body_selectors[1:] if len(body_selectors) > 1 else []
        
        # Parse content selectors
        content = ContentSelectors(
            title=title_primary,
            body=body_primary,
            title_fallback=title_fallback,
            body_fallback=body_fallback
        )
        
        # Parse navigation - simplified format doesn't include navigation
        navigation = NavigationSelectors()
        
        # Parse metadata - support both string and list formats
        metadata_data = data.get('metadata', {})
        metadata_fields = {}
        
        for field in ['date', 'author', 'category', 'tags']:
            value = metadata_data.get(field)
            if isinstance(value, list):
                metadata_fields[field] = value[0] if value else None
                metadata_fields[f'{field}_fallback'] = value[1:] if len(value) > 1 else []
            else:
                metadata_fields[field] = value
        
        metadata = MetadataSelectors(
            date=metadata_fields.get('date'),
            author=metadata_fields.get('author'),
            category=metadata_fields.get('category'),
            tags=metadata_fields.get('tags'),
            date_fallback=metadata_fields.get('date_fallback', []),
            author_fallback=metadata_fields.get('author_fallback', [])
        )
        
        # Parse detection - simplified format
        detection_list = data.get('detection', [])
        
        detection = DetectionSelectors(
            required=detection_list,
            optional=[]
        )
        
        # Parse exclusion - simplified format (optional)
        exclusion_list = data.get('exclude', [])
        exclusion = ExclusionSelectors(
            remove=exclusion_list,
            skip_if_contains=[]
        )
        
        # Wait selectors - use defaults for simplified format
        wait = WaitSelectors()
        
        return cls(
            name=name,
            version=version,
            content=content,
            navigation=navigation,
            metadata=metadata,
            detection=detection,
            exclusion=exclusion,
            wait=wait,
            priority=priority,
            enabled=enabled
        )
    
    @classmethod
    def from_json(cls, json_data: str) -> 'SelectorConfig':
        """Create SelectorConfig from JSON string."""
        try:
            data = json.loads(json_data)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in selector config: {e}")
            raise ValueError(f"Invalid JSON format: {e}")
    
    @classmethod
    def from_yaml(cls, yaml_data: str) -> 'SelectorConfig':
        """Create SelectorConfig from YAML string."""
        try:
            data = yaml.safe_load(yaml_data)
            return cls.from_dict(data)
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in selector config: {e}")
            raise ValueError(f"Invalid YAML format: {e}")
    
    @classmethod
    def from_file(cls, file_path: Union[str, Path]) -> 'SelectorConfig':
        """Load SelectorConfig from file (JSON or YAML)."""
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Selector config file not found: {file_path}")
        
        try:
            content = path.read_text(encoding='utf-8')
            
            if path.suffix.lower() in ['.json']:
                return cls.from_json(content)
            elif path.suffix.lower() in ['.yaml', '.yml']:
                return cls.from_yaml(content)
            else:
                # Try to detect format from content
                content_stripped = content.strip()
                if content_stripped.startswith('{'):
                    return cls.from_json(content)
                else:
                    return cls.from_yaml(content)
                    
        except Exception as e:
            logger.error(f"Error loading selector config from {file_path}: {e}")
            raise ValueError(f"Failed to load selector config: {e}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert SelectorConfig to dictionary."""
        return {
            'name': self.name,
            'version': self.version,
            'priority': self.priority,
            'enabled': self.enabled,
            'content': {
                'title': self.content.title,
                'body': self.content.body,
                'text': self.content.text,
                'description': self.content.description,
                'title_fallback': self.content.title_fallback,
                'body_fallback': self.content.body_fallback
            },
            'navigation': {
                'links': self.navigation.links,
                'pagination': self.navigation.pagination,
                'next_page': self.navigation.next_page,
                'breadcrumbs': self.navigation.breadcrumbs,
                'links_fallback': self.navigation.links_fallback,
                'pagination_fallback': self.navigation.pagination_fallback
            },
            'metadata': {
                'date': self.metadata.date,
                'author': self.metadata.author,
                'category': self.metadata.category,
                'tags': self.metadata.tags,
                'date_fallback': self.metadata.date_fallback,
                'author_fallback': self.metadata.author_fallback
            },
            'detection': {
                'required': self.detection.required,
                'optional': self.detection.optional
            },
            'exclusion': {
                'remove': self.exclusion.remove,
                'skip_if_contains': self.exclusion.skip_if_contains
            },
            'wait': {
                'required': self.wait.required,
                'optional': self.wait.optional,
                'timeout': self.wait.timeout
            }
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert SelectorConfig to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    def to_yaml(self) -> str:
        """Convert SelectorConfig to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False)
    
    def validate(self) -> List[str]:
        """Validate selector configuration and return list of issues."""
        issues = []
        
        # Check if at least one content selector is provided
        if not any([self.content.title, self.content.body, self.content.text]):
            issues.append("At least one content selector (title, body, or text) should be provided")
        
        # Validate CSS selectors (basic validation)
        for selector_type, selectors in [
            ("content.title", [self.content.title]),
            ("content.body", [self.content.body]),
            ("content.text", [self.content.text]),
            ("navigation.links", [self.navigation.links]),
            ("navigation.pagination", [self.navigation.pagination]),
        ]:
            for selector in selectors:
                if selector and not self._is_valid_css_selector(selector):
                    issues.append(f"Invalid CSS selector for {selector_type}: {selector}")
        
        return issues
    
    def _is_valid_css_selector(self, selector: str) -> bool:
        """Basic CSS selector validation."""
        if not selector or not selector.strip():
            return False
        
        # Basic checks for common CSS selector patterns
        # This is a simplified validation - a full CSS parser would be better
        invalid_chars = ['<', '>', '"', "'", '`']
        return not any(char in selector for char in invalid_chars)


# Predefined configurations are now loaded from framework JSON files
# See: onyx/connectors/web/selectors/frameworks/