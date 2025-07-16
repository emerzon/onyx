"""
Selector resolver for managing precedence and fallback of selector configurations.
"""

from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup

from .config import SelectorConfig
from .framework_loader import get_framework_loader
from onyx.utils.logger import setup_logger

logger = setup_logger()


class SelectorResolver:
    """Resolves selector configurations with precedence and fallback handling."""
    
    def __init__(self):
        """Initialize resolver with framework and custom configurations."""
        self.custom_configs: List[SelectorConfig] = []
        self.framework_loader = get_framework_loader()
        
        logger.info(f"Loaded {len(self.framework_loader.list_frameworks())} framework configurations")
    
    def register_framework_config(self, config: SelectorConfig) -> None:
        """Register a framework selector configuration (deprecated - use framework files)."""
        logger.warning("register_framework_config is deprecated. Use framework JSON files instead.")
        # For backward compatibility, we could add to framework loader
        pass
    
    def register_custom_config(self, config: SelectorConfig) -> None:
        """Register a custom selector configuration with higher priority."""
        self.custom_configs.append(config)
        self.custom_configs.sort(key=lambda x: x.priority, reverse=True)
        logger.info(f"Registered custom selector config: {config.name}")
    
    def get_extractor(self, soup: BeautifulSoup, url: str) -> Optional['GenericWebExtractor']:
        """
        Get the appropriate generic extractor for a page.
        
        Priority order:
        1. Custom configurations (highest priority)
        2. Default configurations 
        3. None if no match
        """
        # Import here to avoid circular import
        from .generic_extractor import GenericWebExtractor
        
        # Try custom configurations first
        for config in self.custom_configs:
            if not config.enabled:
                continue
                
            try:
                extractor = GenericWebExtractor(config)
                if extractor.can_extract(soup, url):
                    logger.info(f"Selected custom generic extractor: {config.name} for {url}")
                    return extractor
            except Exception as e:
                logger.warning(f"Error checking custom config {config.name}: {e}")
                continue
        
        # Try framework configurations
        for config in self.framework_loader.get_frameworks_by_priority():
            if not config.enabled:
                continue
                
            try:
                extractor = GenericWebExtractor(config)
                if extractor.can_extract(soup, url):
                    logger.info(f"Selected framework extractor: {config.name} for {url}")
                    return extractor
            except Exception as e:
                logger.warning(f"Error checking framework config {config.name}: {e}")
                continue
        
        logger.debug(f"No generic extractor configuration matched for {url}")
        return None
    
    def list_configs(self) -> List[str]:
        """Get list of available configuration names."""
        framework_configs = [config.name for config in self.framework_loader.get_frameworks_by_priority() if config.enabled]
        custom_configs = [config.name for config in self.custom_configs if config.enabled]
        return custom_configs + framework_configs
    
    def get_config_by_name(self, name: str) -> Optional[SelectorConfig]:
        """Get a specific configuration by name."""
        # Check custom configs first
        for config in self.custom_configs:
            if config.name == name and config.enabled:
                return config
        
        # Then check framework configs
        framework_config = self.framework_loader.get_framework(name)
        if framework_config and framework_config.enabled:
            return framework_config
        
        return None
    
    def merge_configs(self, base_config: SelectorConfig, override_config: SelectorConfig) -> SelectorConfig:
        """
        Merge two configurations, with override_config taking precedence.
        
        This allows for partial configuration overrides while keeping defaults.
        """
        # Create a copy of base config
        merged_dict = base_config.to_dict()
        override_dict = override_config.to_dict()
        
        # Merge dictionaries recursively
        def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
            result = base.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge_dicts(result[key], value)
                else:
                    result[key] = value
            return result
        
        merged_dict = merge_dicts(merged_dict, override_dict)
        
        # Update metadata
        merged_dict['name'] = f"{base_config.name}_merged"
        merged_dict['priority'] = max(base_config.priority, override_config.priority)
        
        return SelectorConfig.from_dict(merged_dict)
    
    def create_quick_config(
        self, 
        name: str,
        title_selector: Optional[str] = None,
        content_selector: Optional[str] = None,
        links_selector: Optional[str] = None,
        exclude_selectors: Optional[List[str]] = None,
        detection_selectors: Optional[List[str]] = None,
        url_patterns: Optional[List[str]] = None
    ) -> SelectorConfig:
        """
        Create a quick configuration for common use cases.
        
        This is a convenience method for CLI usage.
        """
        config_dict = {
            'name': name,
            'priority': 100,  # High priority for custom configs
            'content': {},
            'navigation': {},
            'exclusion': {},
            'detection': {}
        }
        
        if title_selector:
            config_dict['content']['title'] = title_selector
        
        if content_selector:
            config_dict['content']['body'] = content_selector
        
        if links_selector:
            config_dict['navigation']['links'] = links_selector
        
        if exclude_selectors:
            config_dict['exclusion']['remove'] = exclude_selectors
        
        if detection_selectors:
            config_dict['detection']['required'] = detection_selectors
        
        if url_patterns:
            config_dict['detection']['url_patterns'] = url_patterns
        
        return SelectorConfig.from_dict(config_dict)
    
    def clear_custom_configs(self) -> None:
        """Clear all custom configurations."""
        self.custom_configs.clear()
        logger.info("Cleared all custom selector configurations")
    
    def clear_all_configs(self) -> None:
        """Clear all configurations (custom and framework)."""
        self.custom_configs.clear()
        self.framework_loader.reload_frameworks()
        logger.info("Cleared all selector configurations")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about registered configurations."""
        framework_configs = list(self.framework_loader.get_all_frameworks().values())
        enabled_framework_configs = [c for c in framework_configs if c.enabled]
        enabled_custom_configs = [c for c in self.custom_configs if c.enabled]
        
        return {
            'total_configs': len(self.custom_configs) + len(framework_configs),
            'custom_configs': len(self.custom_configs),
            'framework_configs': len(framework_configs),
            'enabled_configs': len(enabled_custom_configs) + len(enabled_framework_configs),
            'config_names': self.list_configs(),
            'framework_info': self.framework_loader.get_framework_info()
        }