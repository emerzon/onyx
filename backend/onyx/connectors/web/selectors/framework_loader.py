"""
Framework loader for dynamically loading selector configurations from JSON files.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

from .config import SelectorConfig
from onyx.utils.logger import setup_logger

logger = setup_logger()


class FrameworkLoader:
    """Loads and manages framework-specific selector configurations."""
    
    def __init__(self, frameworks_dir: Optional[Path] = None):
        """Initialize the framework loader.
        
        Args:
            frameworks_dir: Directory containing framework JSON files.
                           Defaults to frameworks/ subdirectory.
        """
        if frameworks_dir is None:
            frameworks_dir = Path(__file__).parent / "frameworks"
        
        self.frameworks_dir = Path(frameworks_dir)
        self._loaded_frameworks: Dict[str, SelectorConfig] = {}
        self._load_all_frameworks()
    
    def _load_all_frameworks(self) -> None:
        """Load all framework configurations from the frameworks directory."""
        if not self.frameworks_dir.exists():
            logger.warning(f"Frameworks directory not found: {self.frameworks_dir}")
            return
        
        json_files = list(self.frameworks_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} framework files in {self.frameworks_dir}")
        
        for json_file in json_files:
            try:
                self._load_framework_file(json_file)
            except Exception as e:
                logger.error(f"Failed to load framework from {json_file}: {e}")
    
    def _load_framework_file(self, file_path: Path) -> None:
        """Load a single framework configuration file.
        
        Args:
            file_path: Path to the JSON framework file
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                framework_data = json.load(f)
            
            # Validate required fields
            if 'name' not in framework_data:
                logger.error(f"Framework file {file_path} missing required 'name' field")
                return
            
            config = SelectorConfig.from_dict(framework_data)
            framework_name = config.name
            
            self._loaded_frameworks[framework_name] = config
            logger.info(f"Loaded framework: {framework_name} from {file_path.name}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in framework file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading framework from {file_path}: {e}")
    
    def get_framework(self, name: str) -> Optional[SelectorConfig]:
        """Get a framework configuration by name.
        
        Args:
            name: Name of the framework
            
        Returns:
            SelectorConfig if found, None otherwise
        """
        return self._loaded_frameworks.get(name)
    
    def get_all_frameworks(self) -> Dict[str, SelectorConfig]:
        """Get all loaded framework configurations.
        
        Returns:
            Dictionary mapping framework names to configurations
        """
        return self._loaded_frameworks.copy()
    
    def list_frameworks(self) -> List[str]:
        """Get list of available framework names.
        
        Returns:
            List of framework names
        """
        return list(self._loaded_frameworks.keys())
    
    def get_frameworks_by_priority(self) -> List[SelectorConfig]:
        """Get frameworks sorted by name (for consistent ordering).
        
        Returns:
            List of framework configurations sorted by name
        """
        frameworks = list(self._loaded_frameworks.values())
        return sorted(frameworks, key=lambda x: x.name)
    
    def reload_frameworks(self) -> None:
        """Reload all framework configurations from disk."""
        self._loaded_frameworks.clear()
        self._load_all_frameworks()
    
    def add_framework_directory(self, directory: Path) -> None:
        """Add an additional directory to search for framework files.
        
        Args:
            directory: Path to directory containing framework JSON files
        """
        if not directory.exists():
            logger.warning(f"Framework directory not found: {directory}")
            return
        
        json_files = list(directory.glob("*.json"))
        logger.info(f"Loading {len(json_files)} additional framework files from {directory}")
        
        for json_file in json_files:
            try:
                self._load_framework_file(json_file)
            except Exception as e:
                logger.error(f"Failed to load additional framework from {json_file}: {e}")
    
    def get_framework_for_url(self, url: str) -> Optional[SelectorConfig]:
        """Get the most suitable framework for a given URL.
        
        Note: This method is deprecated since URL patterns have been removed.
        Use get_framework_for_page() with DOM-based detection instead.
        
        Args:
            url: URL (no longer used for matching)
            
        Returns:
            None - URL-based matching is no longer supported
        """
        logger.debug(f"URL-based framework matching is deprecated for URL: {url}")
        return None
    
    def validate_all_frameworks(self) -> Dict[str, List[str]]:
        """Validate all loaded frameworks.
        
        Returns:
            Dictionary mapping framework names to lists of validation issues
        """
        validation_results = {}
        
        for name, config in self._loaded_frameworks.items():
            issues = config.validate()
            if issues:
                validation_results[name] = issues
        
        return validation_results
    
    def get_framework_info(self) -> Dict[str, Dict[str, any]]:
        """Get summary information about all loaded frameworks.
        
        Returns:
            Dictionary with framework information
        """
        info = {}
        
        for name, config in self._loaded_frameworks.items():
            info[name] = {
                'name': config.name,
                'enabled': config.enabled,
                'description': getattr(config, 'description', 'No description'),
                'required_selectors': config.detection.required,
                'validation_issues': config.validate()
            }
        
        return info


# Global instance for easy access
_framework_loader = None

def get_framework_loader() -> FrameworkLoader:
    """Get the global framework loader instance."""
    global _framework_loader
    if _framework_loader is None:
        _framework_loader = FrameworkLoader()
    return _framework_loader

def reload_frameworks() -> None:
    """Reload all framework configurations."""
    global _framework_loader
    if _framework_loader is not None:
        _framework_loader.reload_frameworks()
    else:
        _framework_loader = FrameworkLoader()