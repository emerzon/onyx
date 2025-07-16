"""
Configurable selector system for generic web content extraction.
"""

from .config import SelectorConfig, SelectorType
from .generic_extractor import GenericWebExtractor
from .resolver import SelectorResolver
from .framework_loader import FrameworkLoader, get_framework_loader

__all__ = [
    'SelectorConfig',
    'SelectorType', 
    'GenericWebExtractor',
    'SelectorResolver',
    'FrameworkLoader',
    'get_framework_loader'
]