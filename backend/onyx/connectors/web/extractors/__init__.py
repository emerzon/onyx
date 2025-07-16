"""
Web content extractors for different documentation frameworks.
"""

from .base import BaseExtractor
from .factory import ExtractorFactory

__all__ = ['BaseExtractor', 'ExtractorFactory']