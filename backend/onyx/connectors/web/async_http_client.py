"""
Async HTTP client wrapper for web connector operations.
Replaces blocking requests operations with async httpx operations.
"""

import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from onyx.utils.logger import setup_logger

logger = setup_logger()


class AsyncHttpClient:
    """
    Enhanced async HTTP client wrapper around httpx with connection pooling, 
    retry logic, and proper resource management.
    """
    
    def __init__(
        self,
        timeout: float = 30.0,
        follow_redirects: bool = True,
        oauth_token: Optional[str] = None,
        max_connections: int = 20,
        max_keepalive_connections: int = 10,
        keepalive_expiry: float = 30.0,
        max_retries: int = 3,
        retry_backoff_factor: float = 0.5,
    ):
        self.timeout = timeout
        self.follow_redirects = follow_redirects
        self.oauth_token = oauth_token
        self.max_retries = max_retries
        self.retry_backoff_factor = retry_backoff_factor
        
        # Default headers
        self.headers = {
            "User-Agent": "Onyx-WebConnector/2.0"
        }
        
        if oauth_token:
            self.headers["Authorization"] = f"Bearer {oauth_token}"
        
        # Connection pool limits
        self.limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
        )
        
        # Client instance (will be created when needed)
        self._client: Optional[httpx.AsyncClient] = None
        self._client_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_client(self):
        """Ensure HTTP client is initialized."""
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        timeout=self.timeout,
                        follow_redirects=self.follow_redirects,
                        limits=self.limits,
                        headers=self.headers,
                    )
    
    async def close(self):
        """Close the HTTP client and cleanup resources."""
        if self._client is not None:
            async with self._client_lock:
                if self._client is not None:
                    await self._client.aclose()
                    self._client = None
    
    async def _make_request_with_retry(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic and exponential backoff.
        
        Args:
            method: HTTP method (GET, HEAD, etc.)
            url: URL to request
            headers: Additional headers
            timeout: Request timeout (overrides default)
            **kwargs: Additional arguments for httpx request
            
        Returns:
            httpx.Response object
            
        Raises:
            httpx.HTTPError: For HTTP errors after all retries
            asyncio.TimeoutError: For timeout errors after all retries
        """
        await self._ensure_client()
        
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        request_timeout = timeout or self.timeout
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    timeout=request_timeout,
                    **kwargs
                )
                
                # Log retry success if this was a retry
                if attempt > 0:
                    logger.info(f"Request succeeded on attempt {attempt + 1} for {url}")
                
                return response
                
            except httpx.TimeoutException as e:
                last_exception = e
                if attempt < self.max_retries:
                    backoff_time = self.retry_backoff_factor * (2 ** attempt)
                    logger.warning(f"Timeout on attempt {attempt + 1} for {url}, retrying in {backoff_time:.1f}s")
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"Request timed out after {self.max_retries + 1} attempts for {url}")
                    raise asyncio.TimeoutError(f"Request to {url} timed out after {self.max_retries + 1} attempts")
            
            except (httpx.ConnectError, httpx.NetworkError) as e:
                last_exception = e
                if attempt < self.max_retries:
                    backoff_time = self.retry_backoff_factor * (2 ** attempt)
                    logger.warning(f"Network error on attempt {attempt + 1} for {url}: {e}, retrying in {backoff_time:.1f}s")
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"Network error after {self.max_retries + 1} attempts for {url}: {e}")
                    raise
            
            except httpx.HTTPStatusError as e:
                # Don't retry on client errors (4xx), only server errors (5xx)
                if e.response.status_code >= 500 and attempt < self.max_retries:
                    backoff_time = self.retry_backoff_factor * (2 ** attempt)
                    logger.warning(f"Server error {e.response.status_code} on attempt {attempt + 1} for {url}, retrying in {backoff_time:.1f}s")
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"HTTP error {e.response.status_code} for {url}: {e.response.text}")
                    raise
            
            except Exception as e:
                last_exception = e
                logger.error(f"Unexpected error on attempt {attempt + 1} for {url}: {e}")
                if attempt >= self.max_retries:
                    raise
                
                backoff_time = self.retry_backoff_factor * (2 ** attempt)
                await asyncio.sleep(backoff_time)
        
        # This should never be reached, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError(f"Failed to make request to {url} after {self.max_retries + 1} attempts")
    
    async def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """
        Async GET request with retry logic and proper error handling.
        
        Args:
            url: URL to fetch
            headers: Additional headers to include
            timeout: Request timeout (overrides default)
            
        Returns:
            httpx.Response object
            
        Raises:
            httpx.HTTPError: For HTTP errors after all retries
            asyncio.TimeoutError: For timeout errors after all retries
        """
        response = await self._make_request_with_retry("GET", url, headers, timeout)
        response.raise_for_status()
        return response
    
    async def head(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """
        Async HEAD request with retry logic and proper error handling.
        
        Args:
            url: URL to check
            headers: Additional headers to include  
            timeout: Request timeout (overrides default)
            
        Returns:
            httpx.Response object (status code not checked for HEAD requests)
        """
        return await self._make_request_with_retry("HEAD", url, headers, timeout)
    
    async def get_content(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """
        Get raw content bytes from URL with retry logic.
        
        Args:
            url: URL to fetch
            headers: Additional headers to include
            timeout: Request timeout (overrides default)
            
        Returns:
            Raw content bytes
        """
        response = await self.get(url, headers=headers, timeout=timeout)
        return response.content
    
    async def get_text(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> str:
        """
        Get text content from URL with retry logic.
        
        Args:
            url: URL to fetch
            headers: Additional headers to include
            timeout: Request timeout (overrides default)
            
        Returns:
            Text content
        """
        response = await self.get(url, headers=headers, timeout=timeout)
        return response.text
    
    async def get_soup(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        parser: str = "html.parser",
    ) -> BeautifulSoup:
        """
        Get BeautifulSoup object from URL with retry logic.
        
        Args:
            url: URL to fetch
            headers: Additional headers to include
            timeout: Request timeout (overrides default)
            parser: BeautifulSoup parser to use
            
        Returns:
            BeautifulSoup object
        """
        text_content = await self.get_text(url, headers=headers, timeout=timeout)
        return BeautifulSoup(text_content, parser)
    
    async def stream_content(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        chunk_size: int = 8192,
    ):
        """
        Stream content from URL for large files.
        
        Args:
            url: URL to stream
            headers: Additional headers to include
            timeout: Request timeout (overrides default)
            chunk_size: Size of chunks to yield
            
        Yields:
            bytes: Content chunks
        """
        await self._ensure_client()
        
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        request_timeout = timeout or self.timeout
        
        async with self._client.stream(
            "GET", 
            url, 
            headers=request_headers, 
            timeout=request_timeout
        ) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size):
                yield chunk
    
    def get_last_modified_datetime(self, response: httpx.Response) -> Optional[datetime]:
        """
        Extract last modified datetime from response headers.
        
        Args:
            response: httpx.Response object
            
        Returns:
            datetime object or None if not available
        """
        last_modified = response.headers.get("Last-Modified")
        if not last_modified:
            return None
        
        try:
            return datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            logger.warning(f"Could not parse Last-Modified header: {last_modified}")
            return None


# Manual sitemap functions removed - now using Crawlee's native SitemapRequestLoader
# which provides better performance, memory management, and URL filtering capabilities