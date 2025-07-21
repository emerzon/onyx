"""
OpenTelemetry instrumentation for Crawlee crawlers.

This module provides tracing and monitoring capabilities for web crawlers
using OpenTelemetry as recommended by Crawlee documentation.
"""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

try:
    from crawlee.otel import CrawlerInstrumentor
    from crawlee.storages import RequestQueue, KeyValueStore, Dataset
    CRAWLEE_OTEL_AVAILABLE = True
except ImportError:
    CRAWLEE_OTEL_AVAILABLE = False

from onyx.utils.logger import setup_logger

logger = setup_logger()


def setup_crawler_instrumentation(
    service_name: str = "OnyxWebCrawler",
    service_version: str = "1.0.0",
    environment: str = "production",
    otlp_endpoint: str = None,
    enable_instrumentation: bool = True
) -> None:
    """
    Set up OpenTelemetry instrumentation for Crawlee crawlers.
    
    Args:
        service_name: Name of the service for tracing
        service_version: Version of the service
        environment: Environment (development, staging, production)
        otlp_endpoint: OTLP exporter endpoint (e.g., 'localhost:4317')
        enable_instrumentation: Whether to enable instrumentation
    """
    if not enable_instrumentation:
        logger.info("Crawler instrumentation is disabled")
        return
        
    if not CRAWLEE_OTEL_AVAILABLE:
        logger.warning(
            "Crawlee OpenTelemetry support not available. "
            "Install with: pip install 'crawlee[otel]' or 'crawlee[all]'"
        )
        return
    
    if not otlp_endpoint:
        logger.info(
            "No OTLP endpoint specified. Instrumentation will be set up but "
            "traces won't be exported. Set OTEL_EXPORTER_OTLP_ENDPOINT to enable."
        )
        return
    
    try:
        # Create resource with service information
        resource = Resource.create({
            'service.name': service_name,
            'service.version': service_version,
            'environment': environment,
        })
        
        # Create and configure tracer provider
        provider = TracerProvider(resource=resource)
        
        # Create OTLP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=otlp_endpoint,
            insecure=True  # Use insecure for local development
        )
        
        # Add span processor with exporter
        provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
        
        # Set the global tracer provider
        trace.set_tracer_provider(provider)
        
        # Instrument Crawlee components
        CrawlerInstrumentor(
            instrument_classes=[RequestQueue, KeyValueStore, Dataset]
        ).instrument()
        
        logger.info(
            f"Crawler instrumentation enabled. Exporting traces to {otlp_endpoint}. "
            f"Service: {service_name} v{service_version} ({environment})"
        )
        
    except Exception as e:
        logger.error(f"Failed to set up crawler instrumentation: {e}")


def get_tracer(name: str = "onyx.web_connector") -> trace.Tracer:
    """
    Get a tracer instance for manual instrumentation.
    
    Args:
        name: Name of the tracer (typically module name)
        
    Returns:
        OpenTelemetry tracer instance
    """
    return trace.get_tracer(name)


# Environment variable configuration
import os

# Check if instrumentation should be enabled
ENABLE_CRAWLER_INSTRUMENTATION = os.getenv("ENABLE_CRAWLER_INSTRUMENTATION", "false").lower() == "true"
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "OnyxWebCrawler")
OTEL_SERVICE_VERSION = os.getenv("OTEL_SERVICE_VERSION", "1.0.0")
OTEL_ENVIRONMENT = os.getenv("OTEL_ENVIRONMENT", "production")

# Auto-setup if enabled via environment
if ENABLE_CRAWLER_INSTRUMENTATION:
    setup_crawler_instrumentation(
        service_name=OTEL_SERVICE_NAME,
        service_version=OTEL_SERVICE_VERSION,
        environment=OTEL_ENVIRONMENT,
        otlp_endpoint=OTEL_EXPORTER_OTLP_ENDPOINT,
        enable_instrumentation=True
    )