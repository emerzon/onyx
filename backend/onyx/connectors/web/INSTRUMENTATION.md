# Web Crawler Instrumentation Guide

This guide explains how to enable tracing and monitoring for the Onyx web crawler using OpenTelemetry, as recommended by Crawlee documentation.

## Prerequisites

1. Install Crawlee with OpenTelemetry support:
   ```bash
   pip install 'crawlee[otel]'
   # or
   pip install 'crawlee[all]'
   ```

2. Set up Jaeger for trace visualization (optional but recommended):
   ```bash
   docker run -d --name jaeger \
     -e COLLECTOR_OTLP_ENABLED=true \
     -p 16686:16686 \
     -p 4317:4317 \
     -p 4318:4318 \
     jaegertracing/all-in-one:latest
   ```

## Configuration

### Environment Variables

Configure instrumentation using these environment variables:

- `ENABLE_CRAWLER_INSTRUMENTATION`: Set to `true` to enable instrumentation (default: `false`)
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OTLP endpoint for exporting traces (e.g., `localhost:4317`)
- `OTEL_SERVICE_NAME`: Service name for traces (default: `OnyxWebCrawler`)
- `OTEL_SERVICE_VERSION`: Service version (default: `1.0.0`)
- `OTEL_ENVIRONMENT`: Environment name (default: `production`)

### Example Usage

1. **Enable instrumentation with local Jaeger:**
   ```bash
   export ENABLE_CRAWLER_INSTRUMENTATION=true
   export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317
   python connector.py --url https://example.com --type recursive
   ```

2. **Enable instrumentation without exporter (for testing):**
   ```bash
   export ENABLE_CRAWLER_INSTRUMENTATION=true
   python connector.py --url https://example.com --type single
   ```

3. **Production setup with custom endpoint:**
   ```bash
   export ENABLE_CRAWLER_INSTRUMENTATION=true
   export OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector.example.com:4317
   export OTEL_SERVICE_NAME=onyx-web-crawler
   export OTEL_ENVIRONMENT=production
   python connector.py --url https://docs.example.com --type sitemap
   ```

## Viewing Traces

1. If using local Jaeger, open http://localhost:16686 in your browser
2. Select "OnyxWebCrawler" (or your custom service name) from the service dropdown
3. Click "Find Traces" to view crawler execution traces

## What's Instrumented

The following Crawlee components are automatically instrumented:
- **RequestQueue**: Track request queue operations
- **KeyValueStore**: Monitor key-value storage operations
- **Dataset**: Observe dataset push/get operations

## Manual Instrumentation

For custom spans in your code:

```python
from onyx.connectors.web.instrumentation import get_tracer

tracer = get_tracer()

async def my_custom_operation():
    with tracer.start_as_current_span("custom_operation") as span:
        span.set_attribute("operation.type", "custom")
        # Your code here
```

## Performance Considerations

- Instrumentation adds minimal overhead but may impact performance in high-throughput scenarios
- Use sampling in production to reduce data volume
- Disable instrumentation when not needed: `ENABLE_CRAWLER_INSTRUMENTATION=false`

## Troubleshooting

1. **No traces appearing:**
   - Verify Jaeger/OTLP collector is running
   - Check endpoint configuration
   - Ensure `crawlee[otel]` is installed

2. **Import errors:**
   - Install required package: `pip install 'crawlee[otel]'`

3. **Connection errors:**
   - Verify OTLP endpoint is accessible
   - Check firewall rules
   - Try with `insecure=True` for local development