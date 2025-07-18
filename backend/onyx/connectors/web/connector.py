"""
Web Connector implementation using Crawlee library.

This module provides the async-native web connector implementation.
"""

from onyx.configs.app_configs import INDEX_BATCH_SIZE


# Use the native async implementation directly
from onyx.connectors.web.async_connector import AsyncWebConnectorCrawlee

# Alias for backward compatibility
WebConnectorCrawlee = AsyncWebConnectorCrawlee


if __name__ == "__main__":
    # Import the original main section for CLI usage
    # This maintains backward compatibility for command-line usage
    
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Adaptive Web Connector - Extract content from websites with automatic JavaScript detection and specialized framework support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single page extraction
  python connector.py --url https://example.com --type single
  
  # Recursive crawling (automatically detects JS requirements)
  python connector.py --url https://docs.example.com --type recursive
  
  # Sitemap-based crawling
  python connector.py --url https://example.com/sitemap.xml --type sitemap
  
  # Save batches for ingestion API
  python connector.py --url https://docs.example.com --type recursive --save-batches --batch-dir ./ingestion_batches
        """
    )
    
    parser.add_argument(
        "--url", "-u",
        required=True,
        help="URL to crawl (base URL for recursive, sitemap URL for sitemap, specific page for single)"
    )
    
    parser.add_argument(
        "--type", "-t",
        choices=["single", "recursive", "sitemap"],
        default="single",
        help="Crawling type (default: single)"
    )
    
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=INDEX_BATCH_SIZE,
        help=f"Number of documents per batch (default: {INDEX_BATCH_SIZE})"
    )
    
    
    parser.add_argument(
        "--no-mintlify",
        action="store_true",
        help="Disable Mintlify-specific cleanup"
    )
    
    parser.add_argument(
        "--no-specialized",
        action="store_true",
        help="Disable specialized extractors (Zoomin, etc.)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file to save extracted content (JSON format)"
    )
    
    
    parser.add_argument(
        "--save-batches",
        action="store_true",
        help="Save each batch as a separate JSON file for ingestion API"
    )
    
    parser.add_argument(
        "--batch-dir",
        default="./batches",
        help="Directory to save batch files (default: ./batches)"
    )
    
    parser.add_argument(
        "--crawler-mode",
        choices=["adaptive", "static", "dynamic"],
        default="adaptive",
        help="Crawler mode: adaptive (auto-detect JS needs), static (BeautifulSoup only), dynamic (Playwright only). Default: adaptive"
    )
    
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip image files (PNG, JPG, GIF, etc.) during crawling"
    )
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        import logging
        from onyx.utils.logger import setup_logger
        logger = setup_logger()
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Setup batch saving if requested
    if args.save_batches:
        import os
        import json
        from datetime import datetime
        
        # Create batch directory
        os.makedirs(args.batch_dir, exist_ok=True)
        print(f"Batch files will be saved to: {args.batch_dir}")
    
    def save_batch_for_ingestion(batch_docs: list, batch_num: int, source_url: str) -> str:
        """Save a batch of documents in ingestion API format."""
        if not args.save_batches:
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"batch_{batch_num:03d}_{timestamp}.json"
        filepath = os.path.join(args.batch_dir, filename)
        
        # Convert documents to ingestion API format
        ingestion_docs = []
        for doc in batch_docs:
            # Extract first text section
            text_content = ""
            source_link = None
            if doc.sections:
                text_content = doc.sections[0].text
                source_link = doc.sections[0].link
            
            # Prepare metadata in the expected format
            metadata = doc.metadata.copy() if doc.metadata else {}
            metadata["document_id"] = doc.id
            metadata["source_url"] = source_url
            metadata["batch_number"] = batch_num
            metadata["extraction_timestamp"] = timestamp
            
            # Create ingestion document
            ingestion_doc = {
                "document": {
                    "id": doc.id,
                    "sections": [
                        {
                            "text": text_content,
                            "link": source_link or doc.id
                        }
                    ],
                    "source": "web",
                    "semantic_identifier": doc.semantic_identifier or doc.id,
                    "metadata": metadata,
                    "from_ingestion_api": True
                }
            }
            
            # Add optional fields if present
            if doc.doc_updated_at:
                ingestion_doc["document"]["doc_updated_at"] = doc.doc_updated_at.isoformat()
            
            ingestion_docs.append(ingestion_doc)
        
        # Save to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(ingestion_docs, f, indent=2, ensure_ascii=False)
            return filepath
        except Exception as e:
            print(f"Error saving batch {batch_num}: {e}")
            return None
    
    # Create connector with specified parameters
    try:
        # Always use async implementation
        import asyncio
        
        async def run_async_main():
            connector = WebConnectorCrawlee(
                base_url=args.url,
                web_connector_type=args.type,
                mintlify_cleanup=not args.no_mintlify,
                batch_size=args.batch_size,
                enable_specialized_extraction=not args.no_specialized,
                crawler_mode=args.crawler_mode,
                skip_images=args.skip_images,
            )
            
            print(f"Starting {args.type} crawl of: {args.url}")
            print(f"Crawler mode: {args.crawler_mode}")
            print("-" * 60)
            
            all_documents = []
            batch_count = 0
            
            async for batch in connector.load_from_state_async():
                batch_count += 1
                batch_size = len(batch)
                all_documents.extend(batch)
                
                print(f"Batch {batch_count}: {batch_size} documents")
                
                # Save batch for ingestion API if requested
                if args.save_batches:
                    saved_file = save_batch_for_ingestion(batch, batch_count, args.url)
                    if saved_file:
                        print(f"  Saved to: {saved_file}")
            
            print("-" * 60)
            print(f"Crawling complete! Total: {len(all_documents)} documents in {batch_count} batches")
            
            # Report failed URLs
            if connector.failed_urls:
                print(f"\nFailed URLs ({len(connector.failed_urls)}):")
                for failed_url, error_reason in connector.failed_urls.items():
                    print(f"  {error_reason}: {failed_url}")
            
            return all_documents
        
        all_documents = asyncio.run(run_async_main())
        
        # Save to file if requested
        if args.output:
            import json
            output_data = []
            for doc in all_documents:
                doc_data = {
                    "id": doc.id,
                    "title": doc.semantic_identifier,
                    "content": doc.sections[0].text if doc.sections else "",
                    "source": doc.source.value if hasattr(doc.source, 'value') else str(doc.source),
                    "metadata": doc.metadata or {},
                    "updated_at": doc.doc_updated_at.isoformat() if doc.doc_updated_at else None
                }
                output_data.append(doc_data)
            
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"Results saved to: {args.output}")
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)