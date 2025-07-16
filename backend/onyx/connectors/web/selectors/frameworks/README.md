# Framework Selector Definitions

This directory contains JSON configuration files for popular web frameworks and documentation systems. These files define how to extract content from specific types of websites using configurable selectors.

## Available Frameworks

### zoomin.json
Configuration for Zoomin documentation platform
- **Detection**: `#zDocsContent` element
- **Specialized for**: Zoomin documentation sites

### mkdocs.json
Configuration for MkDocs Material theme
- **Detection**: `.md-content` element
- **Specialized for**: MkDocs-generated documentation


## Framework File Format

Each framework file follows this simplified JSON structure:

```json
{
  "name": "framework_name",
  "url_patterns": ["/specific-path/", ".specific-domain"],
  "detection": [".framework-specific-element"],
  "title": [
    "h1",
    ".page-title",
    ".title"
  ],
  "body": [
    ".content",
    ".main-content",
    "article",
    "main"
  ],
  "metadata": {
    "date": ["meta[name='date']", ".date"],
    "author": "meta[name='author']"
  },
  "exclude": ["button", ".edit-link"]
}
```

### Field Descriptions:

- **name**: Unique identifier for the framework
- **detection**: Required selectors that must exist on the page
- **title**: Ordered list of title selectors (fallback order)
- **body**: Ordered list of body selectors (multiple selectors combined, nested content automatically deduplicated)
- **metadata**: Optional metadata selectors (can be string or array)
- **exclude**: (Optional) UI elements within content area to remove


## Adding New Frameworks

To add a new framework:

1. **Create JSON file**: Add a new `.json` file in this directory
2. **Define selectors**: Specify content, navigation, and metadata selectors
3. **Set detection rules**: Define required elements and URL patterns
4. **Configure exclusions**: List elements to remove from content
5. **Test**: Verify the framework works with target websites

### Example: Adding a new framework

```json
{
  "name": "my_framework",
  "detection": [".doc-container"],
  "title": [".doc-title", "h1"],
  "body": [".doc-content", ".content", "article"],
  "metadata": {
    "date": "meta[name='date']",
    "author": ".author"
  },
  "exclude": ["button", ".edit-link"]
}
```

### How the Simplified Format Works:

1. **Title fallback**: Selectors are tried in order - first `.doc-title`, then `h1`
2. **Body combination**: All matching selectors are combined - `.doc-content` + `.content` + `article` (nested content automatically deduplicated)
3. **Metadata arrays**: For metadata like `"date": ["meta[name='date']", ".date"]`, the first selector is primary, others are fallback
4. **Detection**: All selectors in the `detection` array must be present on the page
5. **Exclude field**: Optional - only needed for UI elements (buttons, links) that appear **within** the content area. Structural elements (nav, header, footer) are automatically excluded by positive selection.

## Automatic Features

- **JavaScript/CSS Removal**: `script`, `style`, and `noscript` tags are automatically removed
- **Validation**: Configurations are validated when loaded
- **Fallback Selectors**: Multiple fallback options for robust extraction
- **DOM-based Detection**: Framework selection based on page content analysis

## Usage

### Programmatic Usage

```python
from onyx.connectors.web.selectors.framework_loader import get_framework_loader

# Get the global framework loader
loader = get_framework_loader()

# List available frameworks
frameworks = loader.list_frameworks()

# Get specific framework
zoomin_config = loader.get_framework("zoomin")

# Find framework for page (DOM-based detection)
config = loader.get_framework_for_page(soup, "https://docs.example.com")
```

### Command Line Usage

Frameworks are automatically loaded and used by the web connector:

```bash
# Frameworks are automatically selected based on DOM detection
python -m onyx.connectors.web.connector --url https://docs.example.com

# You can still override with custom selectors
python -m onyx.connectors.web.connector --title-selector "h1" --content-selector ".content"
```

## Validation

Framework configurations are validated when loaded:
- CSS selector syntax checking
- Required field validation
- Detection pattern validation

View validation results:
```python
loader = get_framework_loader()
issues = loader.validate_all_frameworks()
```

## Troubleshooting

### Framework Not Loading
- Check JSON syntax
- Verify required fields are present
- Check file permissions
- Review logs for error messages

### Framework Not Matching
- Check required detection selectors exist on target pages
- Test selectors manually on target pages

### Poor Content Extraction
- Review exclusion selectors
- Check fallback selector coverage
- Verify content selectors target correct elements
- Test with different page layouts

## Best Practices

1. **Specific Detection**: Use specific required selectors for accurate framework detection
2. **Comprehensive Fallbacks**: Provide multiple fallback options for robustness
3. **Thorough Exclusions**: Remove all navigation, UI, and advertising elements
4. **Testing**: Test with real websites before deployment