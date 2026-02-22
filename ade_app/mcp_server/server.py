"""
ADE MCP Server - Simplified Version (JSON Backend)

Exposes ADE metadata catalog to AI agents via Model Context Protocol.
This version uses JSON files as backend for easy setup and portability.

Usage:
    python -m ade_app.mcp_server.server

Or configure in Claude Code's MCP settings.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Optional

# Setup logging
LOG_FILE = Path(__file__).parent.parent.parent / 'ade_mcp.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger('ade_mcp')

from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("ade")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Repository root (ade-core/)
REPO_ROOT = Path(__file__).parent.parent.parent
ADE_DATA_ROOT = REPO_ROOT / "ade_data"

# Current environment (can be changed at runtime)
_current_environment: str = "demo"


def _get_environment_path(env_name: str = None) -> Path:
    """Get path to environment data folder."""
    env = env_name or _current_environment
    return ADE_DATA_ROOT / env


def _load_json_file(filepath: Path) -> list | dict:
    """Load JSON file, return empty list/dict if not found."""
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


def _get_extractions_path(platform: str, env_name: str = None) -> Path:
    """Get path to platform extractions folder."""
    return _get_environment_path(env_name) / "extractions" / platform


# =============================================================================
# ENVIRONMENT MANAGEMENT
# =============================================================================

def _get_available_environments() -> list[dict]:
    """Scan ade_data directory for available environments."""
    environments = []

    if not ADE_DATA_ROOT.exists():
        return environments

    for env_dir in sorted(ADE_DATA_ROOT.iterdir()):
        if env_dir.is_dir() and not env_dir.name.startswith('_'):
            config_file = env_dir / "config.yaml"

            # Check for config or extractions
            extractions_dir = env_dir / "extractions"
            has_data = config_file.exists() or extractions_dir.exists()

            if has_data:
                # Try to load config for details
                env_info = {
                    "id": env_dir.name,
                    "name": env_dir.name.upper(),
                    "path": str(env_dir),
                    "has_config": config_file.exists(),
                    "has_extractions": extractions_dir.exists(),
                }

                if config_file.exists():
                    try:
                        import yaml
                        with open(config_file, 'r', encoding='utf-8') as f:
                            config = yaml.safe_load(f)
                        env_info["name"] = config.get('environment', {}).get('name', env_dir.name.upper())
                        env_info["description"] = config.get('environment', {}).get('description', '')
                    except Exception:
                        pass

                environments.append(env_info)

    return environments


@mcp.tool()
async def list_environments() -> dict:
    """List all available ADE environments.

    Scans the ade_data directory for configured environments.
    Each environment contains extracted metadata from various platforms.

    Returns:
        List of environments with their details
    """
    environments = _get_available_environments()
    return {
        "current_environment": _current_environment,
        "count": len(environments),
        "environments": environments
    }


@mcp.tool()
async def set_environment(environment_id: str) -> dict:
    """Switch to a different ADE environment.

    Changes the active environment for all subsequent catalog queries.

    Args:
        environment_id: The environment ID to switch to (e.g., 'demo')

    Returns:
        Confirmation of environment switch
    """
    global _current_environment

    env_path = ADE_DATA_ROOT / environment_id
    if not env_path.exists():
        available = [e["id"] for e in _get_available_environments()]
        return {
            "success": False,
            "error": f"Environment '{environment_id}' not found",
            "available_environments": available
        }

    old_environment = _current_environment
    _current_environment = environment_id

    logger.info(f"Environment switched: {old_environment} -> {environment_id}")

    return {
        "success": True,
        "previous_environment": old_environment,
        "current_environment": environment_id,
        "message": f"Switched to environment '{environment_id}'"
    }


@mcp.tool()
async def get_environment_info() -> dict:
    """Get information about the current ADE environment.

    Returns the active environment and available platforms/data.
    """
    env_path = _get_environment_path()
    extractions_path = env_path / "extractions"

    # Check which platforms have data
    platforms = []
    if extractions_path.exists():
        for platform_dir in extractions_path.iterdir():
            if platform_dir.is_dir():
                json_files = list(platform_dir.glob("*.json"))
                if json_files:
                    platforms.append({
                        "name": platform_dir.name,
                        "files": [f.stem for f in json_files]
                    })

    return {
        "environment_id": _current_environment,
        "path": str(env_path),
        "platforms_with_data": platforms,
        "available_environments": [e["id"] for e in _get_available_environments()]
    }


# =============================================================================
# CATALOG SEARCH
# =============================================================================

def _search_in_list(items: list, query: str, name_field: str = "name") -> list:
    """Search for items matching query in a list of dicts."""
    if not query or query in ('*', '%', ''):
        return items

    query_lower = query.lower()
    return [
        item for item in items
        if query_lower in str(item.get(name_field, '')).lower()
        or query_lower in str(item.get('path', '')).lower()
        or query_lower in str(item.get('description', '')).lower()
    ]


def _load_platform_data(platform: str, object_type: str = None) -> list:
    """Load all data for a platform, optionally filtered by type."""
    extractions_path = _get_extractions_path(platform)
    all_data = []

    if not extractions_path.exists():
        return all_data

    for json_file in extractions_path.glob("*.json"):
        file_type = json_file.stem  # e.g., "notebooks", "jobs"

        # Filter by object_type if specified
        if object_type:
            # Match singular/plural: "notebook" matches "notebooks.json"
            if not (object_type.lower() in file_type.lower() or
                    file_type.lower().startswith(object_type.lower())):
                continue

        data = _load_json_file(json_file)
        if isinstance(data, list):
            for item in data:
                item['_platform'] = platform
                item['_type'] = file_type.rstrip('s')  # notebooks -> notebook
                item['_source_file'] = json_file.name
            all_data.extend(data)
        elif isinstance(data, dict):
            # Handle dict format (e.g., {id: object})
            for key, item in data.items():
                if isinstance(item, dict):
                    item['_platform'] = platform
                    item['_type'] = file_type.rstrip('s')
                    item['_source_file'] = json_file.name
                    all_data.append(item)

    return all_data


@mcp.tool()
async def search_catalog(
    query: str = "",
    platform: str = None,
    object_type: str = None,
    limit: int = 20
) -> dict:
    """Search the ADE metadata catalog for objects.

    Args:
        query: Search term (searches in object names). Use '*' or empty for all.
        platform: Filter by platform (databricks, postgresql, powerbi, etc.)
        object_type: Filter by type (notebook, job, table, measure, etc.)
        limit: Maximum results (default 20, max 100)

    Returns:
        List of matching objects with basic metadata
    """
    limit = min(limit, 100)
    all_results = []

    extractions_path = _get_environment_path() / "extractions"

    if not extractions_path.exists():
        return {
            "query": query,
            "error": f"No extractions found in {extractions_path}",
            "results": []
        }

    # Determine which platforms to search
    if platform:
        platforms_to_search = [platform]
    else:
        platforms_to_search = [
            d.name for d in extractions_path.iterdir()
            if d.is_dir()
        ]

    # Search each platform
    for plat in platforms_to_search:
        data = _load_platform_data(plat, object_type)
        matches = _search_in_list(data, query)
        all_results.extend(matches)

        if len(all_results) >= limit:
            break

    # Trim and format results
    results = all_results[:limit]

    # Simplify output for readability
    simplified = []
    for item in results:
        simplified.append({
            "name": item.get("name") or item.get("path", "").split("/")[-1],
            "platform": item.get("_platform"),
            "type": item.get("_type"),
            "path": item.get("path", ""),
            "description": item.get("description", "")[:100] if item.get("description") else "",
        })

    return {
        "query": query,
        "filters": {"platform": platform, "object_type": object_type},
        "count": len(simplified),
        "results": simplified
    }


@mcp.tool()
async def get_object_details(
    name: str,
    platform: str,
    object_type: str = None
) -> dict:
    """Get detailed information about a specific object.

    Args:
        name: Name or path of the object
        platform: The platform (databricks, postgresql, powerbi, etc.)
        object_type: Optional type filter (notebook, job, table, etc.)

    Returns:
        Full object details including all metadata
    """
    data = _load_platform_data(platform, object_type)

    # Search for exact or partial match
    name_lower = name.lower()

    # Try exact match first
    for item in data:
        item_name = item.get("name", "") or item.get("path", "").split("/")[-1]
        if item_name.lower() == name_lower:
            return {"found": True, "object": item}

    # Try partial match
    for item in data:
        item_name = item.get("name", "") or item.get("path", "").split("/")[-1]
        item_path = item.get("path", "")
        if name_lower in item_name.lower() or name_lower in item_path.lower():
            return {"found": True, "object": item}

    return {
        "found": False,
        "error": f"Object '{name}' not found in {platform}",
        "suggestion": "Use search_catalog() to find available objects"
    }


@mcp.tool()
async def get_platform_stats(platform: str = None) -> dict:
    """Get statistics about objects in the metadata catalog.

    Args:
        platform: Optional - filter stats for a specific platform

    Returns:
        Object counts by platform and type
    """
    extractions_path = _get_environment_path() / "extractions"
    stats = {}

    if not extractions_path.exists():
        return {"error": "No extractions found", "stats": {}}

    # Determine which platforms to count
    if platform:
        platforms_to_count = [platform]
    else:
        platforms_to_count = [
            d.name for d in extractions_path.iterdir()
            if d.is_dir()
        ]

    for plat in platforms_to_count:
        plat_path = extractions_path / plat
        if not plat_path.exists():
            continue

        plat_stats = {}
        for json_file in plat_path.glob("*.json"):
            data = _load_json_file(json_file)
            if isinstance(data, list):
                plat_stats[json_file.stem] = len(data)
            elif isinstance(data, dict):
                plat_stats[json_file.stem] = len(data)

        if plat_stats:
            stats[plat] = plat_stats

    return {
        "environment": _current_environment,
        "platform_filter": platform,
        "stats": stats
    }


# =============================================================================
# LINEAGE (Simplified - based on notebook parsing)
# =============================================================================

def _extract_table_references(source_code: str) -> list[str]:
    """Extract table references from notebook source code."""
    import re

    tables = set()

    # Common patterns for table references
    patterns = [
        r'spark\.table\(["\']([^"\']+)["\']\)',  # spark.table("schema.table")
        r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # FROM schema.table
        r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # JOIN schema.table
        r'INTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # INTO schema.table
        r'\.saveAsTable\(["\']([^"\']+)["\']\)',  # .saveAsTable("table")
        r'\.insertInto\(["\']([^"\']+)["\']\)',  # .insertInto("table")
    ]

    for pattern in patterns:
        matches = re.findall(pattern, source_code, re.IGNORECASE)
        tables.update(matches)

    return sorted(list(tables))


@mcp.tool()
async def get_notebook_lineage(notebook_name: str) -> dict:
    """Analyze a Databricks notebook to find table dependencies.

    Parses the notebook source code to identify:
    - Tables read (upstream dependencies)
    - Tables written (downstream outputs)

    Args:
        notebook_name: Name or path of the notebook

    Returns:
        Lineage information with upstream and downstream tables
    """
    # Find the notebook
    result = await get_object_details(notebook_name, "databricks", "notebook")

    if not result.get("found"):
        return {
            "notebook": notebook_name,
            "error": "Notebook not found",
            "upstream": [],
            "downstream": []
        }

    notebook = result["object"]
    source_code = notebook.get("source_code", "") or notebook.get("content", "")

    if not source_code:
        return {
            "notebook": notebook_name,
            "warning": "No source code available for analysis",
            "upstream": [],
            "downstream": []
        }

    # Extract table references
    all_tables = _extract_table_references(source_code)

    # Heuristic: tables in FROM/JOIN are upstream, tables in INTO/saveAsTable are downstream
    import re

    upstream = set()
    downstream = set()

    # Upstream patterns (reading)
    for pattern in [r'FROM\s+(\S+)', r'JOIN\s+(\S+)', r'spark\.table\(["\']([^"\']+)["\']\)']:
        for match in re.findall(pattern, source_code, re.IGNORECASE):
            if '.' in match or match.isidentifier():
                upstream.add(match.strip('`"\''))

    # Downstream patterns (writing)
    for pattern in [r'INTO\s+(\S+)', r'\.saveAsTable\(["\']([^"\']+)["\']\)', r'\.insertInto\(["\']([^"\']+)["\']\)']:
        for match in re.findall(pattern, source_code, re.IGNORECASE):
            if '.' in match or match.isidentifier():
                downstream.add(match.strip('`"\''))

    return {
        "notebook": notebook.get("name") or notebook.get("path"),
        "path": notebook.get("path"),
        "upstream": sorted(list(upstream)),
        "downstream": sorted(list(downstream)),
        "all_table_references": all_tables
    }


# =============================================================================
# RESOURCES - Documentation
# =============================================================================

@mcp.resource("ade://guide")
def get_ade_guide() -> str:
    """Complete guide to ADE - what it is and how to use it."""
    return """# ADE Core - Analytics Data Environment

## What is ADE?

ADE is an open-source framework for **agentic data engineering**. It provides:

- **Metadata extraction** from data platforms (Databricks, Power BI, etc.)
- **Unified catalog** searchable via MCP tools
- **AI agent integration** via Model Context Protocol

## Quick Start

1. Check environment: `get_environment_info()`
2. Search catalog: `search_catalog("sales")`
3. Get details: `get_object_details("notebook_name", "databricks")`
4. Analyze lineage: `get_notebook_lineage("etl_notebook")`

## Available Tools

| Tool | Description |
|------|-------------|
| `list_environments()` | See available environments |
| `set_environment(id)` | Switch environment |
| `get_environment_info()` | Current environment details |
| `search_catalog(query)` | Find objects by name |
| `get_object_details(name, platform)` | Full object metadata |
| `get_platform_stats()` | Object counts |
| `get_notebook_lineage(name)` | Notebook dependencies |

## Supported Platforms

- **Databricks**: notebooks, jobs
- **Power BI**: datasets, measures (coming soon)
- **PostgreSQL**: tables, views (coming soon)

## Example Workflow

```
1. get_environment_info()           # Check what's available
2. search_catalog("etl")            # Find ETL notebooks
3. get_object_details("etl_sales", "databricks")  # See code
4. get_notebook_lineage("etl_sales")  # Find dependencies
```
"""


@mcp.tool()
async def get_ade_overview() -> dict:
    """Get a description of what ADE is and its capabilities.

    Returns:
        Overview of ADE including description, tools, and quick start
    """
    return {
        "name": "ADE Core - Analytics Data Environment",
        "version": "0.1.0 (MVP)",
        "description": (
            "Open-source framework for agentic data engineering. "
            "Extracts metadata from data platforms and exposes it to AI agents via MCP."
        ),
        "backend": "JSON files (simplified version)",
        "supported_platforms": [
            "databricks (notebooks, jobs)",
            "powerbi (coming soon)",
            "postgresql (coming soon)"
        ],
        "available_tools": [
            "list_environments()",
            "set_environment(id)",
            "get_environment_info()",
            "search_catalog(query, platform?, type?)",
            "get_object_details(name, platform)",
            "get_platform_stats(platform?)",
            "get_notebook_lineage(name)"
        ],
        "quick_start": [
            "1. get_environment_info() - See current environment",
            "2. search_catalog('*') - Browse all objects",
            "3. get_object_details('name', 'databricks') - Get full details"
        ],
        "current_environment": _current_environment,
        "repository": "https://github.com/robertobutinar/ade-core"
    }


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Run the MCP server."""
    logger.info(f"Starting ADE MCP Server (JSON backend)")
    logger.info(f"Data root: {ADE_DATA_ROOT}")
    logger.info(f"Default environment: {_current_environment}")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
