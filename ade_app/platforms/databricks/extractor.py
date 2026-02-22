"""
Databricks Metadata Extractor

Extracts metadata from Databricks workspaces via REST API.
Outputs JSON files for use with ADE MCP Server.

Usage:
    python -m ade_app.platforms.databricks.extractor --help

Example:
    python -m ade_app.platforms.databricks.extractor \\
        --host https://your-workspace.azuredatabricks.net \\
        --token dapi... \\
        --output ade_data/demo/extractions/databricks
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class NotebookMetadata:
    """Metadata for a Databricks notebook."""
    name: str
    path: str
    language: str
    object_type: str = "notebook"
    source_code: Optional[str] = None
    created_at: Optional[str] = None
    modified_at: Optional[str] = None


@dataclass
class JobMetadata:
    """Metadata for a Databricks job."""
    job_id: int
    name: str
    tasks: list
    schedule: Optional[dict] = None
    created_at: Optional[str] = None
    creator_user_name: Optional[str] = None


class DatabricksExtractor:
    """Extract metadata from Databricks workspace."""

    def __init__(self, host: str, token: str):
        """Initialize extractor.

        Args:
            host: Databricks workspace URL (e.g., https://xxx.azuredatabricks.net)
            token: Databricks personal access token
        """
        self.host = host.rstrip('/')
        self.token = token
        self._session = None

    @property
    def session(self):
        """Get requests session with auth headers."""
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.headers.update({
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            })
        return self._session

    def _api_get(self, endpoint: str, params: dict = None) -> dict:
        """Make GET request to Databricks API."""
        url = f"{self.host}/api/2.0/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _api_post(self, endpoint: str, data: dict = None) -> dict:
        """Make POST request to Databricks API."""
        url = f"{self.host}/api/2.0/{endpoint}"
        response = self.session.post(url, json=data or {})
        response.raise_for_status()
        return response.json()

    def list_notebooks(self, path: str = "/") -> list[dict]:
        """List all notebooks in workspace recursively.

        Args:
            path: Starting path (default: root)

        Returns:
            List of notebook metadata dicts
        """
        notebooks = []

        try:
            result = self._api_get("workspace/list", {"path": path})
            objects = result.get("objects", [])

            for obj in objects:
                obj_type = obj.get("object_type")
                obj_path = obj.get("path")

                if obj_type == "NOTEBOOK":
                    notebooks.append({
                        "name": obj_path.split("/")[-1],
                        "path": obj_path,
                        "language": obj.get("language", "UNKNOWN"),
                        "object_type": "notebook"
                    })
                elif obj_type == "DIRECTORY":
                    # Recurse into directory
                    notebooks.extend(self.list_notebooks(obj_path))

        except Exception as e:
            logger.warning(f"Error listing {path}: {e}")

        return notebooks

    def get_notebook_source(self, path: str) -> Optional[str]:
        """Get notebook source code.

        Args:
            path: Notebook path in workspace

        Returns:
            Source code as string, or None if error
        """
        try:
            result = self._api_get("workspace/export", {
                "path": path,
                "format": "SOURCE"
            })
            import base64
            content = result.get("content", "")
            return base64.b64decode(content).decode('utf-8')
        except Exception as e:
            logger.warning(f"Error getting source for {path}: {e}")
            return None

    def extract_notebooks(self, root_path: str = "/", include_source: bool = True) -> list[dict]:
        """Extract all notebooks with optional source code.

        Args:
            root_path: Starting path for extraction
            include_source: Whether to include source code (slower but more useful)

        Returns:
            List of notebook metadata with source
        """
        logger.info(f"Extracting notebooks from {root_path}...")
        notebooks = self.list_notebooks(root_path)
        logger.info(f"Found {len(notebooks)} notebooks")

        if include_source:
            logger.info("Extracting source code...")
            for i, nb in enumerate(notebooks):
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(notebooks)}")
                nb["source_code"] = self.get_notebook_source(nb["path"])

        return notebooks

    def list_jobs(self) -> list[dict]:
        """List all jobs in workspace.

        Returns:
            List of job metadata dicts
        """
        logger.info("Extracting jobs...")
        jobs = []

        try:
            result = self._api_get("jobs/list")
            raw_jobs = result.get("jobs", [])

            for job in raw_jobs:
                jobs.append({
                    "job_id": job.get("job_id"),
                    "name": job.get("settings", {}).get("name", f"job_{job.get('job_id')}"),
                    "tasks": job.get("settings", {}).get("tasks", []),
                    "schedule": job.get("settings", {}).get("schedule"),
                    "created_at": job.get("created_time"),
                    "creator_user_name": job.get("creator_user_name")
                })

            logger.info(f"Found {len(jobs)} jobs")

        except Exception as e:
            logger.error(f"Error listing jobs: {e}")

        return jobs

    def extract_all(self, root_path: str = "/", include_source: bool = True) -> dict:
        """Extract all metadata from workspace.

        Args:
            root_path: Starting path for notebooks
            include_source: Include notebook source code

        Returns:
            Dict with 'notebooks' and 'jobs' keys
        """
        return {
            "notebooks": self.extract_notebooks(root_path, include_source),
            "jobs": self.list_jobs(),
            "extracted_at": datetime.utcnow().isoformat(),
            "workspace": self.host
        }


def save_extractions(data: dict, output_dir: Path):
    """Save extracted data to JSON files.

    Args:
        data: Extraction results from extract_all()
        output_dir: Directory to save JSON files
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save notebooks
    notebooks_file = output_dir / "notebooks.json"
    with open(notebooks_file, 'w', encoding='utf-8') as f:
        json.dump(data.get("notebooks", []), f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(data.get('notebooks', []))} notebooks to {notebooks_file}")

    # Save jobs
    jobs_file = output_dir / "jobs.json"
    with open(jobs_file, 'w', encoding='utf-8') as f:
        json.dump(data.get("jobs", []), f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(data.get('jobs', []))} jobs to {jobs_file}")

    # Save extraction metadata
    meta_file = output_dir / "_extraction_meta.json"
    with open(meta_file, 'w', encoding='utf-8') as f:
        json.dump({
            "extracted_at": data.get("extracted_at"),
            "workspace": data.get("workspace"),
            "notebook_count": len(data.get("notebooks", [])),
            "job_count": len(data.get("jobs", []))
        }, f, indent=2)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract metadata from Databricks workspace"
    )
    parser.add_argument(
        "--host",
        required=True,
        help="Databricks workspace URL (e.g., https://xxx.azuredatabricks.net)"
    )
    parser.add_argument(
        "--token",
        help="Databricks personal access token (or set DATABRICKS_TOKEN env var)"
    )
    parser.add_argument(
        "--output", "-o",
        default="ade_data/demo/extractions/databricks",
        help="Output directory for JSON files"
    )
    parser.add_argument(
        "--path",
        default="/",
        help="Workspace path to extract (default: /)"
    )
    parser.add_argument(
        "--no-source",
        action="store_true",
        help="Skip extracting notebook source code (faster)"
    )

    args = parser.parse_args()

    # Get token from args or env
    token = args.token or os.environ.get("DATABRICKS_TOKEN")
    if not token:
        print("Error: Databricks token required. Use --token or set DATABRICKS_TOKEN env var")
        sys.exit(1)

    # Run extraction
    extractor = DatabricksExtractor(args.host, token)
    data = extractor.extract_all(
        root_path=args.path,
        include_source=not args.no_source
    )

    # Save results
    output_dir = Path(args.output)
    save_extractions(data, output_dir)

    print(f"\nExtraction complete!")
    print(f"  Notebooks: {len(data.get('notebooks', []))}")
    print(f"  Jobs: {len(data.get('jobs', []))}")
    print(f"  Output: {output_dir}")


if __name__ == "__main__":
    main()
