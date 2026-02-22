"""
ADE Databricks Platform

Metadata extraction from Databricks workspaces.
Supports notebooks, jobs, and clusters.
"""

from .extractor import DatabricksExtractor

__all__ = ["DatabricksExtractor"]
