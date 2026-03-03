"""Tests for Databricks extractor — save_to_catalog with real demo JSON data."""

import json
import pytest
from pathlib import Path

from ade_app.platforms.databricks.extractor import save_to_catalog
from ade_app.core.catalog import CatalogDB

DEMO_DIR = Path(__file__).resolve().parent.parent / "ade_data/demo/extractions/databricks"


@pytest.fixture
def demo_data():
    """Load the real demo JSON files as extraction data."""
    notebooks = json.loads((DEMO_DIR / "notebooks.json").read_text(encoding="utf-8"))
    jobs = json.loads((DEMO_DIR / "jobs.json").read_text(encoding="utf-8"))
    return {
        "notebooks": notebooks,
        "jobs": jobs,
        "workspace": "https://demo.azuredatabricks.net",
        "extracted_at": "2025-01-01T00:00:00",
    }


@pytest.fixture
def catalog_db(tmp_path, demo_data):
    db_path = tmp_path / "test.db"
    save_to_catalog(demo_data, db_path)
    catalog = CatalogDB(db_path)
    yield catalog
    catalog.close()


class TestSaveToCatalog:
    def test_total_objects(self, catalog_db):
        stats = catalog_db.get_stats(platform="databricks")
        total = sum(stats.values())
        assert total == 7  # 5 notebooks + 2 jobs

    def test_notebooks_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="databricks")
        assert stats["notebook"] == 5

    def test_jobs_stored(self, catalog_db):
        stats = catalog_db.get_stats(platform="databricks")
        assert stats["job"] == 2

    def test_notebook_has_source_code(self, catalog_db):
        nb = catalog_db.get_object("01_ingest_raw_sales")
        assert nb is not None
        assert nb["source_code"] is not None
        assert len(nb["source_code"]) > 0

    def test_notebook_has_path(self, catalog_db):
        nb = catalog_db.get_object("01_ingest_raw_sales")
        assert nb["path"] != ""

    def test_notebook_language_metadata(self, catalog_db):
        nb = catalog_db.get_object("01_ingest_raw_sales")
        assert nb["language"].lower() == "python"

    def test_job_has_metadata(self, catalog_db):
        job = catalog_db.get_object("daily_sales_pipeline")
        assert job is not None
        assert job["job_id"] is not None
        assert job["tasks"] is not None
        assert len(job["tasks"]) > 0

    def test_job_has_schedule(self, catalog_db):
        job = catalog_db.get_object("daily_sales_pipeline")
        assert job["schedule"] is not None

    def test_search_notebook_source(self, catalog_db):
        results = catalog_db.search("spark")
        assert len(results) > 0

    def test_extraction_recorded(self, catalog_db):
        row = catalog_db.conn.execute(
            "SELECT * FROM extraction_meta WHERE platform = 'databricks'"
        ).fetchone()
        assert row is not None
        assert row["object_count"] == 7

    def test_clear_and_re_extract(self, tmp_path, demo_data):
        db_path = tmp_path / "reextract.db"
        save_to_catalog(demo_data, db_path)
        save_to_catalog(demo_data, db_path)  # second run should clear first
        catalog = CatalogDB(db_path)
        stats = catalog.get_stats(platform="databricks")
        assert sum(stats.values()) == 7  # not doubled
        catalog.close()


class TestSaveToCatalogEdgeCases:
    def test_empty_data(self, tmp_path):
        db_path = tmp_path / "empty.db"
        count = save_to_catalog({"notebooks": [], "jobs": []}, db_path)
        assert count == 0

    def test_notebook_without_source(self, tmp_path):
        db_path = tmp_path / "nosource.db"
        data = {
            "notebooks": [{"name": "test_nb", "path": "/test", "language": "python"}],
            "jobs": [],
        }
        count = save_to_catalog(data, db_path)
        assert count == 1
        catalog = CatalogDB(db_path)
        nb = catalog.get_object("test_nb")
        assert nb is not None
        assert nb["source_code"] is None
        catalog.close()
