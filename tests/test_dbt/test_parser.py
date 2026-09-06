"""Tests for dbt artifact parsing."""

import json
import tempfile
from pathlib import Path

import pytest

from drift.dbt.parser import (
    parse_manifest,
    parse_run_results,
    parse_dbt_target,
    DbtModel,
    DbtModelExecution,
    DbtRunResult,
)


@pytest.fixture
def sample_manifest():
    """Sample dbt manifest.json structure."""
    return {
        "metadata": {
            "project_name": "test_project",
            "dbt_version": "1.5.0"
        },
        "nodes": {
            "model.test_project.users": {
                "resource_type": "model",
                "unique_id": "model.test_project.users",
                "name": "users",
                "schema": "public",
                "database": "analytics",
                "alias": "dim_users",
                "compiled_code": "SELECT id, name, email FROM raw.users WHERE active = true",
                "description": "User dimension table",
                "config": {
                    "materialized": "table"
                },
                "depends_on": {
                    "nodes": ["source.test_project.raw.users"]
                },
                "tags": ["core", "pii"]
            },
            "model.test_project.orders": {
                "resource_type": "model",
                "unique_id": "model.test_project.orders",
                "name": "orders",
                "schema": "public",
                "database": "analytics",
                "compiled_code": "SELECT * FROM raw.orders",
                "config": {
                    "materialized": "incremental"
                },
                "depends_on": {
                    "nodes": ["model.test_project.users", "source.test_project.raw.orders"]
                },
                "tags": []
            },
            "source.test_project.raw.users": {
                "resource_type": "source",
                "unique_id": "source.test_project.raw.users",
                "name": "users"
            }
        }
    }


@pytest.fixture
def sample_run_results():
    """Sample dbt run_results.json structure."""
    return {
        "metadata": {
            "invocation_id": "abc-123-def",
            "dbt_version": "1.5.0"
        },
        "elapsed_time": 45.5,
        "results": [
            {
                "unique_id": "model.test_project.users",
                "status": "success",
                "execution_time": 12.3,
                "adapter_response": {
                    "rows_affected": 1000
                }
            },
            {
                "unique_id": "model.test_project.orders",
                "status": "success",
                "execution_time": 33.2,
                "adapter_response": {
                    "rows_affected": 5000
                }
            }
        ],
        "args": {
            "which": "run"
        }
    }


class TestParseManifest:
    """Tests for parse_manifest function."""

    def test_parse_models_from_manifest(self, sample_manifest, tmp_path):
        """Test parsing models from manifest.json."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))

        models = parse_manifest(manifest_path)

        assert len(models) == 2
        assert "model.test_project.users" in models
        assert "model.test_project.orders" in models

    def test_model_attributes(self, sample_manifest, tmp_path):
        """Test that model attributes are correctly parsed."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))

        models = parse_manifest(manifest_path)
        users_model = models["model.test_project.users"]

        assert users_model.unique_id == "model.test_project.users"
        assert users_model.name == "users"
        assert users_model.schema_name == "public"
        assert users_model.database == "analytics"
        assert users_model.alias == "dim_users"
        assert users_model.materialized == "table"
        assert users_model.description == "User dimension table"
        assert "SELECT" in users_model.compiled_sql
        assert users_model.tags == ["core", "pii"]

    def test_depends_on_filters_to_models(self, sample_manifest, tmp_path):
        """Test that depends_on only includes model references."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))

        models = parse_manifest(manifest_path)
        orders_model = models["model.test_project.orders"]

        # Should only include model dependencies, not sources
        assert "model.test_project.users" in orders_model.depends_on
        assert "source.test_project.raw.orders" not in orders_model.depends_on

    def test_excludes_non_models(self, sample_manifest, tmp_path):
        """Test that non-model nodes are excluded."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))

        models = parse_manifest(manifest_path)

        # Source nodes should not be included
        assert "source.test_project.raw.users" not in models

    def test_relation_name_generated(self, sample_manifest, tmp_path):
        """Test that relation_name is properly generated."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(sample_manifest))

        models = parse_manifest(manifest_path)

        # Users model has alias, so relation should use alias
        assert models["model.test_project.users"].relation_name == "public.dim_users"
        # Orders model has no alias, so relation uses name
        assert models["model.test_project.orders"].relation_name == "public.orders"


class TestParseRunResults:
    """Tests for parse_run_results function."""

    def test_parse_run_results(self, sample_run_results, tmp_path):
        """Test parsing run_results.json."""
        results_path = tmp_path / "run_results.json"
        results_path.write_text(json.dumps(sample_run_results))

        run_result = parse_run_results(results_path)

        assert run_result.invocation_id == "abc-123-def"
        assert run_result.status == "success"
        assert len(run_result.executions) == 2

    def test_execution_details(self, sample_run_results, tmp_path):
        """Test that execution details are correctly parsed."""
        results_path = tmp_path / "run_results.json"
        results_path.write_text(json.dumps(sample_run_results))

        run_result = parse_run_results(results_path)
        users_exec = next(e for e in run_result.executions if e.unique_id == "model.test_project.users")

        assert users_exec.status == "success"
        assert users_exec.execution_time == 12.3
        assert users_exec.rows_affected == 1000

    def test_error_status_when_any_error(self, sample_run_results, tmp_path):
        """Test that overall status is error if any model failed."""
        sample_run_results["results"][0]["status"] = "error"
        results_path = tmp_path / "run_results.json"
        results_path.write_text(json.dumps(sample_run_results))

        run_result = parse_run_results(results_path)

        assert run_result.status == "error"


class TestParseDbtTarget:
    """Tests for parse_dbt_target function."""

    def test_parse_target_with_both_files(self, sample_manifest, sample_run_results, tmp_path):
        """Test parsing target directory with both files."""
        target_dir = tmp_path / "target"
        target_dir.mkdir()

        (target_dir / "manifest.json").write_text(json.dumps(sample_manifest))
        (target_dir / "run_results.json").write_text(json.dumps(sample_run_results))

        models, run_result = parse_dbt_target(target_dir)

        assert len(models) == 2
        assert run_result is not None
        assert run_result.invocation_id == "abc-123-def"

    def test_parse_target_manifest_only(self, sample_manifest, tmp_path):
        """Test parsing target directory with only manifest."""
        target_dir = tmp_path / "target"
        target_dir.mkdir()

        (target_dir / "manifest.json").write_text(json.dumps(sample_manifest))

        models, run_result = parse_dbt_target(target_dir)

        assert len(models) == 2
        assert run_result is None

    def test_parse_target_no_manifest_raises(self, tmp_path):
        """Test that missing manifest.json raises FileNotFoundError."""
        target_dir = tmp_path / "target"
        target_dir.mkdir()

        with pytest.raises(FileNotFoundError):
            parse_dbt_target(target_dir)
