from __future__ import annotations

from pathlib import Path


def test_ci_workflow_includes_web_job() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "jobs:" in workflow
    assert "lint-and-test:" in workflow
    assert "\n  web:\n" in workflow
    assert "working-directory: web" in workflow
    assert "cache-dependency-path: web/package-lock.json" in workflow
    assert "run: npm ci" in workflow
    assert "run: npm run lint" in workflow
    assert "run: npm run typecheck" in workflow
    assert "run: npm run build" in workflow
    assert "uv run pytest --tb=short -q --cov=src/atlas_stf --cov-report=term-missing --cov-fail-under=83" in workflow
