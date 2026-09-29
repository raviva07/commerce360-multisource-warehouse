import importlib
import shutil
from pathlib import Path
import pytest

@pytest.fixture(autouse=True)
def isolated_project(tmp_path, monkeypatch, request):
    source = Path(__file__).resolve().parents[1]
    target = tmp_path / "project"
    shutil.copytree(source, target, ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache", ".venv", "warehouse", "state", "model", "feature_store"))
    for module_file in (source / "src").glob("*.py"):
        module = importlib.import_module("src." + module_file.stem)
        if hasattr(module, "ROOT"):
            monkeypatch.setattr(module, "ROOT", target)
    monkeypatch.setattr(request.module, "ROOT", target)
