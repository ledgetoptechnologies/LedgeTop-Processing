"""Keep generated test data in the ignored local output tree."""

from pathlib import Path
from uuid import uuid4


def pytest_configure(config):
    if config.option.basetemp is None:
        # Avoid a shared pytest-of-user directory owned by another Windows
        # execution context. Unique paths also isolate concurrent test runs.
        root = Path(config.rootpath) / "outputs" / "pytest"
        root.mkdir(parents=True, exist_ok=True)
        config.option.basetemp = str(root / uuid4().hex)
