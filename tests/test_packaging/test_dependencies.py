from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_requests_dependency_declared():
    requirements = (PROJECT_ROOT / "requirements.txt").read_text().splitlines()
    assert any(line.strip().startswith("requests") for line in requirements), "requests must be declared in requirements.txt"
