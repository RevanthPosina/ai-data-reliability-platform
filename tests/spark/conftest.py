import sys
from pathlib import Path

# tests/ is mounted flat in the container; make the sibling test modules importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
