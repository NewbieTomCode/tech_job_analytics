# conftest.py - configure pytest to find the src package
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
