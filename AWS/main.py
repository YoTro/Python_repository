import sys
import os

# Adjust sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.entry.cli.main import main

if __name__ == "__main__":
    main()
