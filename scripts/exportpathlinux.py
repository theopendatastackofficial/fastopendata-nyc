#!/usr/bin/env python
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline.constants import DAGSTER_PATH

dagster_path = DAGSTER_PATH.replace("\\", "/")
dagster_home = dagster_path

# Print DAGSTER_HOME
print(dagster_home)
