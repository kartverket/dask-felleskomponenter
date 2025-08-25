# Databricks notebook source

import os
import sys
from pathlib import Path

# Local imports for testing
project_root = Path(os.getcwd()).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
print(f"Added to sys.path for imports: {project_root}")

# --- Import UDFs and Registration function ---
from udf_tools import generate_contours_udf

# COMMAND ----------
