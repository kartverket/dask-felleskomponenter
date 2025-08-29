# Databricks notebook source

# MAGIC %md
# MAGIC # Tests for UDF Registration and SQL Functionality
# MAGIC
# MAGIC This notebook contains tests for the `register_generate_contours_udf` and the corresponding SQL function `generate_contours_udf`.

# COMMAND ----------

import os
import sys
from pathlib import Path

from pyspark.sql.functions import col
from pyspark.sql.types import BinaryType

# Local imports for testing
project_root = Path(os.getcwd()).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
print(f"Added to sys.path for imports: {project_root}")

# --- Import UDFs and Registration function ---
from udf_tools import (
    register_generate_contours_udf,
    register_all_udfs,
)

output_schema = "plattform_dataprodukter_dev.hoydekurver_tests"

# Create output schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: Create Sample Data
# MAGIC
# MAGIC Recreate the initial DataFrame to be used for testing.

# COMMAND ----------

dtm_table = spark.table("sjo_sehavnivaa.raster_silver.dtm1")

small_tile_selection = [33122116005003, 33122116006003, 33122116005004, 33122116006004]

dtm_selection_df = dtm_table.filter(
    dtm_table.kartblad_tile_id.isin(small_tile_selection)
)

# Create a temporary view to test the SQL UDF
dtm_selection_df.createOrReplaceTempView("dtm_selection_for_sql_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: `register_generate_contours_udf`
# MAGIC
# MAGIC This test verifies that the `register_generate_contours_udf` function correctly registers the UDF with the Spark session.

# COMMAND ----------

# Register the UDF
register_generate_contours_udf(spark)

# Check if the UDF is in the list of registered functions
registered_functions = [f.name for f in spark.catalog.listFunctions()]
assert (
    "generate_contours_udf" in registered_functions
), "Test Failed: `generate_contours_udf` was not registered."

print("Test Passed: `register_generate_contours_udf` successfully registered the UDF.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: SQL `generate_contours_udf` Functionality
# MAGIC
# MAGIC This test executes a SQL query using the registered `generate_contours_udf` to ensure it works as expected.

# COMMAND ----------

# Use the registered UDF in a Spark SQL query
contours_sql_df = spark.sql(
    """
    SELECT
        kartblad_tile_id,
        generate_contours_udf(tile_geotiff, 10, 0) AS contours_wkb_sql
    FROM dtm_selection_for_sql_test
    """
)

# --- Assertions ---

# 1. Check if the resulting DataFrame has the expected columns
expected_columns = ["kartblad_tile_id", "contours_wkb_sql"]
assert all(
    col in contours_sql_df.columns for col in expected_columns
), f"Test Failed: Columns do not match. Expected {expected_columns}, got {contours_sql_df.columns}"

# 2. Check the data type of the generated column
assert isinstance(
    contours_sql_df.schema["contours_wkb_sql"].dataType, BinaryType
), "Test Failed: The output column `contours_wkb_sql` is not of BinaryType."

# 3. Check if the output is not null for a valid input
assert (
    contours_sql_df.filter(col("contours_wkb_sql").isNotNull()).count() > 0
), "Test Failed: No contours were generated, or all were null."

print(
    "Test Passed: SQL function `generate_contours_udf` executed successfully and passed all assertions."
)
display(contours_sql_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test: `register_all_udfs`
# MAGIC
# MAGIC This test verifies that the `register_all_udfs` function correctly calls all the individual UDF registration functions.

# COMMAND ----------

# Register all UDFs
register_all_udfs(spark)

# Check if the UDFs are in the list of registered functions
registered_functions = [f.name for f in spark.catalog.listFunctions()]
assert (
    "generate_contours_udf" in registered_functions
), "Test Failed: `generate_contours_udf` was not registered by `register_all_udfs`."
# You can add asserts for other UDFs registered by `register_all_udfs` here

print("Test Passed: `register_all_udfs` ran successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC
# MAGIC Remove the temporary view created for the tests.

# COMMAND ----------

spark.catalog.dropTempView("dtm_selection_for_sql_test")
print("Cleanup complete: Dropped temporary view `dtm_selection_for_sql_test`.")
