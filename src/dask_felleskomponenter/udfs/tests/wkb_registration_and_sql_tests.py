# Databricks notebook source

# MAGIC %md
# MAGIC # Tests for WKB UDF Registration and SQL Functionality
# MAGIC
# MAGIC This notebook tests the registration and SQL invocation for UDFs from two separate modules: `udf_tools` and `udf_conversions`.

# COMMAND ----------

import os
import sys
from pathlib import Path

from osgeo import ogr
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.sql.functions import expr, col

# Local imports for testing
project_root = Path(os.getcwd()).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
print(f"Added to sys.path for imports: {project_root}")

# --- Corrected Import UDFs and Registration functions from their respective modules ---

# Import from udf_conversions
from udf_conversions import (
    register_curved_to_linear_wkb_to_spark,
    register_all_udfs as register_all_conversion_udfs,  # Using alias to avoid name collision
)

# Import from udf_tools
from udf_tools import (
    register_get_wkb_geom_type_to_spark,
    register_all_udfs as register_all_tool_udfs,  # Using alias to avoid name collision
)

print(
    "Successfully imported UDFs and registration functions from both 'udf_tools' and 'udf_conversions'."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: Create Sample Data
# MAGIC
# MAGIC Generate a comprehensive test DataFrame with various WKB and EWKB geometry types. This data will be used to test the SQL functions.

# COMMAND ----------


def generate_test_data_with_sedona(spark: SparkSession):
    """
    Generates a comprehensive test dataset including both standard WKB and Sedona-generated EWKB.
    """
    wkt_test_cases = {
        # Standard Linear Geometries
        "point_2d": "POINT (10 20)",
        "linestring_2d": "LINESTRING (10 10, 20 20, 30 15)",
        "polygon_2d": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
        # Curved (Complex) Geometries
        "curvepolygon_2d": "CURVEPOLYGON (CIRCULARSTRING (0 0, 4 0, 4 4, 0 4, 0 0))",
        "multicurve_2d": "MULTICURVE (LINESTRING (0 0, 1 1), CIRCULARSTRING (2 2, 3 3, 4 2))",
        # 3D and Measured Geometries
        "point_3d_z": "POINT Z (10 20 5)",
        "point_4d_zm": "POINT ZM (10 20 5 3)",
        # Empty Geometries
        "point_empty": "POINT EMPTY",
    }
    final_prepared_data = []
    # --- Generate a complete baseline set using OGR (standard WKB) ---
    for name, wkt_string in wkt_test_cases.items():
        geom = ogr.CreateGeometryFromWkt(wkt_string)
        wkb_bytes = geom.ExportToWkb()
        final_prepared_data.append((name, wkt_string, wkb_bytes))

    # --- Generate an additional set of EWKB using Sedona for non-curved types ---
    sedona_process_list = [
        ("point_2d_ewkb", "POINT (10 20)"),
        ("linestring_3d_z_ewkb", "LINESTRING Z (10 10 5, 20 20 10)"),
    ]
    sedona_df = spark.createDataFrame(sedona_process_list, ["name", "wkt"])
    ewkb_df = sedona_df.withColumn(
        "wkb_bytes", expr("ST_AsEWKB(ST_SetSRID(ST_GeomFromWKT(wkt), 4326))")
    )
    for row in ewkb_df.collect():
        final_prepared_data.append((row["name"], row["wkt"], row["wkb_bytes"]))
    return final_prepared_data


# Generate data and create a temporary view for SQL testing
test_data = generate_test_data_with_sedona(spark)
schema = StructType(
    [
        StructField("name", StringType(), False),
        StructField("wkt", StringType(), False),
        StructField("wkb_bytes", BinaryType(), True),
    ]
)
test_df = spark.createDataFrame(test_data, schema).cache()
test_df.createOrReplaceTempView("wkb_test_data")

print("Test data generated and temporary view 'wkb_test_data' created.")
display(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: `get_wkb_geom_type` (from `udf_tools`)
# MAGIC
# MAGIC This test verifies that the `register_get_wkb_geom_type_to_spark` function correctly registers the UDF and that it can be successfully called from SQL.

# COMMAND ----------
# Register the UDF from udf_tools
register_get_wkb_geom_type_to_spark(spark)

# Check for registration
registered_functions = [f.name for f in spark.catalog.listFunctions()]
assert (
    "get_wkb_geom_type" in registered_functions
), "Test Failed: `get_wkb_geom_type` was not registered."
print(
    "Test Passed: `register_get_wkb_geom_type_to_spark` successfully registered the UDF."
)

# Use the registered UDF in a Spark SQL query
geom_type_sql_df = spark.sql(
    """
    SELECT name, get_wkb_geom_type(wkb_bytes) AS geom_type FROM wkb_test_data
"""
)

# Assertions
point_z_geom_type = geom_type_sql_df.filter(col("name") == "point_3d_z").first()[
    "geom_type"
]
assert (
    point_z_geom_type == "Point Z"
), f"Test Failed: Expected 'Point Z', but got '{point_z_geom_type}'."
ewkb_geom_type = geom_type_sql_df.filter(col("name") == "linestring_3d_z_ewkb").first()[
    "geom_type"
]
assert (
    ewkb_geom_type == "LineString Z"
), f"Test Failed: Expected 'LineString Z' for EWKB, but got '{ewkb_geom_type}'."

print(
    "Test Passed: SQL function `get_wkb_geom_type` executed successfully and passed all assertions."
)
display(geom_type_sql_df.select("name", "geom_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: `curved_to_linear_wkb` (from `udf_conversions`)
# MAGIC
# MAGIC This test verifies the registration and SQL functionality of the `curved_to_linear_wkb` UDF. It depends on `get_wkb_geom_type` from the previous test.

# COMMAND ----------
# Register the UDF from udf_conversions
register_curved_to_linear_wkb_to_spark(spark)

# Check for registration
registered_functions = [f.name for f in spark.catalog.listFunctions()]
assert (
    "curved_to_linear_wkb" in registered_functions
), "Test Failed: `curved_to_linear_wkb` was not registered."
print(
    "Test Passed: `register_curved_to_linear_wkb_to_spark` successfully registered the UDF."
)

# Use both registered UDFs in a Spark SQL query for verification
linear_sql_df = spark.sql(
    """
    SELECT
        name,
        get_wkb_geom_type(wkb_bytes) AS original_type,
        get_wkb_geom_type(curved_to_linear_wkb(wkb_bytes, 5.0)) AS converted_type
    FROM wkb_test_data
"""
)

# Assertions
curvepoly_result = linear_sql_df.filter(col("name") == "curvepolygon_2d").first()
assert (
    curvepoly_result["original_type"] == "CurvePolygon"
), f"Test Failed: Expected original type 'CurvePolygon', but got '{curvepoly_result['original_type']}'."
assert (
    curvepoly_result["converted_type"] == "Polygon"
), f"Test Failed: Expected converted type 'Polygon', but got '{curvepoly_result['converted_type']}'."

print(
    "Test Passed: SQL function `curved_to_linear_wkb` executed successfully and passed all assertions."
)
display(linear_sql_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: `register_all_udfs` from both modules
# MAGIC
# MAGIC This integration test ensures that the aggregator registration functions from both modules work as expected.

# COMMAND ----------
# We can run these again to ensure they are idempotent.
print("Calling `register_all_tool_udfs` from udf_tools...")
register_all_tool_udfs(spark)

print("Calling `register_all_conversion_udfs` from udf_conversions...")
register_all_conversion_udfs(spark)

# Check that UDFs from both modules are now registered
registered_functions = [f.name for f in spark.catalog.listFunctions()]

assert (
    "get_wkb_geom_type" in registered_functions
), "Test Failed: `get_wkb_geom_type` was not registered by `register_all_tool_udfs`."
assert (
    "curved_to_linear_wkb" in registered_functions
), "Test Failed: `curved_to_linear_wkb` was not registered by `register_all_conversion_udfs`."

print(
    "\nTest Passed: Both `register_all_udfs` functions ran successfully, registering UDFs from their respective modules."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC
# MAGIC Remove the temporary view created for the tests.

# COMMAND ----------

spark.catalog.dropTempView("wkb_test_data")
print("Cleanup complete: Dropped temporary view `wkb_test_data`.")
