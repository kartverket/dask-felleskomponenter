# Databricks notebook source

import os
import sys
from pathlib import Path

from osgeo import ogr

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.sql.functions import expr, col, lit

# Local imports for testing
project_root = Path(os.getcwd()).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
print(f"Added to sys.path for imports: {project_root}")

# --- Import UDFs and Registration function ---
from udf_tools import get_wkb_geom_type
from udf_conversions import curved_to_linear_wkb

# COMMAND ----------


def generate_test_data_with_sedona(spark: SparkSession):
    """
    Generates a comprehensive test dataset including both standard WKB and Sedona-generated EWKB.

    1. Generates a baseline set for ALL geometries using OGR (standard WKB).
    2. Generates an additional set for all NON-CURVED geometries using Sedona (EWKB).
    """
    wkt_test_cases = {
        # Standard Linear Geometries
        "point_2d": "POINT (10 20)",
        "linestring_2d": "LINESTRING (10 10, 20 20, 30 15)",
        "polygon_2d": "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
        "multipolygon_2d": "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))",
        # Curved (Complex) Geometries
        "circularstring_2d": "CIRCULARSTRING (0 0, 1 1, 2 0)",
        "compoundcurve_2d": "COMPOUNDCURVE (CIRCULARSTRING (0 0, 1 1, 2 0), (2 0, 3 5))",
        "curvepolygon_2d": "CURVEPOLYGON (CIRCULARSTRING (0 0, 4 0, 4 4, 0 4, 0 0))",
        "multicurve_2d": "MULTICURVE (LINESTRING (0 0, 1 1), CIRCULARSTRING (2 2, 3 3, 4 2))",
        "multisurface_2d": "MULTISURFACE (CURVEPOLYGON (CIRCULARSTRING (0 0, 4 0, 4 4, 0 4, 0 0)))",
        # 3D and Measured Geometries
        "point_3d_z": "POINT Z (10 20 5)",
        "linestring_3d_z": "LINESTRING Z (10 10 5, 20 20 10)",
        "point_3d_m": "POINT M (10 20 3)",
        "point_4d_zm": "POINT ZM (10 20 5 3)",
        # Empty Geometries
        "point_empty": "POINT EMPTY",
        "polygon_empty": "POLYGON EMPTY",
    }

    final_prepared_data = []

    # Generate a complete baseline set using OGR (standard WKB) ---
    print("--- Generating baseline WKB data using OGR ---")
    for name, wkt_string in wkt_test_cases.items():
        geom = ogr.CreateGeometryFromWkt(wkt_string)
        wkb_bytes = geom.ExportToWkb()
        wkb_hex = wkb_bytes.hex().upper()
        final_prepared_data.append((name, wkt_string, wkb_hex, wkb_bytes))
    print(f"Generated {len(final_prepared_data)} baseline WKB test cases.")

    # Generate an additional set of EWKB using Sedona for non-curved types ---
    print("\n--- Generating additional EWKB data using Sedona ---")
    CURVED_KEYWORDS = [
        "CIRCULARSTRING",
        "COMPOUNDCURVE",
        "CURVEPOLYGON",
        "MULTICURVE",
        "MULTISURFACE",
    ]
    sedona_process_list = []
    for name, wkt_string in wkt_test_cases.items():
        if not any(keyword in wkt_string for keyword in CURVED_KEYWORDS):
            sedona_process_list.append((name, wkt_string))

    if sedona_process_list:
        sedona_df = spark.createDataFrame(sedona_process_list, ["name", "wkt"])
        ewkb_df = sedona_df.withColumn(
            "wkb_bytes", expr("ST_AsEWKB(ST_SetSRID(ST_GeomFromWKT(wkt), 4326))")
        ).withColumn("wkb_hex", expr("hex(wkb_bytes)"))

        for row in ewkb_df.collect():
            ewkb_name = f"{row['name']}_ewkb"
            final_prepared_data.append(
                (ewkb_name, row["wkt"], row["wkb_hex"], row["wkb_bytes"])
            )
        print(f"Generated {len(sedona_process_list)} additional EWKB test cases.")

    print(f"\nTotal test cases generated: {len(final_prepared_data)}")
    return final_prepared_data


# COMMAND ----------

test_data = generate_test_data_with_sedona(spark)
schema = StructType(
    [
        StructField("name", StringType(), False),
        StructField("wkt", StringType(), False),
        StructField("wkb_hex", StringType(), True),
        StructField("wkb_bytes", BinaryType(), True),
    ]
)

test_df = spark.createDataFrame(test_data, schema).cache()
test_df.createOrReplaceTempView("test_dataframe")

test_df.display()

# COMMAND ----------

# Test get_wkb_geom_type by adding a new column to the DataFrame
# Note on Sedona EWKB: ST_SetSRID will ignore the M coordinate if present.
test_df = test_df.withColumn("geom_type", get_wkb_geom_type(test_df.wkb_bytes))
test_df.select("name", "wkt", "geom_type").display()


# COMMAND ----------
# Test curved_to_linear_wkb

print("Applying the curved_to_linear_wkb UDF...")

# Apply the conversion UDF to get the linearized WKB,
# then apply our validated get_wkb_geom_type UDF to check the result.
results_df = test_df.withColumn(
    "linear_wkb", curved_to_linear_wkb(test_df.wkb_bytes, lit(5.0))
).withColumn("linear_geom_type", get_wkb_geom_type(col("linear_wkb")))

# Display a summary table for easy verification
# Original type vs. the type after conversion
print("Verification Results (Original vs. Linearized):")
results_df.select(
    "name",
    col("geom_type").alias("original_type"),
    col("linear_geom_type").alias("converted_type"),
).display()
