# Databricks notebook source

import os
import sys
from pathlib import Path

from pyspark.sql.functions import col, lit

# Local imports for testing
project_root = Path(os.getcwd()).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
print(f"Added to sys.path for imports: {project_root}")

# --- Import UDFs and Registration function ---
from udf_tools import generate_contours_udf, generate_contours_wkb

output_schema = "plattform_dataprodukter_dev.hoydekurver"

# Create output schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

# COMMAND ----------

dtm_table = spark.table("sjo_sehavnivaa.raster_silver.dtm1")

small_tile_selection = [33122116005003, 33122116006003, 33122116005004, 33122116006004]

dtm_selection_df = dtm_table.filter(
    dtm_table.kartblad_tile_id.isin(small_tile_selection)
)

dtm_selection_df.count()

# COMMAND ----------

# Apply the UDF to create a new column with the WKB geometry
contours_df = dtm_selection_df.withColumn(
    "contours_wkb", generate_contours_udf(col("tile_geotiff"), lit(10), lit(0))
)

# Write to output schema
contours_df.select("kartblad_tile_id", "contours_wkb").write.mode(
    "overwrite"
).saveAsTable(f"{output_schema}.contours_test_output")


# COMMAND ----------
# Test generate_contours_wkb
# Collect one raster binary from the DataFrame
raster_binary = dtm_selection_df.first()["tile_geotiff"]

# Call the function with the collected raster binary
contours_wkb = generate_contours_wkb(raster_binary, interval=10, base=0)
