from osgeo import ogr
from pyspark.sql.types import BinaryType
from pyspark.sql.functions import udf


def curved_to_linear_wkb(geometry, dfMaxAngleStepSizeDegrees=0):
    if geometry is None:
        return None

    geometry_serialized = ogr.CreateGeometryFromWkb(geometry)

    if geometry_serialized is None:
        return None

    linear_geometry = geometry_serialized.GetLinearGeometry(dfMaxAngleStepSizeDegrees)
    return linear_geometry.ExportToWkb()


def register_curved_to_linear_wkb_to_udf(spark):
    curved_to_linear_wkb_sql_name = "curved_to_linear_wkb"
    curved_to_linear_wkb_udf = udf(curved_to_linear_wkb, BinaryType())
    spark.udf.register(curved_to_linear_wkb_sql_name, curved_to_linear_wkb_udf)
    print(f"Registered SQL function '{curved_to_linear_wkb_sql_name}'")


def register_all_udfs(spark):
    register_curved_to_linear_wkb_to_udf(spark)


# Usage:
# from udf_conversions import register_all_udfs
# register_all_udfs(spark)
