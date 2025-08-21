from osgeo import ogr
from pyspark.sql.types import BinaryType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession


@udf(BinaryType())
def curved_to_linear_wkb(geometry: BinaryType, dfMaxAngleStepSizeDegrees: float = 0.0):
    """
    Converts a curved geometry in WKB format to its linearized equivalent.

    This function takes a geometry in Well-Known Binary (WKB) format, deserializes it,
    and converts any curved segments (such as arcs) to linear segments using the specified
    maximum angle step size. The result is returned in WKB format.

    Args:
        geometry (bytes): The input geometry in WKB format. Can be None.
        dfMaxAngleStepSizeDegrees (float, optional): The maximum angle step size in degrees
            for linearizing curved segments. Defaults to 0.0 (library default).

    Returns:
        bytes or None: The linearized geometry in WKB format, or None if the input is None
            or invalid.
    """
    if geometry is None:
        return None

    geometry_serialized = ogr.CreateGeometryFromWkb(geometry)

    if geometry_serialized is None:
        return None

    linear_geometry = geometry_serialized.GetLinearGeometry(dfMaxAngleStepSizeDegrees)
    return linear_geometry.ExportToWkb()


def register_curved_to_linear_wkb_to_spark(spark: SparkSession):
    curved_to_linear_wkb_sql_name = "curved_to_linear_wkb"
    spark.udf.register(curved_to_linear_wkb_sql_name, curved_to_linear_wkb)
    print(f"Registered SQL function '{curved_to_linear_wkb_sql_name}'")


def register_all_udfs(spark: SparkSession):
    """
    Registers all user-defined functions (UDFs) required for the application with the provided Spark session.

    Currently, this function registers the curved-to-linear WKB conversion UDF.

    Args:
        spark (pyspark.sql.SparkSession): The Spark session to which the UDFs will be registered.
    """
    register_curved_to_linear_wkb_to_spark(spark)
