from osgeo import ogr

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType


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
    """
    Registers the 'curved_to_linear_wkb' UDF with the provided SparkSession.

    This function registers a user-defined function (UDF) named 'curved_to_linear_wkb' in the given SparkSession,
    making it available for use in Spark SQL queries. The UDF converts curved geometries to their
    linear representations in Well-Known Binary (WKB) format.

    Args:
        spark (SparkSession): The SparkSession instance to register the UDF with.

    Returns:
        None
    """
    curved_to_linear_wkb_sql_name = "curved_to_linear_wkb"
    spark.udf.register(curved_to_linear_wkb_sql_name, curved_to_linear_wkb)
    print(f"Registered SQL function '{curved_to_linear_wkb_sql_name}'")


def register_all_udfs(spark: SparkSession):
    """
    Registers all user-defined functions (UDFs) in udf_conversions with the provided SparkSession.

    Args:
        spark (SparkSession): The SparkSession instance to register the UDFs with.

    This function calls individual UDF registration functions to ensure that all necessary UDFs are available
    for use in Spark SQL queries and DataFrame operations.
    """
    register_curved_to_linear_wkb_to_spark(spark)
