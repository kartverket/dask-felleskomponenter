import struct
from osgeo import ogr

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType

# EWKB bit flag for the SRID
EWKB_SRID_FLAG = 0x20000000


@udf(BinaryType())
def curved_to_linear_wkb(geometry, dfMaxAngleStepSizeDegrees: float = 0.0):
    """
    Converts a curved geometry in WKB/EWKB format to its linearized equivalent.
    This version is EWKB-aware: it removes both the SRID flag and value
    before passing the geometry to OGR for processing.
    """
    if not isinstance(geometry, (bytes, bytearray)):
        return None

    wkb_bytes = geometry

    # EWKB SRID STRIPPING
    if len(wkb_bytes) >= 9:  # Minimum length for a header with SRID
        byte_order_char = "<" if wkb_bytes[0] == 1 else ">"
        geom_type_int = struct.unpack(f"{byte_order_char}I", wkb_bytes[1:5])[0]

        # Check if the SRID flag is set in the type integer
        if (geom_type_int & EWKB_SRID_FLAG) != 0:

            # Create the new, standard WKB type integer by removing the SRID flag.
            new_geom_type_int = geom_type_int & ~EWKB_SRID_FLAG

            # Re-pack the header with the corrected type integer.
            header = bytearray()
            header.append(wkb_bytes[0])  # Copy byte order
            header.extend(struct.pack(f"{byte_order_char}I", new_geom_type_int))

            # Get the body, skipping the 4-byte SRID value that follows the header.
            body = wkb_bytes[9:]

            # Reassemble the final, standard WKB byte string.
            wkb_bytes = bytes(header + body)

    try:
        geometry_serialized = ogr.CreateGeometryFromWkb(wkb_bytes)
        if geometry_serialized is None:
            return None

        # Cast the angle to a float
        angle = float(
            dfMaxAngleStepSizeDegrees if dfMaxAngleStepSizeDegrees is not None else 0.0
        )

        linear_geometry = geometry_serialized.GetLinearGeometry(angle)
        return linear_geometry.ExportToWkb()
    except Exception:
        return None


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
