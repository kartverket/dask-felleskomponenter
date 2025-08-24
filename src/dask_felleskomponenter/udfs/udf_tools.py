import uuid
import pandas as pd
from typing import Iterator
from osgeo import gdal, ogr

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, BinaryType


# ----- WKB GEOMETRY TYPE UDF -----

# WKB Geometry type lookup
WKB_GEOM_TYPES = {
    0: "Geometry",
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
    8: "CircularString",
    9: "CompoundCurve",
    10: "CurvePolygon",
    11: "MultiCurve",
    12: "MultiSurface",
    13: "Curve",
    14: "Surface",
    15: "PolyhedralSurface",
    16: "TIN",
    17: "Triangle",
    18: "Circle",
    19: "GeodesicString",
    20: "EllipticalCurve",
    21: "NurbsCurve",
    22: "Clothoid",
    23: "SpiralCurve",
    24: "CompoundSurface",
    102: "AffinePlacement",
    1025: "BrepSolid",
}


@udf(StringType())
def get_wkb_geom_type(wkb_value):
    """
    Extracts the geometry type from a WKB (Well-Known Binary) value.

    This UDF (User Defined Function) is intended for use with PySpark and returns a string
    representing the geometry type (e.g., 'Point', 'Polygon', etc.) from a WKB or EWKB value.
    It handles both hexadecimal string and bytes input, and accounts for dimensionality flags
    (e.g., Z, M, ZM) as well as invalid or malformed inputs.

    Args:
        wkb_value (str or bytes): The WKB or EWKB value as a hexadecimal string or bytes.

    Returns:
        str: The geometry type as a string, possibly suffixed with dimensionality (e.g., 'Point Z').
            Returns 'Invalid' or a specific error message for malformed inputs.
    """
    if wkb_value is None:
        return "Invalid"

    if isinstance(wkb_value, str):
        try:
            wkb_bytes = bytes.fromhex(wkb_value)
        except Exception:
            return "Invalid (not hex)"
    else:
        wkb_bytes = wkb_value

    if len(wkb_bytes) < 5:
        return "Invalid (too short)"

    byte_order = wkb_bytes[0]

    if byte_order == 0:
        geom_type_int = int.from_bytes(wkb_bytes[1:5], byteorder="big", signed=False)
    else:
        geom_type_int = int.from_bytes(wkb_bytes[1:5], byteorder="little", signed=False)

    # EWKB: Remove high bits (PostGIS style)
    # 0x20000000 → SRID flag
    # 0x80000000 → other flag
    base_geom_type_int = geom_type_int & 0xFFFF  # lower 16 bits → geometry type
    dimensional_flag = geom_type_int - base_geom_type_int

    # Dimensionality
    if dimensional_flag >= 3000:
        suffix = " ZM"
    elif dimensional_flag >= 2000:
        suffix = " M"
    elif dimensional_flag >= 1000:
        suffix = " Z"
    else:
        suffix = ""

    geom_type_str = (
        WKB_GEOM_TYPES.get(base_geom_type_int, f"Unknown({base_geom_type_int})")
        + suffix
    )
    return geom_type_str


# Register UDF
def register_get_wkb_geom_type_to_spark(spark: SparkSession):
    """
    Registers the 'get_wkb_geom_type' user-defined function (UDF) with the provided SparkSession.
    This makes the 'get_wkb_geom_type' UDF available as a SQL function in Spark SQL queries.

    Args:
        spark (SparkSession): The SparkSession instance to register the UDF with.
    """
    get_wkb_geom_type_sql_name = "get_wkb_geom_type"
    spark.udf.register(get_wkb_geom_type_sql_name, get_wkb_geom_type)
    print(f"Registered SQL function '{get_wkb_geom_type_sql_name}'")


# ----- GDAL COUNTOURS UDF -----


def generate_contours_wkb(
    raster_binary: bytes, interval: float = 10, base: float = 0
) -> bytes | None:
    """
    Generates contours from a binary raster object and returns a single
    MultiLineString geometry in Well-Known Binary (WKB) format.

    Args:
        raster_binary: The raw bytes of the raster file (e.g., GeoTIFF).
        interval: The interval between contour lines in the raster's vertical units.
        base: The base elevation for the contours.

    Returns:
        A byte string containing the WKB of a MultiLineString, or None if an error occurs.
    """
    if not raster_binary:
        return None

    # Use a unique name for the in-memory file to avoid collisions in a distributed environment
    in_mem_raster_path = f"/vsimem/{uuid.uuid4().hex}"
    gdal_ds = None
    ogr_ds = None

    try:
        # Write the raster bytes to GDAL's in-memory virtual filesystem
        gdal.FileFromMemBuffer(in_mem_raster_path, raster_binary)

        # Open the in-memory raster dataset
        gdal_ds = gdal.Open(in_mem_raster_path)
        if gdal_ds is None:
            # Failed to open
            return None
        band = gdal_ds.GetRasterBand(1)

        # Create an in-memory vector layer to store the contour results
        # This is a temporary container for the generated contour lines
        ogr_driver = ogr.GetDriverByName("Memory")
        ogr_ds = ogr_driver.CreateDataSource("temp_mem_ds")
        srs = gdal_ds.GetSpatialRef()  # Use the same spatial reference as the raster

        ogr_layer = ogr_ds.CreateLayer("contours", srs=srs, geom_type=ogr.wkbLineString)
        # We need to define at least one field for ContourGenerate to work
        ogr_layer.CreateField(ogr.FieldDefn("elevation", ogr.OFTReal))

        # Generate the contours
        # This populates the in-memory ogr_layer with LineString features
        gdal.ContourGenerate(
            band,  # Band srcBand
            interval,  # double contourInterval
            base,  # double contourBase
            [],  # int fixedLevelCount, requires list for some reason
            0,  # int useNoData
            0,  # double noDataValue
            ogr_layer,  # Layer dstLayer
            0,  # int idField
            0,  # int elevField
        )

        # Aggregate all generated LineStrings into a single MultiLineString
        if ogr_layer.GetFeatureCount() > 0:
            multi_line = ogr.Geometry(ogr.wkbMultiLineString)
            for feature in ogr_layer:
                geom = feature.GetGeometryRef()
                if geom:
                    # Add a clone of the geometry to the collection
                    multi_line.AddGeometry(geom.Clone())

            return multi_line.ExportToWkb()
        else:
            # No contours were generated
            return None

    except Exception as e:
        # On any error, return None to indicate failure for this row
        print(f"Error processing raster: {e}")
        return None

    finally:
        # Clean up GDAL objects and in-memory files to prevent memory leaks
        band = None
        gdal_ds = None
        ogr_ds = None
        # gdal.Unlink() removes the file from the virtual memory filesystem
        if gdal.VSIStatL(in_mem_raster_path):
            gdal.Unlink(in_mem_raster_path)


@pandas_udf(BinaryType())
def generate_contours_udf(
    raster_series: Iterator[pd.Series], contour_interval: float = 10, contour_base: float = 0
) -> Iterator[pd.Series]:
    """
    A Pandas UDF that generates contour lines from raster data.

    This function processes batches of raster data (as binary blobs), generating contour 
    lines at specified intervals and base levels.
    It uses GDAL for raster processing and outputs the contours as WKB (Well-Known Binary) 
    geometries.

    Parameters:
        raster_series (Iterator[pd.Series]): An iterator over pandas Series, each containing 
        raster data as binary objects.
        contour_interval (float): The interval between contour lines.
        contour_base (float): The base elevation for the contours.

    Yields:
        Iterator[pd.Series]: An iterator over pandas Series, each containing the generated 
        contour lines as binary WKB geometries.

    Raises:
        Any exceptions raised by GDAL during raster processing.
    """
    # Set GDAL to raise Python exceptions on errors, making them catchable
    gdal.UseExceptions()

    for batch in raster_series:
        # Use .apply() to run the function on each raster binary in the pandas Series
        results = batch.apply(
            lambda raster_binary: generate_contours_wkb(
                raster_binary, interval=contour_interval, base=contour_base
            )
        )
        yield results


# Register UDF
def register_generate_contours_udf(spark: SparkSession):
    """
    Registers the 'generate_contours_udf' user-defined function (UDF) with the provided SparkSession.
    This makes the 'generate_contours_udf' UDF available as a SQL function in Spark SQL queries.

    Args:
        spark (SparkSession): The SparkSession instance to register the UDF with.
    """
    generate_contours_udf_sql_name = "generate_contours_udf"
    spark.udf.register(generate_contours_udf_sql_name, generate_contours_udf)
    print(f"Registered SQL function '{generate_contours_udf_sql_name}'")


def register_all_udfs(spark: SparkSession):
    """
    Registers all user-defined functions (UDFs) in udf_tools with the provided SparkSession.

    Args:
        spark (SparkSession): The SparkSession instance to register the UDFs with.

    This function calls individual UDF registration functions to ensure that all necessary UDFs are available
    for use in Spark SQL queries and DataFrame operations.
    """
    register_get_wkb_geom_type_to_spark(spark)
    register_generate_contours_udf(spark)
