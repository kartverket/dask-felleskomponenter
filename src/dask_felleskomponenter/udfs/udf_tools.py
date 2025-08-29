import uuid
import pandas as pd
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

# EWKB bit flags for dimensionality and SRID
EWKB_Z_FLAG = 0x80000000
EWKB_M_FLAG = 0x40000000
EWKB_SRID_FLAG = 0x20000000
EWKB_FLAG_MASK = EWKB_Z_FLAG | EWKB_M_FLAG | EWKB_SRID_FLAG


@udf(StringType())
def get_wkb_geom_type(wkb_value):
    """
    Extracts the geometry type from a WKB or EWKB value, correctly handling
    hex string, bytes, and bytearray inputs from Spark.
    """
    # Unambiguously get the input into a `bytes` object.
    if wkb_value is None:
        return "Invalid (null input)"

    if isinstance(wkb_value, str):
        try:
            wkb_bytes = bytes.fromhex(wkb_value)
        except (ValueError, TypeError):
            return "Invalid (not hex)"
    # Accept both bytes and bytearray as valid binary types.
    elif isinstance(wkb_value, (bytes, bytearray)):
        wkb_bytes = wkb_value
    else:
        return f"Invalid (unsupported type: {type(wkb_value).__name__})"

    # Validate the bytes object.
    if len(wkb_bytes) < 5:
        return "Invalid (too short)"

    # Parse the validated bytes.
    byte_order = "big" if wkb_bytes[0] == 0 else "little"
    geom_type_int = int.from_bytes(wkb_bytes[1:5], byteorder=byte_order, signed=False)

    base_geom_type_int = geom_type_int
    suffix = ""

    # Check for EWKB flags (high-order bits).
    if (geom_type_int & EWKB_FLAG_MASK) != 0:
        has_z = (geom_type_int & EWKB_Z_FLAG) != 0
        has_m = (geom_type_int & EWKB_M_FLAG) != 0
        if has_z and has_m:
            suffix = " ZM"
        elif has_z:
            suffix = " Z"
        elif has_m:
            suffix = " M"
        base_geom_type_int &= ~EWKB_FLAG_MASK
    # Fallback to check for standard WKB dimensional ranges.
    else:
        if 3000 <= geom_type_int < 4000:
            suffix = " ZM"
            base_geom_type_int -= 3000
        elif 2000 <= geom_type_int < 3000:
            suffix = " M"
            base_geom_type_int -= 2000
        elif 1000 <= geom_type_int < 2000:
            suffix = " Z"
            base_geom_type_int -= 1000

    geom_type_str = WKB_GEOM_TYPES.get(
        base_geom_type_int, f"Unknown({base_geom_type_int})"
    )
    return geom_type_str + suffix


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
        # Set GDAL to raise Python exceptions on errors
        gdal.UseExceptions()

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
        print(f"Error generating contours: {e}")
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
    raster_series: pd.Series, interval_series: pd.Series, base_series: pd.Series
) -> pd.Series:
    """
    A Pandas UDF that generates contour lines from raster data.

    This is a Scalar Pandas UDF that processes batches of data.

    Parameters:
        raster_series (pd.Series): A pandas Series of raster data as binary objects.
        interval_series (pd.Series): A pandas Series of contour intervals (will be constant, use lit).
        base_series (pd.Series): A pandas Series of contour bases (will be constant, use lit).

    Returns:
        pd.Series: A pandas Series of generated contour lines as binary WKB geometries.
    """
    # Since interval and base are passed via lit(), every value in their series
    # for a given batch will be the same. We only need the first one.
    contour_interval = float(interval_series.iloc[0])
    contour_base = float(base_series.iloc[0])

    # Use .apply() to run the core logic function on each raster binary in the Series
    # We pass the dynamically received interval and base to it.
    return raster_series.apply(
        lambda raster_binary: generate_contours_wkb(
            raster_binary, interval=contour_interval, base=contour_base
        )
    )


# Register UDF
def register_generate_contours_udf(spark: SparkSession):
    """
    Registers the 'generate_contours_udf' user-defined function (UDF) with the provided SparkSession.
    This makes the UDF available as a SQL function in Spark SQL queries.

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
