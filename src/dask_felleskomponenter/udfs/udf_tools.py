from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BinaryType

from typing import Iterator
import pandas as pd

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
    1025: "BrepSolid"
}

@udf(StringType())
def get_wkb_geom_type(wkb_value):
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
        geom_type_int = int.from_bytes(wkb_bytes[1:5], byteorder='big', signed=False)
    else:
        geom_type_int = int.from_bytes(wkb_bytes[1:5], byteorder='little', signed=False)

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

    geom_type_str = WKB_GEOM_TYPES.get(base_geom_type_int, f"Unknown({base_geom_type_int})") + suffix
    return geom_type_str

# Register UDF
def register_curved_to_linear_wkb_to_udf(spark):
    get_wkb_geom_type_sql_name = "get_wkb_geom_type"
    spark.udf.register(get_wkb_geom_type_sql_name, get_wkb_geom_type)
    print(f"Registered SQL function '{get_wkb_geom_type_sql_name}'")


# ----- GDAL COUNTOURS: PYTHON FUNCTION -----
# This function processes a raster binary into MultiLineString WKB geometry.

def generate_contours_wkb(
    raster_binary: bytes, interval: float, base: float = 0.0
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
    from osgeo import gdal, ogr

    if not raster_binary:
        return None

    # Use a unique name for the in-memory file to avoid collisions in a distributed environment
    in_mem_raster_path = f"/vsimem/{uuid.uuid4().hex}"
    gdal_ds = None
    ogr_ds = None

    try:
        # 1. Write the raster bytes to GDAL's in-memory virtual filesystem
        gdal.FileFromMemBuffer(in_mem_raster_path, raster_binary)

        # 2. Open the in-memory raster dataset
        gdal_ds = gdal.Open(in_mem_raster_path)
        if gdal_ds is None:
            # Failed to open, maybe not a valid raster format
            return None
        band = gdal_ds.GetRasterBand(1)

        # 3. Create an in-memory vector layer to store the contour results
        # This is a temporary container for the generated contour lines
        ogr_driver = ogr.GetDriverByName("Memory")
        ogr_ds = ogr_driver.CreateDataSource("temp_mem_ds")
        srs = gdal_ds.GetSpatialRef()  # Use the same spatial reference as the raster

        ogr_layer = ogr_ds.CreateLayer("contours", srs=srs, geom_type=ogr.wkbLineString)
        # We need to define at least one field for ContourGenerate to work
        ogr_layer.CreateField(ogr.FieldDefn("elevation", ogr.OFTReal))

        # 4. Generate the contours
        # This populates the in-memory ogr_layer with LineString features
        gdal.ContourGenerate(
            band,  # Band srcBand
            interval,  # double contourInterval
            base,  # double contourBase
            [],  # int fixedLevelCount
            0,  # int useNoData
            0,  # double noDataValue
            ogr_layer,  # Layer dstLayer
            0,  # int idField
            0,  # int elevField
        )

        # 5. Aggregate all generated LineStrings into a single MultiLineString
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

    except Exception:
        # On any error, return None to indicate failure for this row
        # In a real pipeline, you might want to log the error: print(f"Error processing raster: {e}")
        return None

    finally:
        # 6. CRITICAL: Clean up GDAL objects and in-memory files to prevent memory leaks
        band = None
        gdal_ds = None
        ogr_ds = None
        # gdal.Unlink() removes the file from the virtual memory filesystem
        if gdal.VSIStatL(in_mem_raster_path):
            gdal.Unlink(in_mem_raster_path)


# ----- GDAL COUNTOURS: PYSPARK PANDAS UDF -----

@pandas_udf(BinaryType())
def generate_contours_udf(raster_series: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Pandas UDF to apply the contour generation function to a series of raster binaries.
    """
    # Set GDAL to raise Python exceptions on errors, making them catchable
    gdal.UseExceptions()

    # Define contour settings here. For dynamic values, see notes below.
    contour_interval = 5
    contour_base = 0.0

    for batch in raster_series:
        # Use .apply() to run the function on each raster binary in the pandas Series
        results = batch.apply(
            lambda raster_binary: generate_contours_wkb(
                raster_binary, interval=contour_interval, base=contour_base
            )
        )
        yield results


def register_all_udfs(spark):
    get_wkb_geom_type_udf(spark)

# Usage:
# from udf_tools import register_all_udfs
# register_all_udfs(spark)