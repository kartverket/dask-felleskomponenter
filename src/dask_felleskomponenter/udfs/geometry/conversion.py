# udfs/geometry/conversion.py

from osgeo import ogr
from pyspark.sql.types import BinaryType
from typing import Optional


def curved_to_linear_wkb(
    geometry_wkb: bytes, dfMaxAngleStepSizeDegrees: float = 0.0
) -> Optional[bytes]:
    """
    Converts curved geometries (e.g., CircularString) in WKB format to linear approximations.

    Args:
        geometry_wkb: The input geometry as WKB bytes.
        dfMaxAngleStepSizeDegrees: The maximum angle step size in degrees for linearization.

    Returns:
        The linearized geometry as WKB bytes, or None on failure.
    """
    if geometry_wkb is None:
        return None
    try:
        geom = ogr.CreateGeometryFromWkb(geometry_wkb)
        if geom is None:
            return None
        linear_geom = geom.GetLinearGeometry(dfMaxAngleStepSizeDegrees)
        return linear_geom.ExportToWkb()
    except Exception:
        return None


# --- UDF REGISTRY ---
# Remember to add new UDF functions here!
_UDF_REGISTRY = {
    # "sql_function_name": (python_function_object, return_type)
    "curved_to_linear_wkb": (curved_to_linear_wkb, BinaryType()),
}
