# udfs/geometry/inspection.py

from pyspark.sql.types import StringType

from utils import WKB_GEOM_TYPES


def get_wkb_geom_type(wkb_value):
    if wkb_value is None:
        return None

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


# --- UDF REGISTRY ---
# Remember to add new UDF functions here!
_UDF_REGISTRY = {
    # "sql_function_name": (python_function_object, return_type)
    "parse_wkb_geom_type": (get_wkb_geom_type, StringType()),
}
