# udfs/geometry/__init__.py

from .._base import _add_registration_functions_to_module
from .conversion import _UDF_REGISTRY as conversion_udfs
from .inspection import _UDF_REGISTRY as inspection_udfs

# Aggregate all UDFs from this domain into a single registry
_DOMAIN_UDF_REGISTRY = {
    **conversion_udfs,
    **inspection_udfs,
    # Add other module registries here: **other_module_udfs
}

# Automatically create `register()` and `register_<udf_name>()` functions
# This is done by passing our own module's globals() so the helper can add functions to it.
_add_registration_functions_to_module(globals(), _DOMAIN_UDF_REGISTRY, "geometry")