# udfs/__init__.py
# This is the public API for the UDFs library.

import logging
from . import geometry  # Import the entire domain package
# from . import strings # Future: Import other domains

logger = logging.getLogger("dask_felleskomponenter.udfs")

# Define which domain packages are part of the library
_DOMAINS = [
    geometry,
    # strings, # Future
]

def register_all(spark):
    """
    Registers all available UDFs from all domains with the SparkSession.
    This is the simplest way to get started.
    """
    logger.info("Registering all UDFs from 'dask-felleskomponenter'...")
    for domain in _DOMAINS:
        domain.register(spark) # Each domain has its own 'register' function
    logger.info("All UDFs registered successfully.")


def list_available() -> dict:
    """
    Lists all available UDFs, their domains, and their descriptions.
    Does not require a SparkSession and does not register anything.

    Returns:
        A dictionary describing the available UDFs, structured by domain.
        Example:
        {
            "geometry": {
                "curved_to_linear_wkb": "Converts curved geometries...",
                "get_wkb_geom_type": "Get WKB/EWKB geometry type..."
            }
        }
    """
    udf_map = {}
    for domain in _DOMAINS:
        domain_name = domain.__name__.split('.')[-1]
        # Access the registry created in the domain's __init__.py
        if hasattr(domain, '_DOMAIN_UDF_REGISTRY'):
            udf_map[domain_name] = {
                sql_name: func.__doc__.strip().split('\n')[0] # Get the first line of the docstring
                for sql_name, (func, _) in domain._DOMAIN_UDF_REGISTRY.items()
            }
    return udf_map

# Expose the main functions and the domain modules directly
__all__ = ["register_all", "list_available", "geometry"]