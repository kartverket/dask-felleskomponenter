# udfs/_base.py
# PRIVATE - For internal use by the UDF framework only.

import logging
from pyspark.sql.functions import udf

logger = logging.getLogger(__name__)

def _add_registration_functions_to_module(module_globals, udf_registry, module_name):
    """
    Dynamically creates and adds registration functions to a module's namespace.

    For each UDF in the registry, it creates:
    1. A `register_<udf_name>(spark)` function for individual registration.
    2. It adds the function name to a list for a collective `register(spark)` function.

    Args:
        module_globals (dict): The `globals()` dict of the calling module (__init__.py).
        udf_registry (dict): The dictionary of UDFs to register.
        module_name (str): The name of the domain (e.g., 'geometry').
    """
    
    all_udf_registrars = []

    for sql_name, (func, return_type) in udf_registry.items():
        # Define the registrar function for a single UDF
        def create_registrar(sql_name_closure, func_closure, return_type_closure):
            def registrar(spark):
                """Registers the individual UDF."""
                try:
                    udf_obj = udf(func_closure, return_type_closure)
                    spark.udf.register(sql_name_closure, udf_obj)
                    logger.info(f"Registered SQL UDF '{sql_name_closure}' from domain '{module_name}'.")
                except Exception as e:
                    logger.error(f"Failed to register UDF '{sql_name_closure}': {e}", exc_info=True)
            
            # Set a meaningful name for the dynamic function
            registrar.__name__ = f"register_{sql_name_closure}_udf"
            registrar.__doc__ = f"Registers the '{sql_name_closure}' UDF with the SparkSession."
            return registrar

        # Create and add the specific registrar to the module's global scope
        registrar_func = create_registrar(sql_name, func, return_type)
        module_globals[registrar_func.__name__] = registrar_func
        all_udf_registrars.append(registrar_func)

    # Define the collective 'register' function for the entire domain
    def register_all_domain_udfs(spark):
        """Registers all UDFs within the '{module_name}' domain."""
        logger.info(f"Registering all UDFs from domain: '{module_name}'...")
        for registrar_func in all_udf_registrars:
            registrar_func(spark)
        logger.info(f"Finished registering UDFs from domain: '{module_name}'.")

    module_globals['register'] = register_all_domain_udfs
    