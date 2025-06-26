Of course. Here is a comprehensive README file for the `udfs` directory in your GitHub repository, written from the perspective of a data platform user. It's designed to be clear, welcoming, and to guide users and contributors effectively.

---

# Spark UDFs in `dask-felleskomponenter`

Welcome to the central library for shared Spark User-Defined Functions (UDFs) on the data platform.

This library provides a standardized, well-documented, and easy-to-use collection of functions to solve common data manipulation tasks. The goal is to reduce boilerplate code, ensure consistent logic across teams, and foster collaboration.

### Core Concepts

UDFs are organized into **domains** based on the type of data they operate on. For example:

*   `udfs.geometry`: Functions for handling geospatial data (WKB, WKT, etc.).
*   `udfs.strings`: Functions for advanced string parsing and manipulation. *(Example of a future domain)*
*   `udfs.datetime`: Functions for complex date and time operations. *(Example of a future domain)*

This structure makes it easy to find the function you need.

---

## Getting Started: Using the UDFs

Using the UDFs in your Databricks notebook is a simple, two-step process:
1.  **Discover** which UDFs are available.
2.  **Register** the ones you need for your Spark session.

### Step 1: Discover Available UDFs

You don't need to read the source code to see what's available. Simply use the `list_available()` function.

```python
import pprint
from udfs import list_available

# Get a dictionary of all UDFs, organized by domain
available_udfs = list_available()

# Use pprint for a clean, readable output
pprint.pprint(available_udfs)
```

**Example Output:**

```
{'geometry': {
    'curved_to_linear_wkb': 'Converts curved geometries (e.g., CircularString) in WKB format to linear approximations.',
    'parse_wkb_geom_type': "Parses a WKB/EWKB geometry and returns its type (e.g., 'Point', 'LineString ZM') as a string."
    }
}
```

This tells you the UDF's domain (`geometry`), its SQL name (`parse_wkb_geom_type`), and a description of what it does.

### Step 2: Register UDFs for Use

Once you know which UDFs you need, you can register them with Spark. You have three flexible options:

#### Option A: Register Everything (Easiest)

If you need functions from multiple domains, this is the simplest way to get started.

```python
from udfs import register_all

# This registers every single UDF in the library
register_all(spark)

# Now you can use them in SQL
df.createOrReplaceTempView("my_data")
spark.sql("""
    SELECT 
        parse_wkb_geom_type(wkb_column) as geom_type,
        curved_to_linear_wkb(wkb_column) as linear_wkb
    FROM my_data
""").show()
```

#### Option B: Register a Specific Domain

If your work is focused on a single area (e.g., geometry), you can register just that domain's UDFs to keep your Spark session clean.

```python
from udfs import geometry

# Register all UDFs from the geometry domain ONLY
geometry.register(spark)

# You can now use any geometry UDF in SQL
spark.sql("SELECT parse_wkb_geom_type(wkb_column) FROM ...").show()
```

#### Option C: Register a Single Function (Most Specific)

If you only need one specific function and want to avoid any potential conflicts, you can register it individually.

```python
from udfs.geometry import register_parse_wkb_geom_type_udf

# Register just this one UDF
register_parse_wkb_geom_type_udf(spark)

# This will work
spark.sql("SELECT parse_wkb_geom_type(wkb_column) FROM ...").show()

# This would fail (because it was not registered)
# spark.sql("SELECT curved_to_linear_wkb(wkb_column) FROM ...").show()
```

---

## How to Contribute New UDFs

We strongly encourage contributions! By adding your useful functions to this library, you help other teams and improve the platform for everyone. The process is designed to be as simple as possible.

You only need to write your function and add a single line to a configuration dictionary. The framework handles the rest.

### The 3-Step "Golden Path" to Contributing

**Scenario:** You want to add a new `get_wkb_srid` function to the `geometry` domain.

#### Step 1: Find or Create the Right File

Navigate to the UDF domain directory. Since your function **inspects** a geometry, the correct file is `udfs/geometry/inspections.py`.

*   If your function was a geometry **conversion**, it would go in `udfs/geometry/conversions.py`.
*   If you were adding a new string function, you might create a new file like `udfs/strings/parsing.py`.

#### Step 2: Write Your Pure Python Function

Add your function to the file. Make sure it's well-documented! The first line of your docstring is automatically used in the `list_available()` helper.

```python
# In file: udfs/geometry/inspections.py

from pyspark.sql.types import IntegerType # Don't forget to import the return type!

def get_wkb_srid(wkb_value: bytes) -> int:
    """
    Extracts the SRID from an EWKB geometry, returning -1 if not found.
    
    This function checks for the EWKB flag and parses the SRID value if present.
    It is safe to use on standard WKB, where it will return -1.
    """
    # Your brilliant function logic here...
    # ...
    return 4326 # or whatever you implement
```

#### Step 3: Add Your Function to the Registry

In the **same file**, find the `_UDF_REGISTRY` dictionary and add a new entry for your function.

```python
# In file: udfs/geometry/inspections.py

# ... your function definition above ...

# --- UDF REGISTRY ---
_UDF_REGISTRY = {
    # "sql_function_name": (python_function_object, return_type)
    "parse_wkb_geom_type": (parse_wkb_geom_type, StringType()),
    
    # Add your new line here:
    "get_wkb_srid": (get_wkb_srid, IntegerType())
}
```

### You're Done!

That's it. Once you commit your changes and your Pull Request is merged, the framework will automatically:
-   Make your UDF visible in `udfs.list_available()`.
-   Include it in `udfs.register_all(spark)`.
-   Include it in `udfs.geometry.register(spark)`.
-   Create a specific `udfs.geometry.register_get_wkb_srid_udf(spark)` function for it.

Thank you for helping us build a better data platform