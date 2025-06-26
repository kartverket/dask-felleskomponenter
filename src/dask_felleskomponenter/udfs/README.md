# Spark UDFs in `dask-felleskomponenter`

Welcome to the central library for shared Spark User-Defined Functions (UDFs) on the data platform.

This library provides a collection of functions to solve common data manipulation tasks.
The goal is to reduce boilerplate code, ensure consistent logic across teams, and foster collaboration.

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
    'get_wkb_geom_type': "Return WKB/EWKB type from bytes (e.g., 'Point', 'LineString ZM') as a string."
    }
}
```

This tells you the UDF's domain (`geometry`), its SQL name (`get_wkb_geom_type`), and a description of what it does.

### Step 2: Register UDFs for Use

Once you know which UDFs you need, you can register them with Spark. You have three options:

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
        get_wkb_geom_type(wkb_column) as geom_type,
        curved_to_linear_wkb(wkb_column) as linear_wkb
    FROM my_data
""").show()
```

#### Option B: Register a Specific Domain

If your work is focused on a single area (e.g., geometry), you can register that domain's UDFs to keep your Spark session clean.

```python
from udfs import geometry

# Register all UDFs from the geometry domain ONLY
geometry.register(spark)

# You can now use any geometry UDF in SQL
spark.sql("SELECT get_wkb_geom_type(wkb_column) FROM ...").show()
```

#### Option C: Register a Single Function (Most Specific)

If you only need one specific function and want to avoid any potential conflicts, you can register it individually.

```python
from udfs.geometry import register_get_wkb_geom_type_udf

# Register just this one UDF
register_get_wkb_geom_type_udf(spark)

# This will work
spark.sql("SELECT get_wkb_geom_type(wkb_column) FROM ...").show()

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

Navigate to the UDF domain directory. Since your function **inspects** a geometry, the correct file is `udfs/geometry/inspection.py`.

*   If your function was a geometry **conversion**, it would go in `udfs/geometry/conversion.py`.
*   If you were adding a new string function, you might create a new file like `udfs/strings/parsing.py`.

#### Step 2: Write Your Python Function

Add your function to the file. Make sure it's well-documented! The first line of your docstring is automatically used in the `list_available()` helper.

```python
# In file: udfs/geometry/inspection.py

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
# In file: udfs/geometry/inspection.py

# ... your function definition above ...

# --- UDF REGISTRY ---
_UDF_REGISTRY = {
    # "sql_function_name": (python_function_object, return_type)
    "get_wkb_geom_type": (get_wkb_geom_type, StringType()),
    
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

Thank you for helping us build a better data platform!

---


### How to Add a New UDF Domain (e.g., `strings`, `datetime`)

If your new UDF doesn't fit into existing domains like `geometry`, you should create a new one. This keeps the library organized and easy to navigate for everyone.

This guide will walk you through the process of creating a brand new `strings` domain.

There are four simple steps to add and integrate a new domain.

---

#### **Step 1: Create the Directory Structure**

First, create a new directory for your domain inside the `udfs/` folder. Inside that, create at least one Python file for your UDF implementations. Good names for these files describe the function's purpose (e.g., `parsing.py`, `transformations.py`, `validations.py`).

For our `strings` domain, we'll start with a `parsing.py` file.

```sh
udfs/
└───strings/
    ├── __init__.py      # We will create this in Step 3
    └───parsing.py       # Create this file now
```

---

#### **Step 2: Add Your Functions to the Implementation File**

Now, open your new `udfs/strings/parsing.py` file. Add your Python functions and register them in the `_UDF_REGISTRY` dictionary, just like you would when contributing to an existing domain.

**File: `udfs/strings/parsing.py`**
```python
from pyspark.sql.types import StringType

def to_title_case(input_str: str) -> str:
    """
    Converts a string to title case, handling None values safely.
    Example: 'hello world' -> 'Hello World'.
    """
    if input_str is None:
        return None
    return input_str.title()

# --- UDF REGISTRY ---
# A developer adds their new UDF function here.
_UDF_REGISTRY = {
    # "sql_function_name": (python_function_object, return_type)
    "to_title_case": (to_title_case, StringType()),
}
```

---

#### **Step 3: Create the Domain's `__init__.py`**

This is the control center for your new domain. It gathers all the UDFs from the implementation files (like `parsing.py`) and uses the framework's helper to automatically generate the necessary `register()` functions.

Create a new `__init__.py` file inside your `udfs/strings/` directory. The content is mostly boilerplate—you can copy and adapt this template.

**File: `udfs/strings/__init__.py`**
```python
# Import the framework's helper. The '..' means go up one directory.
from .._base import _add_registration_functions_to_module

# Import the registry from your implementation file(s).
from .parsing import _UDF_REGISTRY as parsing_udfs

# Aggregate all UDFs from this domain into a single registry.
# If you add a 'transformations.py' later, you would add its registry here.
_DOMAIN_UDF_REGISTRY = {
    **parsing_udfs,
}

# This magic line automatically creates the 'register()' function for the 'strings'
# domain, as well as individual 'register_to_title_case_udf()' functions.
_add_registration_functions_to_module(globals(), _DOMAIN_UDF_REGISTRY, "strings")
```

---

#### **Step 4: Connect Your New Domain to the Main Library**

This final step makes your new domain visible to the rest of the library, so it appears in `list_available()` and `register_all()`.

Open the top-level `udfs/__init__.py` file and make two small additions:

1.  Import your new domain module.
2.  Add it to the `_DOMAINS` list.

**File: `udfs/__init__.py`**
```python
# ... (imports and logger setup) ...

# 1. IMPORT YOUR NEW DOMAIN
from . import geometry
from . import strings  # <-- ADD THIS LINE

# ...

# Define which domain packages are part of the library
_DOMAINS = [
    geometry,
    strings,         # <-- 2. ADD YOUR DOMAIN TO THIS LIST
]

# ... (rest of the file is unchanged) ...
```

### You're Done!

That's all it takes. After completing these steps, your new `strings` domain is a first-class citizen of the UDF library. It is now:

*   **Discoverable:** It will appear in the output of `udfs.list_available()`.
*   **Fully Registrable:** It will be included when a user calls `udfs.register_all(spark)`.
*   **Individually Registrable:** Users can now call `udfs.strings.register(spark)` to get only your new string functions.

This structured approach ensures that the library remains clean, scalable, and easy to use for everyone.