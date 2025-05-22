import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

packages = setuptools.find_packages(where="src")

setuptools.setup(
    name="dask-felleskomponenter",
    version="0.1.8",
    author="Dataplattform@Statens Kartverk",
    author_email="dataplattform@kartverket.no",
    description="Felleskomponenter på DASK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kartverket/dask-modules/dask-felleskomponenter",
    project_urls={
        "Bug Tracker": "https://github.com/kartverket/dask-modules/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=packages,  # Use the variable here
    python_requires=">=3.7",
    install_requires=[
        "requests",
        "setuptools"
    ],
)
