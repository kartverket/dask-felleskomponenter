# DASK Felleskomponenter

This is a repo where we make available governance components, common functions and reusable UDFs. DASK felleskomponenter is still in an early stage of the development process.

You can find the PyPI package [here](https://pypi.org/project/dask-felleskomponenter/).

## Dependencies

You need to install Python 3.10 and higher, and the project uses poetry as the package-manager. To download poetry check out their [documentation](https://python-poetry.org/docs/#installing-with-the-official-installer).


### Code formatting

The python code is validated against [Black](https://black.readthedocs.io/en/stable/) formatting in a Github Action. This means that your pull request will fail if the code isn't formatted according to Black standards. It is therefore suggested to enable automatic formatting using Black in your IDE.

## Bulding and publishing of package

### Publishing using GitHub Actions

Navigate to the [Publish to PyPI](https://github.com/kartverket/dask-modules/actions/workflows/pypi-publish.yml) workflow in GitHub Actions, choose the `main` branch and bump the version.
The workflow is authenticated through [Trusted Publisher](https://docs.pypi.org/trusted-publishers/). The workflow can push to either TestPyPI or PyPI depending on the given input. 

You can choose to not commit the changed version number to github. This is useful if you are doing testing to avoid cleaning up commits. 

One member of Team DASK needs to approve the workflow before it can publish to PyPI.

## Run tests

Use the following command

```sh
coverage run -m unittest discover -s src/dask_felleskomponenter/tests
coverage report -m
```
