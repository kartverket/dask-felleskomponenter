# DASK Felleskomponenter

This is a repo where we make available governance components, common functions and reusable UDFs. DASK felleskomponenter is still in an early stage of the development process.

You can find the PyPI package [here](https://pypi.org/project/dask-felleskomponenter/).

## Dependencies

You need to install Python 3.7 and higher, and to install the dependencies of this project, please execute the following
command

```bash
pip install -r requirements.txt
```

### Code formatting

The python code is validated against [Black](https://black.readthedocs.io/en/stable/) formatting in a Github Action. This means that your pull request will fail if the code isn't formatted according to Black standards. It is therefore suggested to enable automatic formatting using Black in your IDE.

## Bulding and publishing of package

### Publishing using GitHub Actions

Navigate to the [Publish to PyPI](https://github.com/kartverket/dask-modules/actions/workflows/pypi-publish.yml) workflow in GitHub Actions, choose the `main` branch and bump the version.
The workflow is authenticated through [Trusted Publisher](https://docs.pypi.org/trusted-publishers/). The workflow can push to either TestPyPI or PyPI depending on the given input. 

You can choose to not commit the changed version number to github. This is useful if you are doing testing to avoid cleaning up commits. 

One member of Team DASK needs to approve the workflow before it can publish to PyPI.

### Manual publishing to PyPI

To do manual publishing you will need to provide the user credentials of a user with publishing access to the dask-felleskomponenter package on PyPI.

1. Remove old dist-folder, from last time you published
2. Update version in `setup.py`, for instance `0.0.7`->`0.0.8`
3. (Run `pip install -r requirements.txt` if you haven't done that earlier)
4. Run `python3 -m build` (and wait some minutes...)
5. Verify that dist contains a package with the new version in the package name.
6. Run `python3 -m twine upload dist/*` to upload to PyPi

### Manual publishing to TestPyPI

To do manual publishing you will need to provide the user credentials of a user with publishing access to the dask-felleskomponenter package on TestPyPI.

To do a manual publish to TestPyPI do steps 1 through 5 for publishing to PyPI, and finish using the command ` python3 -m twine upload --repository testpypi dist/* `

## Run tests

Use the following command

```sh
coverage run -m unittest discover -s src/dask_felleskomponenter/tests
coverage report -m
```
