# DASK Felleskomponenter

This is a repo where we make available governance components, common functions and reusable UDFs. DASK felleskomponenter is still in an early stage of the development process.

You can find the PyPI package [here](https://pypi.org/project/dask-felleskomponenter/).

## Dependencies

You need to install Python3.7 and higher, and to install the dependencies of this project, please execute the following
command

```bash
pip install -r requirements.txt
```

## Bulding and publishing of package

### Steps for publishing using GitHub Actions

Navigate to the [Publish to PyPI](https://github.com/kartverket/dask-modules/actions/workflows/pypi-publish.yml) workflow in GitHub Actions, choose the `main` branch and bump the version.

One member of Team DASK needs to approve the workflow run before it starts.

### Steps for manual publishing

- Remove old dist-folder, from last time you published
- Update version in `setup.py`, for instance `0.0.7`->`0.0.8`
- Add change info to CHANGES.txt
- (Run `pip install -r requirements.txt` if you haven't done that earlier)
- Run `python3 -m build` (and wait some minutes...)
- Verity that dist contains a package with the new version in the package name.
- Run `python3 -m twine upload dist/*` to upload to pypi

### To upload to PyPi test

Replace the last command with `python3 -m twine upload --repository testpypi dist/*`

## Run tests

Use the following command

```sh
coverage run -m unittest discover -s src/dask_felleskomponenter/tests
coverage report -m
```
