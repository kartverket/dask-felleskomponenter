"""
A script to validate a datacontract against the defined Kartverket-spec and the existing data in Databricks.
"""

from open_data_contract_standard.model import OpenDataContractStandard

import os
import typer
from typing import Annotated
from databricks.sdk import WorkspaceClient
from datacontract.data_contract import DataContract
from datacontract.model.run import Run, Check, Log
from dask_felleskomponenter.governance.data_contract.checks import (
    check_id_uuid,
    check_status_enum,
    check_version_semver,
)

from pathlib import Path

app = typer.Typer(add_completion=False)


def _set_token(token: str | None = None, service_account: str | None = None) -> None:
    """Set the DATABRICKS_TOKEN environment variable based on the provided service account."""
    if service_account:
        ws_client = WorkspaceClient(
            host=os.environ["DATACONTRACT_DATABRICKS_SERVER_HOSTNAME"],
            google_service_account=service_account,
        )
        token = str(ws_client.api_client._cfg.oauth_token())
    assert token is not None
    os.environ["DATACONTRACT_DATABRICKS_TOKEN"] = token


def _create_datacontract_object(file: Path | str) -> DataContract:
    """Create a DataContract object from the provided file."""
    if isinstance(file, Path):
        if file.suffix in [".yml", ".yaml"]:
            return DataContract(data_contract_file=str(file))
    elif isinstance(file, str):
        return DataContract(data_contract_str=file)
    raise ValueError(
        f"Unsupported file type for {file}. Only .yml and .yaml are supported."
    )


def _validate_kartverket_spec(data_contract: OpenDataContractStandard) -> list[Check]:
    """Validate the data contract against the Kartverket spec."""
    checks = list[Check]()
    checks.append(check_id_uuid(data_contract))
    checks.append(check_status_enum(data_contract))
    checks.append(check_version_semver(data_contract))
    return checks


def _pretty_print_checks(checks: list[Check]) -> str:
    """Pretty print the provided checks."""
    return "\n".join(f"{check.type} - {check.name}: {check.result}" for check in checks)


def _validate_data_contracts(files: list[Path] | list[str]) -> list[Run]:
    """Validate the provided data contract files."""
    results = list[Run]()
    for file in files:
        data_contract = _create_datacontract_object(file)
        run = data_contract.test()
        kartverket_checks = _validate_kartverket_spec(
            data_contract=data_contract.get_data_contract()
        )
        if run.checks:
            run.checks.extend(kartverket_checks)
        else:
            run.checks = kartverket_checks
        results.append(run)
    return results


def validate_data_contracts(files: list[Path]) -> list[Run]:
    _set_token()
    return _validate_data_contracts(files=files)


@app.command("validate-data-contract")
def cli(
    data_contract_paths: Annotated[
        list[Path],
        typer.Argument(
            help="Path to data contract files to validate. A list of paths separated by whitespace. Supports both .yml and .yaml files."
        ),
    ],
    databricks_token: Annotated[
        str | None,
        typer.Option(
            "-t",
            "--token",
            help="Personal token to be used for data contract validation. Needs SQL scope.",
        ),
    ] = None,
    databricks_service_account: Annotated[
        str | None,
        typer.Option(
            "-sa",
            "--service-account",
            help="Service account to be used for data contract validation. You need to be already authenticated as this service account using the gcloud cli.",
        ),
    ] = None,
) -> None:
    """Validate data contracts against the Kartverket spec and Databricks data."""
    _set_token(token=databricks_token, service_account=databricks_service_account)
    results = _validate_data_contracts(files=data_contract_paths)
    for run in results:
        typer.echo(
            f"Results for data contract {run.dataContractId} version {run.dataContractVersion}:"
        )
        typer.echo("Checks:")
        typer.echo(_pretty_print_checks([check for check in run.checks]))
    if any(not res.has_passed() for res in results):
        raise Exception("Some data contracts failed validation. See logs for details.")
