"""
A script to validate a datacontract against the defined Kartverket-spec and the existing data in Databricks.
"""

import os
import typer
from typing import Annotated
from databricks.sdk import WorkspaceClient
from datacontract.data_contract import DataContract
from datacontract.model.run import Run

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


def _validate_data_contracts(files: list[Path] | list[str]) -> list[Run]:
    """Validate the provided data contract files."""
    results = list[Run]()
    for file in files:
        if isinstance(file, Path):
            if file.suffix in [".yml", ".yaml"]:
                data_contract = DataContract(data_contract_file=str(file))
        elif isinstance(file, str):
            data_contract = DataContract(data_contract_str=file)
            results.append(data_contract.test())
    return results


def validate_data_contracts(files: list[Path]) -> list[Run]:
    _set_token()
    return _validate_data_contracts(files=files)


@app.command("validate-data-contract")
def cli(
    data_contract_paths: Annotated[
        list[Path], typer.Argument(help="Path to data contract files to validate.")
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
    for res in results:
        if not res.has_passed():
            print(res.pretty_logs())
    if any(not res.has_passed() for res in results):
        raise Exception("Some data contracts failed validation. See logs for details.")
