from databricks.sdk.service.ml import Status
from open_data_contract_standard.model import OpenDataContractStandard
from datacontract.model.run import Check, ResultEnum

import uuid
import semver
from enum import StrEnum


class StatusEnum(StrEnum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


def check_id_uuid(data_contract: OpenDataContractStandard) -> Check:
    """Check that the id of the data contract is a valid UUID."""
    check = Check(
        type="id_as_uuid",
        name="Check that id is a valid UUID",
        field="id",
    )
    try:
        uuid.UUID(data_contract.id)
        check.result = ResultEnum.passed
    except ValueError:
        check.result = ResultEnum.failed
    return check


def check_version_semver(data_contract: OpenDataContractStandard) -> Check:
    """Check that the version of the data contract follows semantic versioning."""
    check = Check(
        type="version_semver",
        name="Check that version follows semantic versioning",
        field="version",
    )
    try:
        semver.VersionInfo.parse(data_contract.version)
        check.result = ResultEnum.passed
    except ValueError:
        check.result = ResultEnum.failed
    return check


def check_status_enum(data_contract: OpenDataContractStandard) -> Check:
    """Check that the status of the data contract is one of the allowed values."""
    check = Check(
        type="status_enum",
        name="Check that status is one of the allowed values",
        field="status",
    )
    try:
        StatusEnum(data_contract.status)
        check.result = ResultEnum.passed
    except ValueError:
        check.result = ResultEnum.failed
    return check
