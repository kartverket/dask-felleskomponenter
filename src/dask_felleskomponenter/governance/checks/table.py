from typing import List, Optional
from .common import (
    MetadataError,
    check_codelist_value,
    TableMetadata,
    get_valid_codelist_values,
    check_codelist_value_local,
    get_valid_codelist_values_local,
    CodelistUrls,
    medaljongnivaa_kodeliste,
)
from .sikkerhetsnivaa_kodeliste import sikkerhetsnivaa_kodeliste
from .tilgangsnivaa_kodeliste import tilgangsnivaa_kodeliste
from .column import check_geometri_encoding


def _generate_metadata_error(
    catalog: str,
    schema: str,
    table: str,
    field: str,
    type: str,
    is_missing: bool,
    valid_values_description: Optional[str] = None,
    valid_values: str | List[str] = "string",
):
    error_reason = "mangler" if is_missing else "er ugyldig"
    description = (
        f"🔴 Feil: '{field}' {error_reason} i table properties. Type: <{type}>"
    )
    if valid_values_description != None:
        description += f" - {valid_values_description}"
    if field == "beskrivelse":
        solution = f"COMMENT ON TABLE {catalog}.{schema}.{table} IS '<<SETT_{field.upper()}_HER>>'"
    else:
        solution = f"ALTER TABLE {catalog}.{schema}.{table} SET TAGS ( '{field}' = '<<SETT_{field.upper()}_HER>>')"
    return MetadataError(
        catalog=catalog,
        schema=schema,
        table=table,
        column=None,
        description=description,
        solution=solution,
        for_field=field,
        valid_values=valid_values,
    )


def check_tittel(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    if not check_codelist_value(None, metadata.tittel):
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "tittel",
                "string",
                metadata.tittel == None,
            )
        )

    return context


def check_beskrivelse(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    if not check_codelist_value(None, metadata.beskrivelse):
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "beskrivelse",
                "string",
                metadata.beskrivelse == None,
            )
        )

    return context


def check_tilgangsnivaa(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    if not check_codelist_value_local(tilgangsnivaa_kodeliste, metadata.tilgangsnivaa):
        valid_values = get_valid_codelist_values_local(tilgangsnivaa_kodeliste)
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "tilgangsnivaa",
                "tilgangsrestriksjoner",
                metadata.tilgangsnivaa is None,
                f"gyldige verdier: {valid_values}",
                valid_values=valid_values,
            )
        )

    return context


def check_medaljongnivaa(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    valid_values = medaljongnivaa_kodeliste
    if not check_codelist_value(None, metadata.medaljongnivaa, valid_values):
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "medaljongnivaa",
                "valør",
                metadata.medaljongnivaa == None,
                f"gyldige verdier: {valid_values}",
                valid_values=valid_values,
            )
        )

    return context


def check_hovedkategori(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    kodeliste_url = CodelistUrls.hovedkategori

    if not check_codelist_value(kodeliste_url, metadata.hovedkategori):
        valid_values = get_valid_codelist_values(kodeliste_url)
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "hovedkategori",
                "tematisk-hovedkategori",
                metadata.hovedkategori == None,
                f"gyldige verdier: {valid_values}",
                valid_values=valid_values,
            )
        )

    return context


def check_emneord(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    if not check_codelist_value(None, metadata.emneord):
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "emneord",
                "string",
                metadata.emneord == None,
            )
        )

    return context


def check_sikkerhetsnivaa(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    if not check_codelist_value_local(
        sikkerhetsnivaa_kodeliste, metadata.sikkerhetsnivaa
    ):
        valid_values = get_valid_codelist_values_local(sikkerhetsnivaa_kodeliste)
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "sikkerhetsnivaa",
                "sikkerhetsniva",
                metadata.sikkerhetsnivaa is None,
                f"gyldige verdier: {valid_values}",
                valid_values=valid_values,
            )
        )

    return context


def check_begrep(
    metadata: TableMetadata, context: List[MetadataError]
) -> List[MetadataError]:
    kodeliste_url = CodelistUrls.begrep

    if not check_codelist_value(kodeliste_url, metadata.begrep):
        valid_values = get_valid_codelist_values(kodeliste_url)
        context.append(
            _generate_metadata_error(
                metadata.catalog,
                metadata.schema,
                metadata.table,
                "begrep",
                "nasjonal-temainndeling",
                metadata.begrep == None,
                f"gyldige verdier: {valid_values}",
                valid_values=valid_values,
            )
        )

    return context


checks_for_valor = {
    "bronze": [check_tittel, check_beskrivelse, check_sikkerhetsnivaa],
    "silver": [
        check_tittel,
        check_beskrivelse,
        check_emneord,
        check_begrep,
        check_sikkerhetsnivaa,
    ],
    "gold": [
        check_tittel,
        check_beskrivelse,
        check_hovedkategori,
        check_emneord,
        check_begrep,
        check_tilgangsnivaa,
        check_sikkerhetsnivaa,
        check_geometri_encoding,
    ],
}


def validate_table(metadata: TableMetadata) -> List[MetadataError]:
    validation_context = check_medaljongnivaa(metadata, [])

    if len(validation_context) > 0:
        return validation_context

    for check in checks_for_valor[metadata.medaljongnivaa]:
        validation_context = check(metadata, validation_context)

    return validation_context


def get_mandatory_metadata_for_medaljongnivaa(
    medaljongnivaa: str, column_properties: Optional[dict] = {}
) -> dict:
    metadata_dict = {}

    for check in checks_for_valor[medaljongnivaa]:
        check_output = check(TableMetadata(column_properties=column_properties), [])
        if len(check_output) == 0:
            continue
        metadata_error = check_output[0]
        metadata_dict[metadata_error.for_field] = metadata_error

    return metadata_dict
