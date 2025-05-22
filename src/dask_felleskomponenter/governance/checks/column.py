from typing import List
from .common import MetadataError, TableMetadata
from .geometri_encoding_kodeliste import geometri_encoding_kodeliste

valid_geometri_encoding = [
    val["codevalue"].lower() for val in geometri_encoding_kodeliste["containeditems"]
]


def check_geometri_encoding(
    metadata: TableMetadata, context: List
) -> List[MetadataError]:
    if metadata.column_properties is None:
        return context

    for key, val in metadata.column_properties.items():
        epsg = val.get("epsg", None)
        geometry_encoding = val.get("geometri_encoding", "")

        if epsg is None:
            continue

        if geometry_encoding.lower() not in valid_geometri_encoding:
            error_obj = MetadataError(
                catalog=metadata.catalog,
                schema=metadata.schema,
                table=metadata.table,
                column=key,
                for_field="geometri_encoding",
                valid_values=valid_geometri_encoding,
                description="🔴 Feil: 'geometri_encoding' mangler i column properties. Type: <geometri_encoding> - gyldige verdier er WKT, WKB, GeoJson eller S2cell ",
                solution=f"ALTER TABLE {metadata.catalog}.{metadata.schema}.{metadata.table} SET TBLPROPERTIES ( 'columns.{key}.geometri_encoding' = '<<SETT_ROMLIG_REPRESENTASJONSTYPE_HER>>')",
            )
            context.append(error_obj)

    return context
