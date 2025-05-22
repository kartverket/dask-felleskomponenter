import pyspark.sql.types as ps
import requests


class Json2Spark:
    TypeMapping = {
        "string": ps.StringType(),
        "decimal": ps.DecimalType(),
        "number": ps.DoubleType(),
        "float": ps.FloatType(),
        "integer": ps.LongType(),
        "boolean": ps.BooleanType(),
        "timestamp": ps.TimestampType(),
        "date": ps.DateType(),
    }

    def __init__(
        self,
        json_data,
        enforce_required_field=True,
        default_type="string",
        defs_location="$def",
        circular_references=None,
        external_ref_base_uri="",
    ):
        self.json = json_data
        self.enforce_required_field = enforce_required_field
        self.default_type = default_type
        self.defs_location = defs_location
        self.circular_references = circular_references or []
        self.external_ref_base_uri = external_ref_base_uri
        self.fetched_schemas = {}

    def nullable(self, field_name, required_fields):
        if required_fields is not None:
            return field_name not in required_fields and self.enforce_required_field
        return True

    def convert2spark(self):
        properties = self.json.get("properties", {})
        if properties:
            fields = []
            for field_name, field_value in properties.items():
                fields.extend(
                    self.property2struct(
                        field_value,
                        field_name,
                        f"#/properties/{field_name}",
                        self.required_fields(self.json),
                    )
                )
            return ps.StructType(fields)
        else:
            raise Exception("No properties found in JSON schema")

    def property2struct(self, node, field_name, path, required_fields=None):
        if "const" in node:
            return []
        elif "$ref" in node:
            return self.refs(node["$ref"], path, field_name)
        elif "enum" in node or self.is_circular_reference(node):
            return [
                ps.StructField(
                    field_name,
                    ps.StringType(),
                    self.nullable(field_name, required_fields),
                    self.metadata(path, node.get("description", "")),
                )
            ]
        elif "type" in node:
            if node["type"] in ["string", "number", "float", "integer", "boolean"]:
                return [
                    ps.StructField(
                        field_name,
                        self.TypeMapping.get(node["type"], ps.StringType()),
                        self.nullable(field_name, required_fields),
                        self.metadata(path, node.get("description", "")),
                    )
                ]
            elif node["type"] == "array":
                item_structs = self.property2struct(
                    node.get("items", {}),
                    "",
                    path + "/items",
                    self.required_fields(node.get("items", {})),
                )
                return [
                    ps.StructField(
                        field_name,
                        ps.ArrayType(
                            item_structs[0].dataType
                            if item_structs
                            else self.TypeMapping.get(
                                self.default_type, ps.StringType()
                            )
                        ),
                        self.nullable(field_name, required_fields),
                        self.metadata(path, node.get("description", "")),
                    )
                ]
            elif node["type"] == "object":
                nested_fields = []
                for k, v in node.get("properties", {}).items():
                    nested_fields.extend(
                        self.property2struct(
                            v, k, f"{path}/properties/{k}", self.required_fields(node)
                        )
                    )
                return [
                    ps.StructField(
                        field_name,
                        ps.StructType(nested_fields),
                        self.nullable(field_name, required_fields),
                        self.metadata(path, node.get("description", "")),
                    )
                ]
        return [
            ps.StructField(
                field_name,
                self.TypeMapping.get(self.default_type, ps.StringType()),
                self.nullable(field_name, required_fields),
                self.metadata(path, node.get("description", "")),
            )
        ]

    def required_fields(self, node):
        return node.get("required")

    def is_circular_reference(self, node):
        path = self.cursor_path(node)
        return path in self.circular_references

    def cursor_at(self, path):
        parts = path.lstrip("#/").split("/")
        cursor = self.json
        for part in parts:
            cursor = cursor.get(part, {})
        return cursor

    def cursor_path(self, node):
        return node.get("$ref", "#")

    def metadata(self, path, description=""):
        return {"path": path, "description": description}

    def fetch_external_schema(self, url):
        if url in self.fetched_schemas:
            return self.fetched_schemas[url]
        response = requests.get(url)
        if response.status_code == 200:
            schema = response.json()
            self.fetched_schemas[url] = schema
            return schema
        else:
            raise Exception(f"Unable to fetch external resource: {url}")

    def refs(self, resource_path, base_path, field_name):
        if resource_path.startswith("#"):
            cursor = self.cursor_at(resource_path)
            return self.property2struct(
                cursor,
                field_name,
                f"{base_path}/$ref/{resource_path}",
                self.required_fields(cursor),
            )
        else:
            if not resource_path.startswith("http"):
                resource_path = self.external_ref_base_uri + "/" + resource_path
            if "#" in resource_path:
                schema_url, fragment = resource_path.split("#", 1)
                fragment = fragment.lstrip("/")
            else:
                schema_url = resource_path
                fragment = ""
            external_json = self.fetch_external_schema(schema_url)
            if fragment:
                parts = fragment.split("/")
                for part in parts:
                    external_json = external_json.get(part, {})
            return self.property2struct(
                external_json, field_name, f"{base_path}/file/{resource_path}"
            )


def convert_json_schema_to_spark(
    json_data_dict: dict,
    enforce_required_field=True,
    default_type="string",
    defs_location="$def",
) -> ps.StructType:
    json2spark = Json2Spark(
        json_data=json_data_dict,
        enforce_required_field=enforce_required_field,
        default_type=default_type,
        defs_location=defs_location,
    )

    return json2spark.convert2spark()
