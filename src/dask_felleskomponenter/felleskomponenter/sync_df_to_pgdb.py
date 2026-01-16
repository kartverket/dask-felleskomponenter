import os
import psycopg
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.dbutils import DBUtils


@dataclass
class PostgresTargetConfig:
    """
    Configuration contract for PostgresSyncManager.

    NOTE: SSL Certificates are NOT configured here. They must be provisioned
    via cluster init scripts and exposed via environment variables:
    - ca
    - cert
    - key
    """

    host: str
    dbname: str
    user: str
    password: str
    staging_table: str
    target_table: str
    update_type_col: str = "update_type"
    srid: int = 0

    def __post_init__(self):
        self._validate_environment()

    def _validate_environment(self) -> None:
        """
        Validates that the runtime environment has the necessary SSL configuration.
        Raises RuntimeError if env vars are missing or certificate files are not found.
        """
        required_env_vars = ["ca", "cert", "key"]
        missing_vars = [var for var in required_env_vars if var not in os.environ]

        if missing_vars:
            raise RuntimeError(
                f"Missing required environment variables for making SSL connection to host {self.host}:"
                f"\t{', '.join(missing_vars)}."
                "Ensure environment variables are set on the cluster for init script to run successfully."
            )

        required_init_script_setup_envs = [
            "CLOUD_SQL_CA",
            "CLOUD_SQL_CERT",
            "CLOUD_SQL_KEY",
        ]

        for var in required_init_script_setup_envs:
            path = os.environ[var]
            if not os.path.exists(path):
                raise FileNotFoundError(
                    f"Certificate file defined in {var} not found at path: {path}. Check init script execution."
                )

        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        security_mode = spark.conf.get(
            "spark.databricks.clusterUsageTags.dataSecurityMode", "UNKNOWN"
        ).upper()
        if security_mode == "USER_ISOLATION":
            raise RuntimeError(
                "CRITICAL: This code requires 'Single User' or 'No Isolation Shared' mode. "
                "Shared clusters (User Isolation) restrict access to local file paths required for SSL certs."
            )


class PostgresSyncManager:
    """
    PostgresSyncManager for synchronizing Spark DataFrames to PostgreSQL table.

    Handles SSL connections, staging table writes, and final target synchronization
    (Snapshot or Merge). Requires a Databricks cluster with 'Single User' or
    'No Isolation Shared' security mode to access SSL certificates.
    """

    def __init__(
        self, config: PostgresTargetConfig, spark: Optional[SparkSession] = None
    ):
        self.config = config

        self.spark = spark or SparkSession.builder.getOrCreate()

        self.certs = {
            "ca": os.environ["CLOUD_SQL_CA"],
            "cert": os.environ["CLOUD_SQL_CERT"],
            "key": os.environ["CLOUD_SQL_KEY"],
        }

        self.jdbc_url = f"jdbc:postgresql://{self.config.host}/{self.config.dbname}"

    @classmethod
    def from_databricks_secrets(
        cls,
        scope: str,
        host_key: str,
        password_key: str,
        dbname: str,
        user: str,
        staging_table: str,
        target_table: str,
        **kwargs,
    ) -> "PostgresSyncManager":
        """
        Factory method to initialize the manager using credentials stored in Databricks Secrets.

        Args:
            scope: The Databricks secret scope name.
            host_key: Key in the scope containing the DB host/IP.
            password_key: Key in the scope containing the DB password.
            dbname: Target database name.
            user: Database username.
            staging_table: Full table path for staging (e.g., "schema.table_staging").
            target_table: Full table path for target (e.g., "schema.table_target").
            **kwargs: Additional args for PostgresTargetConfig (e.g., srid, update_type_col).

        Example Usage:
            manager = PostgresSyncManager.from_databricks_secrets(
                scope="my-project-secrets",
                host_key="db-host",
                password_key="db-pass",
                dbname="geodata",
                user="etl_user",
                staging_table="public.buildings_staging",
                target_table="public.buildings"
            )
        """

        spark = SparkSession.builder.getOrCreate()
        try:
            dbutils = DBUtils(spark)
        except ImportError:
            raise RuntimeError(
                "DBUtils not available. Must be run within Databricks Runtime."
            )

        host = dbutils.secrets.get(scope=scope, key=host_key)
        password = dbutils.secrets.get(scope=scope, key=password_key)

        config = PostgresTargetConfig(
            host=host,
            dbname=dbname,
            user=user,
            password=password,
            staging_table=staging_table,
            target_table=target_table,
            **kwargs,
        )
        return cls(config, spark)

    def _run_sql_command(self, sql: str) -> int:

        with psycopg.connect(
            host=self.config.host,
            dbname=self.config.dbname,
            user=self.config.user,
            password=self.config.password,
            sslmode="verify-ca",
            sslrootcert=self.certs["ca"],
            sslcert=self.certs["cert"],
            sslkey=self.certs["key"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.rowcount

    def _write_to_staging(self, df: DataFrame):
        print(
            f"Data Transfer: Writing {df.count()} rows to {self.config.staging_table}"
        )

        jdbc_props = {
            "user": self.config.user,
            "password": self.config.password,
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": "verify-ca",
            "sslrootcert": self.certs["ca"],
            "sslcert": self.certs["cert"],
            "sslkey": self.certs["key"],
        }

        (
            df.write.format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", self.config.staging_table)
            .options(**jdbc_props)
            .mode("overwrite")
            .option("truncate", "true")
            .save()
        )

    def _fmt_col(self, col_name: str, table_name: str, geometry_cols: List[str]) -> str:
        col_ref = f'{table_name}."{col_name}"'
        if geometry_cols and col_name in geometry_cols:
            return f"ST_GeomFromWKB({col_ref}, {self.config.srid})"
        return col_ref

    def _execute_snapshot(self, cols: List[str], geom_cols: List[str]):
        print(f"Executing SNAPSHOT OVERWRITE on {self.config.target_table}")

        cols_list = ", ".join([f'"{c}"' for c in cols])
        sel_list = ", ".join(
            [self._fmt_col(c, self.config.staging_table, geom_cols) for c in cols]
        )

        sql = f"""
        BEGIN;
        TRUNCATE TABLE {self.config.target_table};
        INSERT INTO {self.config.target_table} ({cols_list})
        SELECT {sel_list} FROM {self.config.staging_table};
        COMMIT;
        """
        self._run_sql_command(sql)
        print("Snapshot Overwrite Complete.")

    def _execute_merge(self, cols: List[str], keys: List[str], geom_cols: List[str]):
        print(f"Executing MERGE on {self.config.target_table}")

        tgt, stg = self.config.target_table, self.config.staging_table
        ut_col = self.config.update_type_col

        join_condition = " AND ".join([f"{tgt}.{k} = {stg}.{k}" for k in keys])

        update_cols = [c for c in cols if c not in keys and c != ut_col]
        update_set = ", ".join(
            [f'"{c}" = {self._fmt_col(c, stg, geom_cols)}' for c in update_cols]
        )

        insert_cols = [c for c in cols if c != ut_col]
        insert_names = ", ".join([f'"{c}"' for c in insert_cols])
        insert_vals = ", ".join([self._fmt_col(c, stg, geom_cols) for c in insert_cols])

        sql = f"""
        MERGE INTO {tgt}
        USING {stg}
        ON {join_condition}
        WHEN MATCHED AND {stg}.{ut_col} = 'delete' THEN
            DELETE
        WHEN MATCHED AND {stg}.{ut_col} != 'delete' THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED AND {stg}.{ut_col} != 'delete' THEN
            INSERT ({insert_names}) VALUES ({insert_vals});
        """

        rows = self._run_sql_command(sql)
        print(f"Merge Complete. Rows affected: {rows}")

    def sync(
        self,
        df: DataFrame,
        mode: str = "snapshot",
        merge_keys: Optional[List[str]] = None,
        geometry_cols: Optional[List[str]] = None,
    ):
        """
        Synchronizes a Spark DataFrame to PostgreSQL.

        Args:
            df: The Spark DataFrame to sync.
            mode: 'snapshot' (truncate & insert) or 'merge' (upsert/delete).
            merge_keys: List of primary key columns. Required if mode='merge'.
            geometry_cols: List of columns containing WKB geometry to be converted.

        Example:
            # Snapshot (Full Overwrite)
            manager.sync(df, mode="snapshot")

            # Merge (Upsert based on keys)
            manager.sync(df, mode="merge", merge_keys=["id"], geometry_cols=["geom"])
        """
        if mode == "merge" and not merge_keys:
            raise ValueError("Argument 'merge_keys' is required when mode='merge'.")

        geometry_cols = geometry_cols or []

        self._write_to_staging(df)

        if mode == "snapshot":
            self._execute_snapshot(df.columns, geometry_cols)
        elif mode == "merge":
            self._execute_merge(df.columns, merge_keys, geometry_cols)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        print("Sync Process Finished.")
