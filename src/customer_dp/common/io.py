"""Lecture et écriture des tables Parquet (et enregistrement Hive optionnel).

Les transformations ne lisent ni n'écrivent : seuls les points d'entrée (main.py) et le
notebook utilisent ces fonctions.
"""
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from customer_dp.common.config import AppConfig, TableConfig
from customer_dp.common.logging import get_logger

_log = get_logger("io")


def read_table(spark: SparkSession, table: TableConfig) -> DataFrame:
    """Lit une table Parquet par son chemin (la lecture ne dépend pas de Hive)."""
    return spark.read.parquet(table.path)


def write_table(
    df: DataFrame, table: TableConfig, config: Optional[AppConfig] = None, spark: Optional[SparkSession] = None
) -> None:
    """Écrit ``df`` en Parquet puis l'enregistre dans Hive si c'est activé.

    ``dynamic`` remplace uniquement les partitions présentes dans ``df`` : un rejeu ne crée
    pas de doublon et ne touche pas aux autres partitions. ``overwrite`` remplace la table.
    """
    mode = "dynamic" if table.write_mode == "dynamic" else "static"
    writer = df.write.mode("overwrite").format("parquet").option("partitionOverwriteMode", mode)
    if table.partition_by:
        writer = writer.partitionBy(*table.partition_by)
    writer.save(table.path)
    _log.info("Table écrite : %s -> %s (mode=%s)", table.name, table.path, table.write_mode)
    if config is not None and config.hive_enabled and spark is not None:
        register_hive_table(spark, config, table)


def register_hive_table(spark: SparkSession, config: AppConfig, table: TableConfig) -> None:
    """Déclare la table externe dans Hive et découvre ses partitions."""
    qualified = f"{config.database}.{table.name}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.database}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {qualified} USING parquet LOCATION '{table.path}'"
    )
    if table.partition_by:
        spark.sql(f"ALTER TABLE {qualified} RECOVER PARTITIONS")
    spark.sql(f"REFRESH TABLE {qualified}")
    _log.info("Table Hive enregistrée : %s", qualified)
