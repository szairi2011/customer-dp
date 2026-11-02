"""Création ou récupération de la SparkSession partagée (jobs, notebook, tests)."""
from typing import Optional

from pyspark.sql import SparkSession

from customer_dp.common.config import AppConfig


def get_spark(config: AppConfig, app_name: str, master: Optional[str] = None) -> SparkSession:
    """Retourne la session active ou en crée une.

    Sous ``spark-submit`` le master est fourni par la commande : il n'est forcé que s'il
    est configuré (mode local, notebook). La session active est réutilisée telle quelle.
    """
    builder = SparkSession.builder.appName(app_name)
    chosen = master or config.spark_master
    if chosen:
        builder = builder.master(chosen)
    # Réécriture contrôlée : seules les partitions présentes dans le DataFrame sont remplacées.
    builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Le spark-defaults de CDP impose PathOutputCommitProtocol (committer cloud) qui ignore le
    # remplacement dynamique de partitions : un rejeu AJOUTERAIT des fichiers (doublons).
    builder = builder.config(
        "spark.sql.sources.commitProtocolClass",
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
    )
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    if config.hive_enabled:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()
