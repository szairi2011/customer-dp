"""Bronze : lecture brute et traçabilité. Aucune règle métier ici."""
import datetime as dt
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Schémas explicites, tout en chaînes : Bronze conserve la valeur brute. Les valeurs
# invalides (montant « abc », date illisible...) ne sont pas des erreurs de lecture :
# elles sont qualifiées et écartées en Silver.
ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_date", T.StringType()),
        T.StructField("updated_at", T.StringType()),
        T.StructField("amount", T.StringType()),
        T.StructField("status", T.StringType()),
    ]
)
CUSTOMERS_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_name", T.StringType()),
        T.StructField("segment", T.StringType()),
        T.StructField("country", T.StringType()),
    ]
)


class BronzeReadError(RuntimeError):
    """Fichier source illisible ou structurellement invalide (erreur de lecture)."""


def read_csv(spark: SparkSession, path: str, schema: T.StructType) -> DataFrame:
    """Lit un CSV avec un schéma explicite (pas d'inférence : passe supplémentaire, résultat instable).

    Fichier absent ou en-tête inattendu : ``BronzeReadError`` immédiate. Avec FAILFAST, une
    ligne mal formée interrompt l'écriture Spark qui consomme le DataFrame (erreur d'exécution).
    """
    expected = schema.fieldNames()
    try:
        first = spark.read.text(path).limit(1).collect()
        header = [name.strip() for name in first[0].value.split(",")] if first else []
        if header != expected:
            raise BronzeReadError(f"En-tête inattendu dans {path} : {header} (attendu : {expected})")
        return spark.read.schema(schema).option("header", "true").option("mode", "FAILFAST").csv(path)
    except BronzeReadError:
        raise
    except Exception as exc:  # noqa: BLE001 - AnalysisException / Py4JJavaError
        raise BronzeReadError(f"Lecture impossible de {path} : {exc}") from exc


def add_ingestion_metadata(
    df: DataFrame, batch_date: str, ingestion_timestamp: Optional[dt.datetime] = None
) -> DataFrame:
    """Ajoute la traçabilité : fichier source, horodatage d'ingestion et date de lot."""
    ts = ingestion_timestamp or dt.datetime.utcnow()
    return (
        df.withColumn("ingestion_timestamp", F.lit(ts).cast("timestamp"))
        .withColumn("source_file", F.input_file_name())
        .withColumn("batch_date", F.lit(batch_date).cast("date"))
    )
