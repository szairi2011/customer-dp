"""Configuration du produit.

Les constantes métier (tables, statuts, partitionnement) sont dans ce module. Les valeurs propres à
un environnement viennent des variables ``CUSTOMER_DP_*`` fournies par le profil
(conf/profiles/<profil>.env, exportées par scripts/run.sh) : aucun fichier de configuration à déployer.
"""
import os
from dataclasses import dataclass, field
from typing import Dict, Mapping, Optional, Tuple

ORDERS_FILE = "orders.csv"
CUSTOMERS_FILE = "customers.csv"


class ConfigError(ValueError):
    """Variable d'environnement absente ou invalide."""


@dataclass(frozen=True)
class TableConfig:
    name: str
    path: str
    partition_by: Tuple[str, ...]
    write_mode: str  # "dynamic" : remplace les partitions présentes ; "overwrite" : remplace tout


@dataclass(frozen=True)
class AppConfig:
    # These environment-specific values come from CUSTOMER_DP_* in the selected profile.
    input_root: str  # CUSTOMER_DP_INPUT_ROOT
    output_root: str  # CUSTOMER_DP_OUTPUT_ROOT
    hive_enabled: bool = False  # CUSTOMER_DP_HIVE_ENABLED
    database: str = "customer_dp"  # CUSTOMER_DP_DATABASE
    spark_master: Optional[str] = None  # CUSTOMER_DP_SPARK_MASTER; usually set by spark-submit

    # These are product rules with code defaults, not profile settings.
    valid_statuses: Tuple[str, ...] = ("NEW", "PAID", "SHIPPED", "CANCELLED")
    counted_statuses: Tuple[str, ...] = ("PAID", "SHIPPED")  # statuts comptés dans le chiffre d'affaires
    amount_scale: int = 2  # decimal(18,2), arrondi HALF_UP
    broadcast_customers: bool = True  # table clients petite : broadcast join
    tables: Dict[str, TableConfig] = field(init=False)

    def __post_init__(self) -> None:
        def table(layer: str, name: str, partition_by: Tuple[str, ...], mode: str) -> TableConfig:
            return TableConfig(name, _join(_join(self.output_root, layer), name), partition_by, mode)

        tables = {
            # Rejeu d'un lot : seule la partition batch_date est remplacée.
            "bronze_orders": table("bronze", "bronze_orders", ("batch_date",), "dynamic"),
            "bronze_customers": table("bronze", "bronze_customers", ("batch_date",), "dynamic"),
            # Silver : recalcul complet depuis Bronze (déduplication globale).
            "silver_orders": table("silver", "silver_orders", ("event_date",), "overwrite"),
            "silver_orders_rejected": table("silver", "silver_orders_rejected", (), "overwrite"),
            # Gold : rejeu d'une période = remplacement des seules partitions concernées.
            "gold_daily_sales": table("gold", "gold_daily_sales", ("event_date",), "dynamic"),
        }
        object.__setattr__(self, "tables", tables)

    def input_path(self, source: str) -> str:
        files = {"orders": ORDERS_FILE, "customers": CUSTOMERS_FILE}
        if source not in files:
            raise ConfigError(f"Source inconnue : '{source}'")
        return _join(self.input_root, files[source])

    def table(self, key: str) -> TableConfig:
        try:
            return self.tables[key]
        except KeyError:
            raise ConfigError(f"Table inconnue : '{key}'") from None


def _join(root: str, name: str) -> str:
    return root.rstrip("/") + "/" + name.lstrip("/")


def load_config(env: Optional[Mapping[str, str]] = None) -> AppConfig:
    """Build AppConfig from the selected profile's exported CUSTOMER_DP_* variables.

    scripts/run.sh loads conf/profiles/<env>.env before starting the job. The optional
    ``env`` argument lets tests provide values without changing the process environment.
    """
    env = os.environ if env is None else env

    def required(name: str) -> str:
        value = env.get(name, "")
        if not value:
            raise ConfigError(
                f"Variable {name} obligatoire : renseignez-la dans le profil conf/profiles/<profil>.env "
                "(ou exportez-la avant d'exécuter le job)."
            )
        return value

    return AppConfig(
        input_root=required("CUSTOMER_DP_INPUT_ROOT"),
        output_root=required("CUSTOMER_DP_OUTPUT_ROOT"),
        hive_enabled=env.get("CUSTOMER_DP_HIVE_ENABLED", "false").lower() == "true",
        database=env.get("CUSTOMER_DP_DATABASE") or "customer_dp",
        spark_master=env.get("CUSTOMER_DP_SPARK_MASTER") or None,
    )
