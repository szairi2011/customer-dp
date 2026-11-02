"""Lanceur unique soumis à spark-submit : ``entrypoint.py <bronze|silver|gold|verify> [options]``.

Il existe parce que spark-submit exécute un fichier .py (pas un module) : ce fichier est
publié sur HDFS avec la version. Chaque couche reste exécutable seule via
``python -m customer_dp.<couche>.main``.
"""
import importlib
import sys

_LAYERS = {
    "bronze": "customer_dp.bronze.main",
    "silver": "customer_dp.silver.main",
    "gold": "customer_dp.gold.main",
    "verify": "customer_dp.verify",
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _LAYERS:
        sys.exit("Usage : entrypoint.py <{}> [options]".format("|".join(_LAYERS)))
    sys.exit(importlib.import_module(_LAYERS[sys.argv[1]]).main(sys.argv[2:]))
