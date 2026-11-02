"""Conventions de journalisation : un format unique, niveau configurable."""
import logging
import sys

_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Initialise le logger racine du produit (idempotent) et le retourne."""
    logger = logging.getLogger("customer_dp")
    logger.setLevel(level.upper())
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT))
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger("customer_dp." + name)
