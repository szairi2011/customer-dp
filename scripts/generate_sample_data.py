"""Génère le jeu d'exemple déterministe (data/sample/orders.csv et customers.csv).

Usage : python scripts/generate_sample_data.py [dossier_de_sortie]
Les lignes sont écrites en dur : le résultat est identique à chaque exécution.
"""
import csv
import sys
from pathlib import Path

CUSTOMERS = [
    ("customer_id", "customer_name", "segment", "country"),
    ("C001", "Alice Martin", "RETAIL", "FR"),
    ("C002", "Bob Durand", "CORPORATE", "tn"),  # pays en minuscules : normalisé en Silver
    ("C003", "  Chloe Ben Ali ", "RETAIL", "TN"),  # espaces parasites : nettoyés en Silver
    ("C004", "Dina Haddad", "SME", "TN"),
    ("C005", "Eric Lopez", "CORPORATE", "ES"),
]

ORDERS = [
    ("order_id", "customer_id", "order_date", "updated_at", "amount", "status"),
    # Mise à jour : deux versions de O1001, la plus récente (PAID) est conservée.
    ("O1001", "C001", "2026-03-01", "2026-03-01 09:00:00", "120.50", "NEW"),
    ("O1001", "C001", "2026-03-01", "2026-03-01 15:30:00", "120.50", "PAID"),
    # Doublon exact.
    ("O1002", "C002", "2026-03-01", "2026-03-01 10:00:00", "300.00", "PAID"),
    ("O1002", "C002", "2026-03-01", "2026-03-01 10:00:00", "300.00", "PAID"),
    ("O1003", "C003", "2026-03-02", "2026-03-02 11:00:00", "80.00", "SHIPPED"),
    ("O1004", "C004", "2026-03-02", "2026-03-02 12:00:00", "45.25", "CANCELLED"),
    # Client inconnu : conservé et signalé.
    ("O1005", "C999", "2026-03-02", "2026-03-02 13:00:00", "60.00", "PAID"),
    # Rejets : montant illisible, montant nul, statut invalide, identifiant manquant, date illisible.
    ("O1006", "C001", "2026-03-03", "2026-03-03 08:00:00", "abc", "PAID"),
    ("O1007", "C002", "2026-03-03", "2026-03-03 09:00:00", "", "PAID"),
    ("O1008", "C005", "2026-03-03", "2026-03-03 10:00:00", "200.00", "LOST"),
    ("", "C001", "2026-03-03", "2026-03-03 11:00:00", "10.00", "PAID"),
    ("O1010", "C003", "not-a-date", "2026-03-03 15:00:00", "99.00", "PAID"),
    # Statut en minuscules : normalisé.
    ("O1009", "C004", "2026-03-03", "2026-03-03 14:00:00", "150.75", "paid"),
    ("O1011", "C005", "2026-03-03", "2026-03-03 16:00:00", "75.10", "SHIPPED"),
    # Même updated_at, montants différents : départage déterministe (montant le plus élevé).
    ("O1012", "C001", "2026-03-03", "2026-03-03 17:00:00", "50.00", "PAID"),
    ("O1012", "C001", "2026-03-03", "2026-03-03 17:00:00", "55.00", "PAID"),
    # Montant avec espaces : converti après nettoyage.
    ("O1013", "C002", "2026-03-02", "2026-03-02 14:00:00", " 42.00 ", "PAID"),
]


def write_csv(path: Path, rows) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        csv.writer(handle, lineterminator="\n").writerows(rows)


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data" / "sample"
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "customers.csv", CUSTOMERS)
    write_csv(out / "orders.csv", ORDERS)
    print(f"Données écrites dans {out}")


if __name__ == "__main__":
    main()
