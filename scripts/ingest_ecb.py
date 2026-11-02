"""Ingestion des taux de change de la Banque centrale européenne (BCE) vers la zone de dépôt.

Script autonome, indépendant de l'ingestion ERP : API publique, sans compte ni clé.
Taux de référence quotidiens = unités de devise pour 1 EUR, un par jour ouvré BCE.

Usage, depuis la racine du dépôt :
    python scripts/ingest_ecb.py                       # depuis le 2023-10-01
    python scripts/ingest_ecb.py 2026-10-01            # depuis une autre date
"""
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import requests

ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/{key}"
CURRENCIES = ["USD", "GBP", "PLN", "CNY", "INR", "TRY"]   # les devises hors EUR des fournisseurs
LANDING = "landing/erp"                                   # même zone de dépôt que l'ERP, entité « fx »

# Erreurs réseau passagères : délai dépassé, connexion refusée, ou coupée pendant la réponse
NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def fetch_rates(start: str) -> str:
    """Appelle l'API BCE et renvoie la réponse CSV (texte). Jusqu'à 5 essais."""
    # Clé SDMX : quotidien . devises . contre l'euro . taux de référence . moyenne du jour
    key = "D." + "+".join(CURRENCIES) + ".EUR.SP00.A"
    params = {"format": "csvdata", "startPeriod": start}

    for attempt in range(1, 6):
        try:
            r = requests.get(ECB_URL.format(key=key), params=params, timeout=120)
        except NETWORK_ERRORS as err:
            wait = 10 * attempt  # 10 s, 20 s, 30 s…
            print(f"{type(err).__name__}, nouvel essai {attempt}/5 dans {wait} s")
            time.sleep(wait)
            continue
        if r.status_code in (429, 500, 502, 503, 504):  # quota ou panne passagère
            wait = int(r.headers.get("Retry-After", 10 * attempt))
            print(f"HTTP {r.status_code}, nouvel essai {attempt}/5 dans {wait} s")
            time.sleep(wait)
            continue
        r.raise_for_status()  # autre erreur (400, 404…) : on s'arrête
        return r.text
    raise RuntimeError("API BCE indisponible après 5 essais")


def save_rates(csv_text: str, landing: str) -> str:
    """Écrit la réponse brute dans landing/fx/ingest_date=…/run_id=…/exr.csv. Renvoie le dossier."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    folder = os.path.join(landing, "fx", f"ingest_date={run_id[:8]}", f"run_id={run_id}")
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "exr.csv"), "w", encoding="utf-8") as f:
        f.write(csv_text)  # sans transformation : le nettoyage est le travail du Silver
    return folder


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2023-10-01"
    csv_text = fetch_rates(start)
    folder = save_rates(csv_text, LANDING)
    n_rows = len(csv_text.splitlines()) - 1  # moins la ligne d'en-tête
    print(f"fx : {n_rows} taux ({', '.join(CURRENCIES)}) depuis {start} -> {folder}")