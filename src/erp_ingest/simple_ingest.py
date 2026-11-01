"""Ingestion simple de l'API ERP vers une zone de dépôt (dossier local ou Volume Databricks)."""
import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests

# Erreurs réseau passagères : délai dépassé, connexion refusée, ou coupée pendant la réponse
NETWORK_ERRORS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
)


def get_token(base_url: str, client_id: str, client_secret: str) -> str:
    """Demande un jeton OAuth (valable 15 minutes) et le renvoie."""
    r = requests.post(
        f"{base_url}/auth/token",
        json={"client_id": client_id, "client_secret": client_secret},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def ingest_entity(base_url: str, client_id: str, client_secret: str,
                  entity: str, landing: str, page_size: int = 1000) -> str:
    """Lit toutes les pages d'une entité et écrit chacune dans un fichier JSON. Renvoie le dossier du run."""
    headers = {"Authorization": f"Bearer {get_token(base_url, client_id, client_secret)}"}

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    folder = os.path.join(landing, entity, f"ingest_date={run_id[:8]}", f"run_id={run_id}")
    os.makedirs(folder, exist_ok=True)

    cursor, n_page, n_rec = None, 0, 0
    attempts = 0  # échecs réseau consécutifs (délai dépassé, connexion coupée)

    while True:
        params = {"limit": page_size}
        if cursor:
            params["cursor"] = cursor

        # Un délai dépassé ou une connexion coupée n'est pas une réponse de l'API mais une exception Python :
        # on la traite comme une erreur passagère (attendre, réessayer la même page).
        try:
            r = requests.get(f"{base_url}/v1/{entity}", headers=headers, params=params, timeout=120)
        except NETWORK_ERRORS as err:
            attempts += 1
            if attempts > 5:
                raise  # trop d'échecs d'affilée : on abandonne
            wait = 10 * attempts  # 10 s, 20 s, 30 s…
            print(f"{entity} page {n_page + 1} : {type(err).__name__}, nouvel essai {attempts}/5 dans {wait} s")
            time.sleep(wait)
            continue
        attempts = 0

        if r.status_code == 401:  # jeton expiré
            headers["Authorization"] = f"Bearer {get_token(base_url, client_id, client_secret)}"
            continue
        if r.status_code == 429:  # quota dépassé
            time.sleep(int(r.headers.get("Retry-After", 5)))
            continue
        if r.status_code in (500, 503):  # panne passagère
            time.sleep(2)
            continue
        r.raise_for_status()

        page = r.json()
        n_page += 1
        n_rec += page["meta"]["count"]
        with open(os.path.join(folder, f"page_{n_page:06d}.json"), "w") as f:
            json.dump({"_run_id": run_id, "_fetched_at": datetime.now(timezone.utc).isoformat(), **page}, f)

        if not page["paging"]["has_more"]:
            break
        cursor = page["paging"]["next_cursor"]

    print(f"{entity} : {n_page} pages, {n_rec} enregistrements -> {folder}")
    return folder
