"""Ingestion simple de l'API ERP vers une zone de dépôt (dossier local ou Volume Databricks)."""
import glob
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


def last_cursor(run_folder: str) -> str | None:
    """Renvoie le curseur de la page suivante, lu dans la dernière page valide d'un run interrompu."""
    for path in sorted(glob.glob(os.path.join(run_folder, "page_*.json")), reverse=True):
        try:
            with open(path) as f:
                page = json.load(f)
        except json.JSONDecodeError:
            print(f"page illisible ignorée : {path}")
            continue
        print(f"reprise après {os.path.basename(path)}")
        return page["paging"]["next_cursor"]
    raise FileNotFoundError(f"aucune page valide dans {run_folder}")


def ingest_entity(base_url: str, client_id: str, client_secret: str,
                  entity: str, landing: str, page_size: int = 1000,
                  start_cursor: str | None = None) -> str:
    """Lit les pages d'une entité et écrit chacune dans un fichier JSON. Renvoie le dossier du run."""
    headers = {"Authorization": f"Bearer {get_token(base_url, client_id, client_secret)}"}

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    folder = os.path.join(landing, entity, f"ingest_date={run_id[:8]}", f"run_id={run_id}")
    os.makedirs(folder, exist_ok=True)

    cursor, n_page, n_rec = start_cursor, 0, 0  # None = depuis le début ; sinon reprise
    attempts = 0

    while True:
        params = {"limit": page_size}
        if cursor:
            params["cursor"] = cursor

        try:
            r = requests.get(f"{base_url}/v1/{entity}", headers=headers, params=params, timeout=120)
        except NETWORK_ERRORS as err:
            attempts += 1
            if attempts > 5:
                raise
            wait = 10 * attempts
            print(f"{entity} page {n_page + 1} : {type(err).__name__}, nouvel essai {attempts}/5 dans {wait} s")
            time.sleep(wait)
            continue
        attempts = 0

        if r.status_code == 401:
            headers["Authorization"] = f"Bearer {get_token(base_url, client_id, client_secret)}"
            continue
        if r.status_code == 429:
            time.sleep(int(r.headers.get("Retry-After", 5)))
            continue
        if r.status_code in (500, 503):
            time.sleep(2)
            continue
        r.raise_for_status()

        page = r.json()
        n_page += 1
        n_rec += page["meta"]["count"]
        with open(os.path.join(folder, f"page_{n_page:06d}.json"), "w") as f:
            json.dump({"_run_id": run_id, "_fetched_at": datetime.now(timezone.utc).isoformat(), **page}, f)
        if n_page % 100 == 0:
            print(f"{entity} : {n_page} pages, {n_rec} enregistrements")

        if not page["paging"]["has_more"]:
            break
        cursor = page["paging"]["next_cursor"]

    print(f"{entity} : {n_page} pages, {n_rec} enregistrements -> {folder}")
    return folder
