"""Lanceur local (après : source scripts/env.sh)

    python scripts/ingest_local.py                         # les trois entités, depuis le début
    python scripts/ingest_local.py parts                   # une entité
    python scripts/ingest_local.py purchase-orders --resume <dossier du run interrompu>
"""
import os
import sys

from erp_ingest.simple_ingest import ingest_entity, last_cursor

PAGE_SIZES = {"suppliers": 1000, "parts": 1000, "purchase-orders": 200}

args = sys.argv[1:]
start_cursor = None
if "--resume" in args:
    i = args.index("--resume")
    start_cursor = last_cursor(args[i + 1])
    args = args[:i] + args[i + 2:]
    if start_cursor is None:
        sys.exit("Ce run était déjà complet : rien à reprendre.")

entities = args or ["suppliers", "parts", "purchase-orders"]

for entity in entities:
    ingest_entity(
        base_url=os.environ["ERP_BASE_URL"].rstrip("/"),
        client_id=os.environ.get("ERP_CLIENT_ID", "agrimeca-data"),
        client_secret=os.environ["ERP_CLIENT_SECRET"],
        entity=entity,
        landing="landing/erp",
        page_size=PAGE_SIZES.get(entity, 1000),
        start_cursor=start_cursor,
    )
