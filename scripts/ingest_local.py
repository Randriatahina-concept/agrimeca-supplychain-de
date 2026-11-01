"""Lanceur local : python scripts/ingest_local.py [entité ...]  (après : source scripts/env.sh)"""
import os
import sys

from erp_ingest.simple_ingest import ingest_entity

# Taille de page par entité : les commandes (JSON imbriqué) sont lourdes à préparer pour l'API
PAGE_SIZES = {"suppliers": 1000, "parts": 1000, "purchase-orders": 200}

entities = sys.argv[1:] or ["suppliers", "parts", "purchase-orders"]

for entity in entities:
    ingest_entity(
        base_url=os.environ["ERP_BASE_URL"].rstrip("/"),
        client_id=os.environ.get("ERP_CLIENT_ID", "agrimeca-data"),
        client_secret=os.environ["ERP_CLIENT_SECRET"],
        entity=entity,
        landing="landing/erp",
        page_size=PAGE_SIZES.get(entity, 1000),
    )
