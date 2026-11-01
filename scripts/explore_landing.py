"""Exploration des données brutes de landing/erp avec DuckDB (SQL, en local, sans Spark).

Objectif : comprendre la forme des données et repérer les défauts AVANT de construire
le Bronze et les règles de nettoyage du Silver.

Usage, depuis la racine du dépôt :
    python scripts/explore_landing.py
"""
import duckdb

LANDING = "landing/erp"

con = duckdb.connect()  # base en mémoire : rien n'est écrit sur le disque


def show(title: str, sql: str) -> None:
    print(f"\n=== {title} ===")
    con.sql(sql).show(max_width=200)


# 1. Une vue par entité : une ligne par enregistrement, JSON brut conservé (comme le futur Bronze)
for entity, view in [("suppliers", "suppliers"), ("parts", "parts"), ("purchase-orders", "purchase_orders")]:
    con.sql(f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT p._run_id,
               p.filename                AS _file,
               unnest(p.data)            AS payload        -- un enregistrement = un JSON brut
        FROM read_json('{LANDING}/{entity}/*/*/page_*.json',
                       columns = {{'_run_id': 'VARCHAR', '_fetched_at': 'VARCHAR', 'data': 'JSON[]'}},
                       filename = true) AS p
    """)

# 2. Volumes
show("Volumes par entité et par run", """
    SELECT 'suppliers' AS entite, _run_id, count(*) AS lignes FROM suppliers GROUP BY ALL
    UNION ALL SELECT 'parts', _run_id, count(*) FROM parts GROUP BY ALL
    UNION ALL SELECT 'purchase_orders', _run_id, count(*) FROM purchase_orders GROUP BY ALL
    ORDER BY entite, _run_id
""")

# 3. Un exemple brut de chaque entité
for view in ["suppliers", "parts", "purchase_orders"]:
    show(f"Exemple brut : {view}", f"SELECT payload FROM {view} LIMIT 1")

# 4. Doublons techniques (plusieurs runs complets)
show("Doublons d'id (dus aux runs répétés)", """
    SELECT 'suppliers' AS entite, count(*) AS lignes,
           count(DISTINCT payload->>'id') AS ids_distincts FROM suppliers
    UNION ALL SELECT 'parts', count(*), count(DISTINCT payload->>'id') FROM parts
    UNION ALL SELECT 'purchase_orders', count(*), count(DISTINCT payload->>'id') FROM purchase_orders
""")

# 5. Fournisseurs : doublons « métier »
show("Fournisseurs au nom quasi identique", """
    WITH s AS (SELECT DISTINCT payload->>'id' AS id, payload->>'name' AS name FROM suppliers)
    SELECT lower(regexp_replace(name, '[^A-Za-z0-9]', '', 'g')) AS nom_normalise,
           count(*) AS nb, string_agg(id || ' : ' || name, ' | ') AS exemples
    FROM s GROUP BY 1 HAVING count(*) > 1
    ORDER BY nb DESC LIMIT 10
""")

# 6. Articles : unités et suppressions logiques
show("Articles par unité de mesure", """
    SELECT payload->>'uom' AS uom, count(DISTINCT payload->>'id') AS articles
    FROM parts GROUP BY 1 ORDER BY 2 DESC
""")
show("Suppressions logiques (deleted = true)", """
    SELECT 'suppliers' AS entite,
           count(DISTINCT payload->>'id') FILTER (WHERE payload->>'deleted' = 'true') AS supprimes
    FROM suppliers
    UNION ALL SELECT 'parts', count(DISTINCT payload->>'id') FILTER (WHERE payload->>'deleted' = 'true') FROM parts
    UNION ALL SELECT 'purchase_orders', count(DISTINCT payload->>'id') FILTER (WHERE payload->>'deleted' = 'true')
    FROM purchase_orders
""")

# 7. Commandes : statuts, devises manquantes, formats de date
show("Commandes par statut", """
    SELECT payload->>'status' AS statut, count(DISTINCT payload->>'id') AS commandes
    FROM purchase_orders GROUP BY 1 ORDER BY 2 DESC
""")
show("Commandes par devise (NULL = devise manquante)", """
    SELECT payload->>'currency' AS devise, count(DISTINCT payload->>'id') AS commandes
    FROM purchase_orders GROUP BY 1 ORDER BY 2 DESC
""")
show("Formats de order_date", """
    SELECT CASE
             WHEN json_type(payload->'order_date') IN ('BIGINT', 'UBIGINT') THEN 'epoch en millisecondes'
             WHEN payload->>'order_date' SIMILAR TO '[0-9]{2}/[0-9]{2}/[0-9]{4}' THEN 'jj/mm/aaaa'
             WHEN payload->>'order_date' LIKE '____-__-__%' THEN 'ISO (aaaa-mm-jj)'
             ELSE 'autre'
           END AS format,
           count(DISTINCT payload->>'id') AS commandes,
           any_value(payload->>'order_date') AS exemple
    FROM purchase_orders GROUP BY 1 ORDER BY 2 DESC
""")

# 8. Lignes de commande : le JSON imbriqué, éclaté (ce que fera le Silver)
con.sql("""
    CREATE OR REPLACE VIEW po_lines AS
    SELECT DISTINCT
           po.payload->>'id'                          AS po_id,
           (l.line->>'line_no')::INT                  AS line_no,
           l.line->'part'->>'id'                      AS part_id,
           (l.line->'quantity'->>'value')::DOUBLE     AS quantity,
           l.line->'quantity'->>'uom'                 AS uom,
           (l.line->'unit_price'->>'amount')::DOUBLE  AS unit_price,
           l.line->>'promised_date'                   AS promised_date_brute,
           l.line->'receipt'->>'date'                 AS received_date,
           (l.line->'receipt'->>'quantity')::DOUBLE   AS received_qty
    FROM purchase_orders AS po,
         unnest(CAST(po.payload->'lines' AS JSON[])) AS l(line)
""")
show("Lignes de commande : exemple éclaté", "SELECT * FROM po_lines LIMIT 5")
show("Lignes de commande : quantités suspectes", """
    SELECT CASE WHEN quantity < 0 THEN 'négative'
                WHEN quantity >= 1000 THEN '>= 1000 (x1000 ?)'
                ELSE 'normale' END AS quantite,
           count(*) AS lignes, min(quantity) AS min, max(quantity) AS max
    FROM po_lines GROUP BY 1 ORDER BY 2 DESC
""")
show("Lignes de commande : réceptions", """
    SELECT received_date IS NOT NULL AS recue, count(*) AS lignes FROM po_lines GROUP BY 1
""")
