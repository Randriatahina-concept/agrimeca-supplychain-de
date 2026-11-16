# AgriMeca Supply Chain — Data Engineering sur Azure Databricks

Plateforme de données de bout en bout pour piloter la performance fournisseurs d'un industriel fictif, AgriMeca : ingestion d'une **API ERP** contrainte, architecture **médaillon** sur **Azure Databricks / Unity Catalog**, transformations et tests **dbt**, restitution **Power BI** et cas d'usage **IA**.

> Projet personnel de portfolio. L'entreprise et son ERP sont simulés ; les volumes, les contraintes techniques et les défauts de données sont réalistes, et **tous les chiffres de ce document sont mesurés** sur les données du projet.

---

## 📊 Problématique

**Contexte** — AgriMeca conçoit et fabrique des **machines agricoles** : outils de travail du sol, semoirs, faucheuses et équipements de fenaison. Ses 5 usines (Colmar, Saverne, Monswiller, Lyon, Nantes) assemblent ces machines à partir de pièces achetées auprès d'environ 21 600 fournisseurs dans 10 pays : tôlerie et fonderie pour les châssis, transmissions et roulements pour les organes en mouvement, hydraulique et électronique pour les commandes, visserie, joints et peinture. Une pièce en retard peut arrêter une ligne d'assemblage en pleine saison.

Les acheteurs ne savent pourtant pas répondre rapidement à des questions simples : quels fournisseurs livrent à l'heure et en totalité, où va l'argent des achats, quelles commandes relancer aujourd'hui.

**Constats mesurés sur les données**

- **37 % des lignes de commande** sont en attente de livraison, sans vue consolidée de celles qui sont déjà en retard.
- Les achats sont libellés en **7 devises** : impossible de comparer les fournisseurs sans conversion.
- Les données de l'ERP sont **peu fiables** : 10 % de dates hors format, 2 % de devises manquantes, 1 % de quantités aberrantes, environ 8 % de fournisseurs en double.
- Conséquence chiffrée : **0,5 % des lignes représentaient 84 % des montants d'achat**. Sans contrôle qualité, les dépenses seraient surestimées d'un facteur 6.

**Objectif** — Construire une plateforme fiable et industrialisée qui collecte les données de l'ERP, les corrige de façon traçable, et calcule les indicateurs de pilotage des achats : fiabilité des fournisseurs, délais, dépenses, risques.

## 🗂️ Sources de données

| Source | Contenu | Volume | Ingestion |
| --- | --- | --- | --- |
| **API ERP AgriMeca** (FastAPI sur Azure Container Apps) | fournisseurs, articles, commandes d'achat et leurs lignes | 1,3 million de commandes, **5,2 millions de lignes**, 400 000 articles | Python (OAuth, pagination par curseur, reprise) → Volume Unity Catalog → Auto Loader |
| **API de la Banque centrale européenne** | taux de change de référence quotidiens | 6 devises × 3 ans | Job Databricks quotidien → Volume → Auto Loader |

L'API ERP reproduit les contraintes d'une vraie API d'entreprise : jeton qui expire toutes les 15 minutes, quotas (`429`), pannes (`5xx`). Un simulateur y crée en continu de nouvelles commandes, réceptions et annulations.

## 🛠️ Stack

| Outil | Rôle |
| --- | --- |
| **Azure Container Apps**, **Azure Key Vault** | hébergement de l'API ERP ; secrets |
| **Python** (`requests`) | ingestion robuste de l'API : nouveaux essais, reprise sur curseur |
| **Databricks** (Delta Lake, Unity Catalog, Volumes) | stockage, gouvernance et calcul serverless |
| **Auto Loader** (PySpark) | ingestion incrémentale du Bronze |
| **dbt Core** (`dbt-databricks`) | transformations Silver et Gold, tests, documentation |
| **Databricks Jobs** | orchestration (Job quotidien des taux de change) |
| **DuckDB**, PySpark local | exploration et mise au point sans coût cloud |
| **Power BI** | tableaux de bord sur la couche Gold |
| **GitHub** | versionnement, relié à Databricks (Git folder) |

## 🏗️ Architecture (médaillon)

| Couche | Contenu | Outil |
| --- | --- | --- |
| 🥉 **Bronze** | réponses d'API brutes, une ligne par enregistrement, colonnes de traçabilité (run, fichier, dates de lecture et de chargement) ; doublons conservés | Auto Loader |
| 🥈 **Silver** | JSON extrait et typé, dernière version de chaque enregistrement, dates normalisées, devise imputée, montants en EUR, lignes défectueuses en quarantaine : `stg_erp__*`, `int_fx_rates_daily`, `int_purchase_order_lines` | dbt |
| 🥇 **Gold** | modèle en étoile : `fact_po_line`, `dim_supplier`, `dim_part`, `dim_plant`, `dim_date` | dbt |

Règles de gestion du Silver : une ligne par enregistrement dans sa version la plus récente, dates ramenées à un seul format, lignes de commande dépliées, un taux de change pour chaque jour (week-ends compris), devise manquante remplacée par celle du fournisseur, boîtes de 12 converties en pièces, montants convertis en EUR au taux du jour de commande, lignes aberrantes mises en quarantaine et jamais supprimées.
<img width="3200" height="1720" alt="image" src="https://github.com/user-attachments/assets/5e856db9-f9e9-4211-a8c0-a0b68315754d" />


## ✅ Qualité et gouvernance

- **45 tests dbt**, exécutés à chaque construction : `unique`, `not_null`, `relationships` (clés étrangères), `accepted_values`, et tests sur les sources Bronze. Si une source échoue, les modèles qui en dépendent ne sont pas reconstruits.
- **Quarantaine** : chaque ligne défectueuse porte un motif (`QTY_OUTLIER`, `QTY_NEGATIVE_OR_ZERO`…) et reste consultable ; le Gold ne lit que les lignes valides (99 %).
- **Traçabilité** : chaque correction laisse une trace (devise imputée, valeur d'origine des dates conservée) ; chaque ligne Bronze remonte à son fichier source.
- **Contrôles de complétude** : à chaque couche, le nombre de lignes est rapproché de la couche précédente (pages lues, lignes chargées, aucune ligne perdue par une jointure).
- **Sécurité** : aucun secret dans le code ni dans Git (Key Vault, secret scope, connexion dbt en OAuth).
- **Lignage** : dépendances entre modèles gérées par dbt (`ref`, `source`), tables gouvernées par Unity Catalog.

## 🎯 Indicateurs

**Métier (couche Gold, spécifiés)**

| Indicateur | Question |
| --- | --- |
| OTIF, décomposé en OT et IF | quels fournisseurs livrent à l'heure et en totalité ? |
| Délai réel et écart au délai promis | combien de temps faut-il vraiment, et la promesse est-elle tenue ? |
| Dépenses | où va l'argent, par catégorie, fournisseur, site et mois ? |
| Classification ABC des articles | quels articles concentrent les achats ? |
| Écart de prix d'achat | paie-t-on plus ou moins que le prix catalogue ? |
| Concentration et articles mono-source | dépend-on trop de quelques fournisseurs ? |
| Retards en cours | quelles commandes relancer aujourd'hui ? |
| Taux de quarantaine | les données de l'ERP sont-elles fiables ? |

**Pipeline (data engineering)**

| Indicateur | Mesuré |
| --- | --- |
| Fraîcheur des taux de change | chaque jour ouvré, après la publication de la BCE |
| Tests dbt passants | 100 % requis avant de publier une couche |
| Durée de chargement | 1,3 million de commandes en Bronze en 56 s (serverless) |
| Reprise d'ingestion | 418 380 commandes reprises en ~1 h 30 au lieu de 5 à 6 h |
| Coût | consommation suivie par notebook et par Job (`system.billing.usage`) ; historique chargé en local pour ne pas payer l'attente de l'API |

## 🚀 Démarrer

```bash
git clone https://github.com/Randriatahina-concept/agrimeca-supplychain-de.git
cd agrimeca-supplychain-de
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" dbt-databricks

python scripts/ingest_local.py suppliers      # ingestion (secrets lus dans Key Vault)
cd dbt/agrimeca && dbt build                  # Silver et Gold : modèles et tests
```

Les configurations Azure, Databricks et dbt sont détaillées pas à pas dans la documentation technique.

## 📁 Structure du dépôt

```text
agrimeca-supplychain-de/
├── src/erp_ingest/     # module d'ingestion de l'API ERP
├── scripts/            # lancements locaux : ingestion, taux BCE, Bronze local, exploration DuckDB
├── Notebook/           # notebooks Databricks : ingestion, Bronze Auto Loader, taux BCE
├── dbt/agrimeca/       # projet dbt : staging, intermediate, marts, macros, tests
├── tests/              # tests pytest
└── pyproject.toml
```

## 🗺️ Feuille de route

| Étape | Statut |
| --- | --- |
| API ERP simulée sur Azure, ingestion robuste avec reprise | fait |
| Bronze Auto Loader, contrôles de complétude et d'idempotence | fait |
| Taux de change BCE, Job quotidien | fait |
| Silver dbt : staging, règles de gestion, quarantaine, tests | fait |
| Spécifications fonctionnelles du Gold | fait |
| Gold : dimensions (`dim_plant` fait) et `fact_po_line` | en cours |
| Tests métier (dates cohérentes, quantités positives) et fraîcheur des sources (`dbt source freshness`) | à venir |
| Historisation SCD2 (noms de fournisseurs, prix) | à venir |
| Ingestion incrémentale et orchestration complète (ingestion → Bronze → `dbt build`) | à venir |
| CI GitHub Actions (`ruff`, `pytest`, `dbt build` sur pull request) | à venir |
| Power BI et modèle IA de risque de retard | à venir |

## 📚 Documentation

- **Dossier technique** : toutes les configurations, le code commenté, les résultats de chaque étape, la démarche FinOps et le dépannage.
- **Dictionnaire des données** : chaque table et chaque colonne, les valeurs possibles et les défauts connus.
- **Spécifications fonctionnelles du Gold** : décisions visées, modèle en étoile, règles de gestion, définition des indicateurs, pages Power BI et critères de recette.

## Auteur

Charles Randriatahina, Data Engineer
