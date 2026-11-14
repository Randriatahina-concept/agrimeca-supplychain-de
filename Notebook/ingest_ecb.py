{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# 01 — Ingestion quotidienne des taux de change BCE\n",
    "\n",
    "Exécuté par le Job `agrimeca_fx_daily`, du lundi au vendredi à 17 h (heure de Paris),\n",
    "après la publication des taux de référence BCE (vers 16 h).\n",
    "\n",
    "- réutilise `scripts/ingest_ecb.py` du dépôt (mêmes fonctions qu'en local) ;\n",
    "- récupère les taux des `days_back` derniers jours : le chevauchement couvre les jours fériés\n",
    "  et un éventuel run manqué, le Silver garde un seul taux par devise et par jour ;\n",
    "- écrit la réponse brute dans `/Volumes/supplychain/landing/erp/fx/ingest_date=…/run_id=…/exr.csv`."
   ]
  },
  {
   "cell_type": "code",
   "metadata": {},
   "execution_count": null,
   "outputs": [],
   "source": [
    "# Cellule 1 - paramètres (modifiables dans le Job sans toucher au code)\n",
    "dbutils.widgets.text(\"landing\", \"/Volumes/supplychain/landing/erp\")\n",
    "dbutils.widgets.text(\"days_back\", \"10\")\n",
    "\n",
    "LANDING = dbutils.widgets.get(\"landing\")\n",
    "DAYS_BACK = int(dbutils.widgets.get(\"days_back\"))"
   ]
  },
  {
   "cell_type": "code",
   "metadata": {},
   "execution_count": null,
   "outputs": [],
   "source": [
    "# Cellule 2 - importer le script du dépôt (dossier scripts/, à côté de Notebook/)\n",
    "import os\n",
    "import sys\n",
    "\n",
    "sys.path.append(os.path.abspath(\"../scripts\"))\n",
    "from ingest_ecb import CURRENCIES, fetch_rates, save_rates  # noqa: E402"
   ]
  },
  {
   "cell_type": "code",
   "metadata": {},
   "execution_count": null,
   "outputs": [],
   "source": [
    "# Cellule 3 - appel de l'API BCE et dépôt dans le Volume\n",
    "from datetime import date, timedelta\n",
    "\n",
    "start = (date.today() - timedelta(days=DAYS_BACK)).isoformat()\n",
    "csv_text = fetch_rates(start)\n",
    "folder = save_rates(csv_text, LANDING)\n",
    "\n",
    "n_rows = len(csv_text.splitlines()) - 1  # moins la ligne d'en-tête\n",
    "print(f\"fx : {n_rows} taux ({', '.join(CURRENCIES)}) depuis {start} -> {folder}\")"
   ]
  },
  {
   "cell_type": "code",
   "metadata": {},
   "execution_count": null,
   "outputs": [],
   "source": [
    "# Cellule 4 - contrôle : un run sans aucun taux fait échouer le Job (et déclenche l'alerte)\n",
    "if n_rows < len(CURRENCIES):\n",
    "    raise ValueError(f\"seulement {n_rows} taux reçus depuis {start} : réponse BCE anormale\")\n",
    "\n",
    "dbutils.notebook.exit(f\"{n_rows} taux depuis {start}\")  # résumé visible dans l'historique du Job"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "language": "python",
   "notebookName": "01_ingest_ecb"
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}