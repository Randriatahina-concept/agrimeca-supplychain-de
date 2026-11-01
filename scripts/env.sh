# Charge la configuration de l'API ERP depuis Azure Key Vault.
# Usage : source scripts/env.sh   à taper dans le terminal pour charger les variables d'environnement
KV=kv-agrimeca-8033
export ERP_BASE_URL=$(az keyvault secret show --vault-name $KV -n erp-base-url --query value -o tsv)
export ERP_CLIENT_SECRET=$(az keyvault secret show --vault-name $KV -n erp-client-secret --query value -o tsv)
export ERP_CLIENT_ID=agrimeca-data
echo "API : $ERP_BASE_URL"


# Java 21 pour PySpark (Java 23+ n'est pas supporté par Spark 4.0)
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
echo "Java : $(java -version 2>&1 | head -1)"
