# ============================================================================
# Setup da infraestrutura GCP — Tech Challenge Fase 2 (alternativa via CLI)
# ----------------------------------------------------------------------------
# OBS.: este script é OPCIONAL. O notebook ETL_Pipeline_GCP.ipynb (seção 0.1)
# provisiona os mesmos recursos de forma idempotente via Python, sem gcloud.
# Use este script se preferir criar/inspecionar a infra pelo terminal.
#
# Pré-requisito: Google Cloud SDK instalado (https://cloud.google.com/sdk).
# Executar no PowerShell, a partir da pasta do projeto (onde está lifecycle.json).
# ============================================================================

# 1. Projeto e autenticação (Application Default Credentials)
gcloud config set project fiap-alfabetizacao
gcloud auth application-default login
gcloud auth application-default set-quota-project fiap-alfabetizacao

# 2. APIs necessárias
gcloud services enable storage.googleapis.com pubsub.googleapis.com bigquery.googleapis.com

# 3. Bucket do data lake (us-east1 = always-free tier; colocalizado com o dataset BQ)
gcloud storage buckets create gs://fiap-alfabetizacao-datalake `
  --location=us-east1 --default-storage-class=STANDARD --uniform-bucket-level-access

# 4. Lifecycle FinOps: landing/_processados expira em 7d; bronze -> Nearline em 60d
gcloud storage buckets update gs://fiap-alfabetizacao-datalake --lifecycle-file=lifecycle.json

# 5. Dataset BigQuery da Gold (external tables exigem mesma região do bucket)
bq mk --location=us-east1 --dataset --description "Camada Gold - Indicador Crianca Alfabetizada" `
  fiap-alfabetizacao:alfabetizacao_gold

# 6. Pub/Sub: tópico + subscription (retenção de 1 dia = custo mínimo)
gcloud pubsub topics create eventos-alfabetizacao
gcloud pubsub subscriptions create eventos-alfabetizacao-sub `
  --topic=eventos-alfabetizacao --ack-deadline=60 --message-retention-duration=1d

# ============================================================================
# Verificação end-to-end (após rodar o notebook com as flags ligadas)
# ============================================================================
# gcloud storage ls -r gs://fiap-alfabetizacao-datalake/bronze/ | Select-Object -First 20
# gcloud storage ls gs://fiap-alfabetizacao-datalake/gold/
# bq query --nouse_legacy_sql "SELECT ano, COUNT(*) n FROM ``fiap-alfabetizacao.alfabetizacao_gold.gold_alfabetizacao_uf`` GROUP BY ano ORDER BY ano"
# gcloud pubsub subscriptions pull eventos-alfabetizacao-sub --auto-ack --limit=1   # vazio após o consumo
