"""Job AWS Glue — camada BRONZE do pipeline de alfabetização (Tech Challenge Fase 2).

Ingere as fontes do Indicador Criança Alfabetizada (Base dos Dados/INEP + IBGE)
e grava Parquet bruto particionado no bucket SOR (s3://BUCKET_SOR/bronze/ENTIDADE).

Nota sobre partições: as tabelas de negócio têm coluna própria `ano` (ano da
avaliação), então as partições de ingestão usam ano_ingestao/mes_ingestao/dia_ingestao
para não colidir com ela.

Modos de ingestão (--MODO):
  bigquery  -> basedosdados.read_table (requer --additional-python-modules basedosdados
               e GOOGLE_APPLICATION_CREDENTIALS apontando para a service account GCP,
               ex.: baixada de Secrets Manager/S3 no bootstrap do job)
  api_ibge  -> API pública do IBGE (somente ENTIDADE=dim_municipio_ibge)
  landing   -> Parquet previamente estagiado em s3://BUCKET_SOR/landing/ENTIDADE/
               (recomendado para `alunos`, ~3,9M linhas: evita o driver pandas)
"""

import sys
import logging
from datetime import datetime, timezone

import requests
import pandas as pd

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ============================================================
# PARÂMETROS DO JOB
# ============================================================
# Job details -> Advanced properties -> Job parameters
#
#   --ENTIDADE    uf | municipio | meta_alfabetizacao_brasil | meta_alfabetizacao_uf
#                 | meta_alfabetizacao_municipio | alunos | dicionario | dim_municipio_ibge
#   --MODO        bigquery | api_ibge | landing
#   --BUCKET_SOR  420411424817-data-sor

## @params: [JOB_NAME, ENTIDADE, MODO, BUCKET_SOR]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'ENTIDADE',
    'MODO',
    'BUCKET_SOR',
])

# ============================================================
# CONTEXTO GLUE E SPARK
# ============================================================

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sparkContext.setLogLevel("WARN")

# ============================================================
# VARIÁVEIS
# ============================================================

JOB_NAME       = args['JOB_NAME']
ENTIDADE       = args['ENTIDADE']
MODO           = args['MODO']
BUCKET_SOR     = args['BUCKET_SOR']
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano_ing, mes_ing, dia_ing = INGESTION_DATE.split("-")

BILLING_PROJECT_ID = "fiap-alfabetizacao"
DATASET_ID         = "br_inep_avaliacao_alfabetizacao"
IBGE_URL           = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?view=nivelado"

ENTIDADES_BD = [
    "uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio", "alunos", "dicionario",
]
ENTIDADES_VALIDAS = ENTIDADES_BD + ["dim_municipio_ibge"]
if ENTIDADE not in ENTIDADES_VALIDAS:
    raise ValueError(f"ENTIDADE invalida: {ENTIDADE}. Use uma de: {ENTIDADES_VALIDAS}")

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"MODO      : {MODO}")
log.info(f"DATA      : {INGESTION_DATE}")
log.info(f"CAMADA    : BRONZE -> s3://{BUCKET_SOR}/bronze/{ENTIDADE}")
log.info("=" * 60)

# ============================================================
# FUNÇÕES — INGESTÃO
# ============================================================

def ingerir_bigquery(entidade):
    """Extrai a tabela da Base dos Dados via BigQuery (driver pandas)."""
    log.info(f"[INGESTAO] basedosdados.read_table: {DATASET_ID}.{entidade}")
    import basedosdados as bd
    df_pandas = bd.read_table(
        dataset_id=DATASET_ID,
        table_id=entidade,
        billing_project_id=BILLING_PROJECT_ID,
    )
    log.info(f"[INGESTAO] {len(df_pandas)} registros recebidos do BigQuery")
    return spark.createDataFrame(df_pandas)

def ingerir_api_ibge():
    """Dimensão de municípios reais (API pública do IBGE, view nivelado)."""
    log.info(f"[INGESTAO] Consumindo: {IBGE_URL}")
    response = requests.get(IBGE_URL, timeout=30)
    response.raise_for_status()
    dim = pd.DataFrame(response.json())[
        ["municipio-id", "municipio-nome", "UF-sigla", "UF-nome", "regiao-nome"]
    ]
    dim.columns = ["id_municipio", "nome_municipio", "sigla_uf", "nome_uf", "regiao"]
    dim["id_municipio"] = dim["id_municipio"].astype(str)
    log.info(f"[INGESTAO] {len(dim)} municipios recebidos | status={response.status_code}")
    return spark.createDataFrame(dim)

def ingerir_landing(entidade):
    """Lê extratos Parquet estagiados na landing zone do bucket SOR."""
    path = f"s3://{BUCKET_SOR}/landing/{entidade}/"
    log.info(f"[INGESTAO] Lendo landing: {path}")
    df = spark.read.parquet(path)
    log.info(f"[INGESTAO] {df.count()} registros lidos da landing")
    return df

def construir_bronze(df):
    """Anexa metadados de linhagem sem alterar os dados de negócio."""
    log.info("[BRONZE] Adicionando metadados de linhagem")
    colunas_negocio = [F.col(c).cast("string") for c in df.columns]
    return (df
        .withColumn("_record_hash",         F.md5(F.concat_ws("|", *colunas_negocio)))
        .withColumn("_ingestion_timestamp", F.lit(INGESTION_TS))
        .withColumn("_ingestion_date",      F.lit(INGESTION_DATE))
        .withColumn("_source_system",       F.lit(f"{MODO}:{DATASET_ID if MODO == 'bigquery' else ENTIDADE}"))
        .withColumn("_source_entity",       F.lit(ENTIDADE))
        .withColumn("_job_name",            F.lit(JOB_NAME))
        .withColumn("ano_ingestao", F.lit(ano_ing))
        .withColumn("mes_ingestao", F.lit(mes_ing))
        .withColumn("dia_ingestao", F.lit(dia_ing))
    )

def checar_qualidade(df, checks):
    log.info(f"[DQ:BRONZE] Iniciando verificacoes | checks={len(checks)}")
    passou = falhou = criticos = 0

    for check in checks:
        tipo    = check["tipo"]
        coluna  = check.get("coluna")
        valor   = check.get("valor")
        critico = check.get("critico", True)
        ok      = False
        detalhe = ""

        try:
            if tipo == "not_null":
                nulos   = df.filter(F.col(coluna).isNull()).count()
                ok      = nulos == 0
                detalhe = f"{nulos} nulos encontrados"
            elif tipo == "min_count":
                contagem = df.count()
                ok       = contagem >= valor
                detalhe  = f"contagem={contagem} | minimo={valor}"
            elif tipo == "unique":
                colunas = coluna if isinstance(coluna, list) else [coluna]
                dups    = df.count() - df.select(*colunas).distinct().count()
                ok      = dups == 0
                detalhe = f"{dups} duplicatas encontradas"
        except Exception as e:
            ok      = False
            detalhe = f"Erro: {e}"

        status = "PASS" if ok else ("FAIL" if critico else "WARN")
        if ok:
            passou += 1
            log.info(f"[DQ:BRONZE] {status} | {tipo} | coluna={coluna} | {detalhe}")
        else:
            falhou += 1
            if critico:
                criticos += 1
                log.error(f"[DQ:BRONZE] {status} | {tipo} | coluna={coluna} | {detalhe}")
            else:
                log.warning(f"[DQ:BRONZE] {status} | {tipo} | coluna={coluna} | {detalhe}")

    score = round(passou / len(checks) * 100, 1)
    log.info(f"[DQ:BRONZE] Score={score}% | PASS={passou} FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"[DQ:BRONZE] {criticos} check(s) critico(s) falharam. Job interrompido.")

def salvar_bronze(df):
    path = f"s3://{BUCKET_SOR}/bronze/{ENTIDADE}"
    log.info(f"[BRONZE] Salvando em: {path}")
    df.write.partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao").mode("overwrite").parquet(path)
    log.info(f"[BRONZE] {df.count()} registros salvos")
    return path

# ============================================================
# REGRAS DE QUALIDADE
# ============================================================

CHECKS = {
    "uf": [
        {"tipo": "min_count", "valor": 50,                     "critico": True},
        {"tipo": "not_null",  "coluna": "ano",                 "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",            "critico": True},
    ],
    "municipio": [
        {"tipo": "min_count", "valor": 1000,                   "critico": True},
        {"tipo": "not_null",  "coluna": "ano",                 "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio",        "critico": True},
    ],
    "meta_alfabetizacao_brasil": [
        {"tipo": "min_count", "valor": 1,                      "critico": True},
        {"tipo": "not_null",  "coluna": "rede",                "critico": True},
    ],
    "meta_alfabetizacao_uf": [
        {"tipo": "min_count", "valor": 27,                     "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",            "critico": True},
        {"tipo": "not_null",  "coluna": "rede",                "critico": True},
    ],
    "meta_alfabetizacao_municipio": [
        {"tipo": "min_count", "valor": 1000,                   "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio",        "critico": True},
    ],
    "alunos": [
        {"tipo": "min_count", "valor": 5000,                   "critico": True},
        {"tipo": "not_null",  "coluna": "ano",                 "critico": True},
        {"tipo": "not_null",  "coluna": "id_aluno",            "critico": True},
    ],
    "dicionario": [
        {"tipo": "min_count", "valor": 1,                      "critico": True},
        {"tipo": "not_null",  "coluna": "chave",               "critico": True},
        {"tipo": "not_null",  "coluna": "nome_coluna",         "critico": True},
    ],
    "dim_municipio_ibge": [
        {"tipo": "min_count", "valor": 5000,                   "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio",        "critico": True},
        {"tipo": "unique",    "coluna": "id_municipio",        "critico": True},
    ],
}

# ============================================================
# EXECUÇÃO
# ============================================================

if MODO == "api_ibge":
    if ENTIDADE != "dim_municipio_ibge":
        raise ValueError("MODO=api_ibge so vale para ENTIDADE=dim_municipio_ibge")
    df_raw = ingerir_api_ibge()
elif MODO == "bigquery":
    if ENTIDADE not in ENTIDADES_BD:
        raise ValueError(f"MODO=bigquery so vale para as tabelas da Base dos Dados: {ENTIDADES_BD}")
    df_raw = ingerir_bigquery(ENTIDADE)
elif MODO == "landing":
    df_raw = ingerir_landing(ENTIDADE)
else:
    raise ValueError(f"MODO invalido: {MODO}. Use bigquery | api_ibge | landing")

df_bronze = construir_bronze(df_raw)

checks = CHECKS.get(ENTIDADE, [])
if checks:
    checar_qualidade(df_bronze, checks)
else:
    log.warning(f"[DQ:BRONZE] Nenhuma regra definida para '{ENTIDADE}' — pulando verificacao")

bronze_path = salvar_bronze(df_bronze)

log.info("=" * 60)
log.info("SUMARIO BRONZE")
log.info(f"  Destino : {bronze_path}/ano_ingestao={ano_ing}/mes_ingestao={mes_ing}/dia_ingestao={dia_ing}/")
log.info(f"  Proxima etapa: executar job etl-silver com BUCKET_SOR={BUCKET_SOR}")
log.info("=" * 60)

job.commit()
