"""Job AWS Glue — camada GOLD do pipeline de alfabetização (Tech Challenge Fase 2).

Lê a partição de ingestão do dia da Silver (bucket SOT) e gera os datasets
analíticos no bucket SPEC (s3://BUCKET_SPEC/gold/<tabela>), espelhando a Gold
validada nos notebooks (ETL_Pipeline_PySpark/GCP):

  --ENTIDADE uf         -> gold_alfabetizacao_uf       (ranking anual + variação temporal)
  --ENTIDADE municipio  -> gold_alfabetizacao_municipio (taxa vs meta do ano, gap, atingiu_meta)
  --ENTIDADE brasil     -> gold_brasil_evolucao         (observado vs trajetória de metas 2024-2030)
  --ENTIDADE alunos     -> gold_desempenho_alunos       (desempenho por município/rede/série)

A Gold é particionada pelo `ano` do dado (ano da avaliação) — não pela data de
processamento — para habilitar partition pruning nas consultas analíticas
(Athena/QuickSight leem apenas as partições filtradas).
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
#   --ENTIDADE    uf | municipio | brasil | alunos
#   --BUCKET_SOT  420411424817-data-sot
#   --BUCKET_SPEC 420411424817-data-spec

## @params: [JOB_NAME, ENTIDADE, BUCKET_SOT, BUCKET_SPEC]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'ENTIDADE',
    'BUCKET_SOT',
    'BUCKET_SPEC',
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
BUCKET_SOT     = args['BUCKET_SOT']
BUCKET_SPEC    = args['BUCKET_SPEC']
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano_ing, mes_ing, dia_ing = INGESTION_DATE.split("-")

# Silver de origem de cada visão Gold (brasil deriva da silver de uf)
ENTIDADE_SILVER_BASE = {
    "uf": "uf",
    "municipio": "municipio",
    "brasil": "uf",
    "alunos": "alunos",
}
NOME_TABELA_GOLD = {
    "uf": "gold_alfabetizacao_uf",
    "municipio": "gold_alfabetizacao_municipio",
    "brasil": "gold_brasil_evolucao",
    "alunos": "gold_desempenho_alunos",
}
if ENTIDADE not in NOME_TABELA_GOLD:
    raise ValueError(f"ENTIDADE invalida: {ENTIDADE}. Use uma de: {sorted(NOME_TABELA_GOLD)}")

def silver_path(entidade):
    """Partição de ingestão do dia gerada pelo job Silver."""
    return (f"s3://{BUCKET_SOT}/silver/{entidade}/"
            f"ano_ingestao={ano_ing}/mes_ingestao={mes_ing}/dia_ingestao={dia_ing}/")

SILVER_PATH = silver_path(ENTIDADE_SILVER_BASE[ENTIDADE])
TABELA_GOLD = NOME_TABELA_GOLD[ENTIDADE]

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"LENDO DE  : {SILVER_PATH}")
log.info(f"CAMADA    : GOLD -> s3://{BUCKET_SPEC}/gold/{TABELA_GOLD}")
log.info("=" * 60)

# ============================================================
# FUNÇÕES — APOIO
# ============================================================

def ler_silver(entidade):
    """Lê a partição do dia de uma entidade da Silver, sem colunas técnicas."""
    df = spark.read.parquet(silver_path(entidade))
    tecnicas = [c for c in df.columns if c.startswith("_") or c.endswith("_ingestao")]
    return df.drop(*tecnicas)

def seleciona_existentes(df, colunas):
    return df.select(*[c for c in colunas if c in df.columns])

# ============================================================
# FUNÇÕES — VISÕES ANALÍTICAS
# ============================================================

def gold_uf(df):
    """Ranking anual das UFs e variação temporal da taxa (rede pública)."""
    log.info("[GOLD] Gerando visao: gold_alfabetizacao_uf")
    return (
        df.filter(F.col("rede") == "publica")
        .groupBy("ano", "sigla_uf")
        .agg(
            F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"),
            F.round(F.avg("media_portugues"), 2).alias("media_portugues"),
            F.round(F.avg("meta_alfabetizacao_2030"), 2).alias("meta_2030"),
        )
        .withColumn(
            "ranking_nacional",
            F.rank().over(Window.partitionBy("ano").orderBy(F.desc("taxa_alfabetizacao"))),
        )
        .withColumn(
            "variacao_anual_pp",
            F.round(
                F.col("taxa_alfabetizacao")
                - F.lag("taxa_alfabetizacao").over(Window.partitionBy("sigla_uf").orderBy("ano")),
                2,
            ),
        )
    )

def gold_municipio(df):
    """Taxa municipal vs meta do ano da avaliação (gap e atingimento)."""
    log.info("[GOLD] Gerando visao: gold_alfabetizacao_municipio")
    meta_do_ano = F.lit(None).cast("double")
    for ano_meta in range(2024, 2031):
        coluna_meta = f"meta_alfabetizacao_{ano_meta}"
        if coluna_meta in df.columns:
            meta_do_ano = F.when(F.col("ano") == ano_meta, F.col(coluna_meta)).otherwise(meta_do_ano)
    df = (
        df.withColumn("meta_ano", F.round(meta_do_ano, 2))
        .withColumn("gap_meta", F.round(F.col("taxa_alfabetizacao") - F.col("meta_ano"), 2))
        .withColumn("atingiu_meta", F.col("gap_meta") >= 0)
    )
    return seleciona_existentes(df, [
        "ano", "id_municipio", "nome_municipio", "sigla_uf", "regiao",
        "rede", "rede_desc", "serie", "serie_desc",
        "taxa_alfabetizacao", "media_portugues", "nivel_alfabetizacao",
        "meta_ano", "gap_meta", "atingiu_meta",
    ])

def gold_brasil(df):
    """Taxa nacional observada vs trajetória de metas 2024-2030 (rede pública)."""
    log.info("[GOLD] Gerando visao: gold_brasil_evolucao")
    observado = (
        df.filter(F.col("rede") == "publica")
        .groupBy("ano")
        .agg(F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"))
        .withColumn("tipo", F.lit("observado"))
    )

    metas_brasil = ler_silver("meta_alfabetizacao_brasil")
    anos_meta = [a for a in range(2024, 2031) if f"meta_alfabetizacao_{a}" in metas_brasil.columns]
    pares_stack = ", ".join(f"'{a}', meta_alfabetizacao_{a}" for a in anos_meta)
    metas_longas = (
        metas_brasil.filter(F.col("rede") == "publica")
        .selectExpr(f"stack({len(anos_meta)}, {pares_stack}) AS (ano, taxa_alfabetizacao)")
        .withColumn("ano", F.col("ano").cast("bigint"))
        .withColumn("tipo", F.lit("meta"))
    )
    return observado.unionByName(metas_longas)

def gold_alunos(df):
    """Desempenho agregado por município/rede/série (atualizado pelo streaming)."""
    log.info("[GOLD] Gerando visao: gold_desempenho_alunos")
    return (
        df.groupBy("ano", "id_municipio", "nome_municipio", "sigla_uf", "rede", "serie", "origem")
        .agg(
            F.countDistinct("id_aluno").alias("n_alunos"),
            F.round(100 * F.avg(F.col("alfabetizado_flag").cast("double")), 2).alias("pct_alfabetizados"),
            F.round(F.avg("proficiencia"), 2).alias("proficiencia_media"),
            F.round(F.avg("peso_aluno"), 2).alias("peso_medio"),
        )
    )

TRANSFORMACOES = {
    "uf": gold_uf,
    "municipio": gold_municipio,
    "brasil": gold_brasil,
    "alunos": gold_alunos,
}

def construir_gold(df_silver):
    log.info("[GOLD] Iniciando transformacao")
    gold_fn = TRANSFORMACOES[ENTIDADE]
    df = gold_fn(df_silver)
    return df.withColumn("_gold_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))

def checar_qualidade(df, checks):
    log.info(f"[DQ:GOLD] Iniciando verificacoes | checks={len(checks)}")
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
            elif tipo == "range":
                mn, mx = valor
                fora   = df.filter((F.col(coluna) < mn) | (F.col(coluna) > mx)).count()
                ok      = fora == 0
                detalhe = f"{fora} fora do intervalo [{mn},{mx}]"
        except Exception as e:
            ok      = False
            detalhe = f"Erro: {e}"

        status = "PASS" if ok else ("FAIL" if critico else "WARN")
        if ok:
            passou += 1
            log.info(f"[DQ:GOLD] {status} | {tipo} | coluna={coluna} | {detalhe}")
        else:
            falhou += 1
            if critico:
                criticos += 1
                log.error(f"[DQ:GOLD] {status} | {tipo} | coluna={coluna} | {detalhe}")
            else:
                log.warning(f"[DQ:GOLD] {status} | {tipo} | coluna={coluna} | {detalhe}")

    score = round(passou / len(checks) * 100, 1)
    log.info(f"[DQ:GOLD] Score={score}% | PASS={passou} FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"[DQ:GOLD] {criticos} check(s) critico(s) falharam. Job interrompido.")

def salvar_gold(df):
    path = f"s3://{BUCKET_SPEC}/gold/{TABELA_GOLD}"
    log.info(f"[GOLD] Salvando em: {path} (particionado por ano do dado)")
    df.write.partitionBy("ano").mode("overwrite").parquet(path)
    log.info(f"[GOLD] {df.count()} registros salvos")
    return path

# ============================================================
# REGRAS DE QUALIDADE
# ============================================================

CHECKS = {
    "uf": [
        {"tipo": "min_count", "valor": 27,                                        "critico": True},
        {"tipo": "not_null",  "coluna": "sigla_uf",                               "critico": True},
        {"tipo": "unique",    "coluna": ["ano", "sigla_uf"],                      "critico": True},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),  "critico": True},
        {"tipo": "range",     "coluna": "ranking_nacional",   "valor": (1, 27),   "critico": True},
    ],
    "municipio": [
        {"tipo": "min_count", "valor": 1000,                                      "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio",                           "critico": True},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),  "critico": True},
        {"tipo": "range",     "coluna": "meta_ano",           "valor": (0, 100),  "critico": False},
    ],
    "brasil": [
        {"tipo": "min_count", "valor": 5,                                         "critico": True},
        {"tipo": "not_null",  "coluna": "ano",                                    "critico": True},
        {"tipo": "not_null",  "coluna": "tipo",                                   "critico": True},
        {"tipo": "unique",    "coluna": ["ano", "tipo"],                          "critico": False},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),  "critico": True},
    ],
    "alunos": [
        {"tipo": "min_count", "valor": 100,                                       "critico": True},
        {"tipo": "not_null",  "coluna": "id_municipio",                           "critico": True},
        {"tipo": "range",     "coluna": "pct_alfabetizados",  "valor": (0, 100),  "critico": True},
        {"tipo": "range",     "coluna": "proficiencia_media", "valor": (300, 1100), "critico": True},
    ],
}

# ============================================================
# EXECUÇÃO
# ============================================================

log.info(f"[GOLD] Lendo Silver de: {SILVER_PATH}")
df_silver = ler_silver(ENTIDADE_SILVER_BASE[ENTIDADE])
log.info(f"[GOLD] {df_silver.count()} registros lidos da Silver")

df_gold = construir_gold(df_silver)
checks  = CHECKS.get(ENTIDADE, [])
if checks:
    checar_qualidade(df_gold, checks)
else:
    log.warning(f"[DQ:GOLD] Nenhuma regra definida para '{ENTIDADE}' — pulando verificacao")

gold_path = salvar_gold(df_gold)

log.info("=" * 60)
log.info("SUMARIO GOLD")
log.info(f"  Lido de  : {SILVER_PATH}")
log.info(f"  Destino  : {gold_path}/ano=<ano_avaliacao>/")
log.info(f"  Pipeline completo para a visao: {TABELA_GOLD}")
log.info("=" * 60)

job.commit()
