"""Job AWS Glue — camada SILVER do pipeline de alfabetização (Tech Challenge Fase 2).

Lê a partição de ingestão do dia da Bronze (bucket SOR), aplica limpeza,
padronização, decodificação via dicionário INEP e integração (metas + dimensão
IBGE), e grava no bucket SOT (s3://BUCKET_SOT/silver/ENTIDADE).

As transformações espelham o pipeline validado nos notebooks
(ETL_Pipeline_PySpark/GCP): padroniza_codigos, mapa de rede do dicionário real
do INEP (1=federal, 2=estadual, 3=municipal, 4=privada), proporções na escala
0-1, metas deduplicadas pela aferição mais recente por UF+rede.

Entidades integradas produzidas aqui:
  uf         -> silver/uf (indicador UF + metas UF + descrições do dicionário)
  municipio  -> silver/municipio (indicador municipal + dim IBGE + metas municipais)
  alunos     -> silver/alunos (microdados decodificados + dim IBGE + eventos de streaming, se houver)
  demais     -> limpeza/padronização simples (metas brasil/uf/municipio, dicionario, dim_municipio_ibge)
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
from pyspark.sql.utils import AnalysisException

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
#   --ENTIDADE    uf | municipio | alunos | meta_alfabetizacao_brasil
#                 | meta_alfabetizacao_uf | meta_alfabetizacao_municipio
#                 | dicionario | dim_municipio_ibge
#   --BUCKET_SOR  420411424817-data-sor
#   --BUCKET_SOT  420411424817-data-sot

## @params: [JOB_NAME, ENTIDADE, BUCKET_SOR, BUCKET_SOT]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'ENTIDADE',
    'BUCKET_SOR',
    'BUCKET_SOT',
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
BUCKET_SOR     = args['BUCKET_SOR']
BUCKET_SOT     = args['BUCKET_SOT']
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano_ing, mes_ing, dia_ing = INGESTION_DATE.split("-")

def bronze_path(entidade):
    """Partição de ingestão do dia gerada pelo job Bronze."""
    return (f"s3://{BUCKET_SOR}/bronze/{entidade}/"
            f"ano_ingestao={ano_ing}/mes_ingestao={mes_ing}/dia_ingestao={dia_ing}/")

BRONZE_PATH = bronze_path(ENTIDADE)

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"ENTIDADE  : {ENTIDADE}")
log.info(f"LENDO DE  : {BRONZE_PATH}")
log.info(f"CAMADA    : SILVER -> s3://{BUCKET_SOT}/silver/{ENTIDADE}")
log.info("=" * 60)

# ============================================================
# FUNÇÕES — APOIO
# ============================================================

COLUNAS_PROPORCAO = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
MAPA_REDE_INDICADOR = {  # dicionário INEP (uf/municipio): chave -> rótulo
    "0": "total", "1": "federal", "2": "estadual", "3": "municipal",
    "4": "privada", "5": "publica", "6": "publica_com_federal",
}
MAPA_CODIGOS_ALUNOS = {  # dicionário INEP (alunos)
    "presenca":     {"0": "ausente", "1": "presente"},
    "alfabetizado": {"0": "nao", "1": "sim"},
    "rede":         {"1": "federal", "2": "estadual", "3": "municipal", "4": "privada"},
}

def ler_bronze(entidade):
    """Lê a partição do dia de uma entidade da Bronze, sem colunas técnicas."""
    df = spark.read.parquet(bronze_path(entidade))
    tecnicas = [c for c in df.columns if c.startswith("_") or c.endswith("_ingestao")]
    return df.drop(*tecnicas)

def padroniza_codigos(df, colunas):
    """Trim + minúsculas + sem acentos em colunas de código (rede, serie, presenca...)."""
    for coluna in colunas:
        if coluna in df.columns:
            df = df.withColumn(
                coluna,
                F.translate(
                    F.lower(F.trim(F.col(coluna).cast("string"))),
                    "áàâãäéèêëíìîïóòôõöúùûüç",
                    "aaaaaeeeeiiiiooooouuuuc",
                ),
            )
    return df

def traduz_codigos(df, coluna, mapa):
    """Códigos do dicionário INEP -> rótulos; valores já textuais passam intactos."""
    expr = F.col(coluna)
    for chave, rotulo in mapa.items():
        expr = F.when(F.col(coluna) == chave, rotulo).otherwise(expr)
    return df.withColumn(coluna, expr)

def junta_descricoes_dicionario(df, id_tabela, colunas):
    """LEFT JOIN (broadcast) com o dicionário INEP para criar colunas *_desc."""
    try:
        dicionario = padroniza_codigos(ler_bronze("dicionario"), ["chave", "nome_coluna", "id_tabela"])
    except AnalysisException:
        log.warning("[SILVER] Bronze 'dicionario' indisponivel hoje — descricoes puladas")
        return df
    for coluna in colunas:
        mapa = (
            dicionario.filter((F.col("id_tabela") == id_tabela) & (F.col("nome_coluna") == coluna))
            .select(F.col("chave").alias(coluna), F.col("valor").alias(f"{coluna}_desc"))
        )
        df = df.join(F.broadcast(mapa), on=coluna, how="left")
    return df

def normaliza_proporcoes(df):
    """Dados reais em percentual (0-100) -> fração (0-1); simulador já vem em fração."""
    presentes = [c for c in COLUNAS_PROPORCAO if c in df.columns]
    if not presentes:
        return df
    soma = sum(F.col(c) for c in presentes)
    df = df.withColumn("_soma_bruta", soma)
    for coluna in presentes:
        df = df.withColumn(
            coluna, F.when(F.col("_soma_bruta") > 50, F.col(coluna) / 100).otherwise(F.col(coluna))
        )
    return df.drop("_soma_bruta")

def metas_uf_mais_recentes(metas_uf):
    """Metas reais trazem 1 linha por ano de aferição; mantém a mais recente por UF+rede."""
    if "ano" not in metas_uf.columns:
        return metas_uf
    janela = Window.partitionBy("sigla_uf", "rede").orderBy(F.col("ano").desc())
    return (
        metas_uf.withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

# ============================================================
# FUNÇÕES — TRANSFORMAÇÕES POR ENTIDADE
# ============================================================

def transformar_uf(df):
    log.info("[SILVER] Aplicando regras: uf (indicador + metas UF)")
    df = padroniza_codigos(df, ["rede", "serie"])
    df = (df
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("ano", F.col("ano").cast("bigint"))
    )
    antes = df.count()
    df = df.dropna(subset=["taxa_alfabetizacao"]).filter(~F.isnan("taxa_alfabetizacao"))
    log.info(f"[SILVER] Ausentes removidos (sem taxa): {antes - df.count()}")
    df = df.dropDuplicates(["ano", "sigla_uf", "rede", "serie"])
    df = junta_descricoes_dicionario(df, "uf", ["serie", "rede"])
    df = traduz_codigos(df, "rede", MAPA_REDE_INDICADOR)
    df = normaliza_proporcoes(df)

    metas_uf = padroniza_codigos(ler_bronze("meta_alfabetizacao_uf"), ["rede"])
    metas_uf = metas_uf.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    colunas_meta = ["sigla_uf", "rede", "percentual_participacao"] + [
        f"meta_alfabetizacao_{a}" for a in range(2024, 2031)
    ]
    metas_recentes = metas_uf_mais_recentes(metas_uf)
    integrado = df.join(F.broadcast(metas_recentes.select(*colunas_meta)),
                        on=["sigla_uf", "rede"], how="left")
    assert integrado.count() == df.count(), "join com metas_uf explodiu linhas"
    return integrado

def transformar_municipio(df):
    log.info("[SILVER] Aplicando regras: municipio (indicador + dim IBGE + metas)")
    df = padroniza_codigos(df, ["rede", "serie"])
    df = (df
        .withColumn("id_municipio", F.trim(F.col("id_municipio").cast("string")))
        .withColumn("ano", F.col("ano").cast("bigint"))
    )
    antes = df.count()
    df = df.dropna(subset=["taxa_alfabetizacao"]).filter(~F.isnan("taxa_alfabetizacao"))
    log.info(f"[SILVER] Ausentes removidos (sem taxa): {antes - df.count()}")
    df = df.dropDuplicates(["ano", "id_municipio", "rede", "serie"])
    df = junta_descricoes_dicionario(df, "municipio", ["serie", "rede"])
    df = traduz_codigos(df, "rede", MAPA_REDE_INDICADOR)
    df = normaliza_proporcoes(df)

    dim_ibge = ler_bronze("dim_municipio_ibge")
    metas_municipio = padroniza_codigos(ler_bronze("meta_alfabetizacao_municipio"), ["rede"])
    metas_municipio = metas_municipio.withColumn(
        "id_municipio", F.trim(F.col("id_municipio").cast("string"))
    )
    colunas_agregadas = [c for c in (
        ["percentual_participacao", "nivel_alfabetizacao"]
        + [f"meta_alfabetizacao_{a}" for a in range(2024, 2031)]
    ) if c in metas_municipio.columns]
    metas_agg = metas_municipio.groupBy("id_municipio").agg(
        *[F.avg(c).alias(c) for c in colunas_agregadas]
    )

    integrado = (df
        .join(F.broadcast(dim_ibge), on="id_municipio", how="left")
        .join(F.broadcast(metas_agg), on="id_municipio", how="left")
    )
    assert integrado.count() == df.count(), "join municipal explodiu linhas"
    return integrado

def transformar_alunos(df):
    log.info("[SILVER] Aplicando regras: alunos (microdados + streaming, se houver)")
    colunas_alunos = [
        "ano", "id_aluno", "id_escola", "id_municipio", "rede", "serie", "caderno",
        "presenca", "preenchimento_caderno", "peso_aluno", "proficiencia", "alfabetizado",
    ]
    df = df.select(*[c for c in colunas_alunos if c in df.columns]).withColumn("origem", F.lit("batch"))
    try:  # eventos de streaming (Kinesis/landing) ingeridos na Bronze, quando existirem
        eventos = (ler_bronze("eventos_alunos")
                   .select(*[c for c in colunas_alunos])
                   .withColumn("origem", F.lit("streaming")))
        df = df.unionByName(eventos)
        log.info("[SILVER] Eventos de streaming unificados ao batch")
    except AnalysisException:
        log.info("[SILVER] Sem eventos de streaming na Bronze de hoje — seguindo so com batch")

    df = padroniza_codigos(df, ["rede", "serie", "presenca", "alfabetizado"])
    for coluna, mapa in MAPA_CODIGOS_ALUNOS.items():
        df = traduz_codigos(df, coluna, mapa)
    df = (df
        .withColumn("id_municipio", F.trim(F.col("id_municipio").cast("string")))
        .withColumn("ano", F.col("ano").cast("bigint"))
        .withColumn("proficiencia", F.col("proficiencia").cast("double"))
    )

    antes = df.count()
    df = df.filter(
        (F.col("presenca") == "presente")
        & F.col("proficiencia").isNotNull()
        & ~F.isnan("proficiencia")
    )
    log.info(f"[SILVER] Removidos (ausentes ou sem proficiencia): {antes - df.count()}")

    sem_alfabetizado = df.filter(~F.col("alfabetizado").isin("sim", "nao")).count()
    if sem_alfabetizado:
        log.warning(f"[SILVER] {sem_alfabetizado} presentes sem 'alfabetizado' — removidos")
        df = df.filter(F.col("alfabetizado").isin("sim", "nao"))

    # dedup mantendo a leitura mais recente (streaming sobrepõe batch)
    janela = Window.partitionBy("ano", "id_aluno").orderBy(F.col("origem").desc())
    df = (df
        .withColumn("_rn", F.row_number().over(janela))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    df = df.withColumn("alfabetizado_flag", F.col("alfabetizado") == "sim")

    dim_ibge = ler_bronze("dim_municipio_ibge")
    return df.join(
        F.broadcast(dim_ibge.select("id_municipio", "nome_municipio", "sigla_uf")),
        on="id_municipio", how="left",
    )

def transformar_metas(df):
    log.info("[SILVER] Aplicando regras: metas (padronizacao)")
    df = padroniza_codigos(df, ["rede"])
    if "sigla_uf" in df.columns:
        df = df.withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    if "id_municipio" in df.columns:
        df = df.withColumn("id_municipio", F.trim(F.col("id_municipio").cast("string")))
    return df

def transformar_dicionario(df):
    log.info("[SILVER] Aplicando regras: dicionario")
    return padroniza_codigos(df, ["chave", "nome_coluna", "id_tabela"])

def transformar_dim_ibge(df):
    log.info("[SILVER] Aplicando regras: dim_municipio_ibge")
    return (df
        .withColumn("id_municipio", F.trim(F.col("id_municipio").cast("string")))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .dropDuplicates(["id_municipio"])
    )

TRANSFORMACOES = {
    "uf":                            transformar_uf,
    "municipio":                     transformar_municipio,
    "alunos":                        transformar_alunos,
    "meta_alfabetizacao_brasil":     transformar_metas,
    "meta_alfabetizacao_uf":         transformar_metas,
    "meta_alfabetizacao_municipio":  transformar_metas,
    "dicionario":                    transformar_dicionario,
    "dim_municipio_ibge":            transformar_dim_ibge,
}

def construir_silver(df_bronze):
    log.info("[SILVER] Iniciando transformacao")
    transformar = TRANSFORMACOES.get(ENTIDADE, lambda d: d)
    df = transformar(df_bronze)
    return (df
        .withColumn("_silver_processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("ano_ingestao", F.lit(ano_ing))
        .withColumn("mes_ingestao", F.lit(mes_ing))
        .withColumn("dia_ingestao", F.lit(dia_ing))
    )

def checar_qualidade(df, checks):
    log.info(f"[DQ:SILVER] Iniciando verificacoes | checks={len(checks)}")
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
            elif tipo == "regex":
                invalidos = df.filter(
                    F.col(coluna).isNotNull() & ~F.col(coluna).rlike(valor)
                ).count()
                ok      = invalidos == 0
                detalhe = f"{invalidos} com formato invalido"
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
            log.info(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")
        else:
            falhou += 1
            if critico:
                criticos += 1
                log.error(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")
            else:
                log.warning(f"[DQ:SILVER] {status} | {tipo} | coluna={coluna} | {detalhe}")

    score = round(passou / len(checks) * 100, 1)
    log.info(f"[DQ:SILVER] Score={score}% | PASS={passou} FAIL={falhou}")

    if criticos > 0:
        raise Exception(f"[DQ:SILVER] {criticos} check(s) critico(s) falharam. Job interrompido.")

def salvar_silver(df):
    path = f"s3://{BUCKET_SOT}/silver/{ENTIDADE}"
    log.info(f"[SILVER] Salvando em: {path}")
    df.write.partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao").mode("overwrite").parquet(path)
    log.info(f"[SILVER] {df.count()} registros salvos")
    return path

# ============================================================
# REGRAS DE QUALIDADE
# ============================================================

UF_REGEX = r"^(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)$"

CHECKS = {
    "uf": [
        {"tipo": "min_count", "valor": 50,                                            "critico": True},
        {"tipo": "not_null",  "coluna": "ano",                                        "critico": True},
        {"tipo": "not_null",  "coluna": "taxa_alfabetizacao",                         "critico": True},
        {"tipo": "unique",    "coluna": ["ano", "sigla_uf", "rede", "serie"],         "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf", "valor": UF_REGEX,                "critico": True},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),      "critico": True},
    ],
    "municipio": [
        {"tipo": "min_count", "valor": 1000,                                          "critico": True},
        {"tipo": "not_null",  "coluna": "taxa_alfabetizacao",                         "critico": True},
        {"tipo": "unique",    "coluna": ["ano", "id_municipio", "rede", "serie"],     "critico": True},
        {"tipo": "regex",     "coluna": "id_municipio", "valor": r"^\d{7}$",          "critico": True},
        {"tipo": "range",     "coluna": "taxa_alfabetizacao", "valor": (0, 100),      "critico": True},
        {"tipo": "not_null",  "coluna": "nome_municipio",                             "critico": False},
    ],
    "alunos": [
        {"tipo": "min_count", "valor": 5000,                                          "critico": True},
        {"tipo": "not_null",  "coluna": "proficiencia",                               "critico": True},
        {"tipo": "unique",    "coluna": ["ano", "id_aluno"],                          "critico": True},
        {"tipo": "range",     "coluna": "proficiencia", "valor": (300, 1100),         "critico": True},
        {"tipo": "regex",     "coluna": "alfabetizado", "valor": r"^(sim|nao)$",      "critico": True},
    ],
    "meta_alfabetizacao_brasil": [
        {"tipo": "min_count", "valor": 1,                                             "critico": True},
        {"tipo": "not_null",  "coluna": "rede",                                       "critico": True},
    ],
    "meta_alfabetizacao_uf": [
        {"tipo": "min_count", "valor": 27,                                            "critico": True},
        {"tipo": "regex",     "coluna": "sigla_uf", "valor": UF_REGEX,                "critico": True},
    ],
    "meta_alfabetizacao_municipio": [
        {"tipo": "min_count", "valor": 1000,                                          "critico": True},
        {"tipo": "regex",     "coluna": "id_municipio", "valor": r"^\d{7}$",          "critico": True},
    ],
    "dicionario": [
        {"tipo": "min_count", "valor": 1,                                             "critico": True},
        {"tipo": "not_null",  "coluna": "chave",                                      "critico": True},
    ],
    "dim_municipio_ibge": [
        {"tipo": "min_count", "valor": 5000,                                          "critico": True},
        {"tipo": "unique",    "coluna": "id_municipio",                               "critico": True},
        {"tipo": "regex",     "coluna": "id_municipio", "valor": r"^\d{7}$",          "critico": True},
    ],
}

# ============================================================
# EXECUÇÃO
# ============================================================

log.info(f"[SILVER] Lendo Bronze de: {BRONZE_PATH}")
df_bronze = ler_bronze(ENTIDADE)
log.info(f"[SILVER] {df_bronze.count()} registros lidos da Bronze")

df_silver = construir_silver(df_bronze)
checks    = CHECKS.get(ENTIDADE, [])
if checks:
    checar_qualidade(df_silver, checks)
else:
    log.warning(f"[DQ:SILVER] Nenhuma regra definida para '{ENTIDADE}' — pulando verificacao")

silver_path = salvar_silver(df_silver)

log.info("=" * 60)
log.info("SUMARIO SILVER")
log.info(f"  Lido de  : {BRONZE_PATH}")
log.info(f"  Destino  : {silver_path}/ano_ingestao={ano_ing}/mes_ingestao={mes_ing}/dia_ingestao={dia_ing}/")
log.info(f"  Proxima etapa: executar job etl-gold com BUCKET_SOT={BUCKET_SOT}")
log.info("=" * 60)

job.commit()
