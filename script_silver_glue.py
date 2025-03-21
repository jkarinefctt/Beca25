import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
from pyspark.sql.functions import col, lower, regexp_replace, lit
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Inicializar o serviço S3
s3_client = boto3.client('s3')

# Configurar os buckets e prefixos
s3_bucket = "elections-bronze-data"
s3_prefix = ""  # Prefixo no bucket de entrada, se houver
output_bucket = "elections-silver-data"
output_prefix = "output/"

# Listar os arquivos no bucket
response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
arquivos = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

# Esquema base esperado
esquema_base = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("cargo", StringType(), True),
    StructField("turno", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("grau_instrucao", StringType(), True),
    StructField("cor_raca", StringType(), True),
    StructField("sigla_partido", StringType(), True),
    StructField("quantidade_votos_validos", IntegerType(), True),
    StructField("quantidade_abtencoes", IntegerType(), True),
    StructField("regiao", StringType(), True),
    StructField("ocupacao", StringType(), True),
    StructField("faixa_etaria", IntegerType(), True),
    StructField("quantidade_faltosos", IntegerType(), True),
])

# Dicionário de colunas a excluir por tabela
colunas_excluir_por_tabela = {
 "candidatos": [
        "etnia_indigena", "identidade_genero", "orientacao_sexual", "quilombola",
        "situacao_de_cassacao", "situacao_desconssituacao_de_desconstituicaotituicao", "situacao_de_julgamento", "data_de_carga",
        "quantidade_de_candidatos_nao_informados"
    ],
    "comparecimento_abstencao": [
        "cor_raca", "identidade_genero", "interprete_libras", "quilombola", "data_de_carga"
    ],
    "deficientes": [
        "detalhe_tipo_deficiencia", "situacao_totalizacao", "data_de_carga"
    ],
    "detalhe_votacao": [
        "quantidade_votos_anulados_separado", "quantidade_votos_nominais_anulados",
        "quantidade_votos_legenda_anulados", "quantidade_votos_nominais_anulados_subjudice",
        "quantidade_votos_legenda_anulados_subjudice", "data_de_carga"
    ],
    "eleitorado_eleicao_capilaridade": [
        "data_de_carga"
    ],
    "eleitores_faltosos": [
        "estado_civil", "data_de_carga"
    ],
    "filiado_eleicao": [
        "raca_cor", "interprete_libras", "quilombola", "data_de_carga"
    ],
    "pesquisas_eleitorais": [
        "codigo_do_registro_no_conre", "dados_municipios", "estatistico_responsavel",
        "metodologia_da_pesquisa", "nome_fantasia_da_empresa", "plano_amostral",
        "sistema_de_controle", "quantidade_de_pesquisa", "valor_da_pesquisa", "data_de_carga"
    ],
    "quocientes_eleitoral": [
        "quantidade_vagas_preenchidas_qp", "quantidade_vagas_obtidas_qp",
        "quantidade_votos_coligacao_qp", "data_de_carga"
    ],
    "vagas": [
        "municipio", "uf", "data_de_eleicao", "data_de_posse", "eleicao", "data_de_carga"
    ],
    "votacao_candidatos": [
        "data_de_carga"
    ],
    "votacao_partida": [
        "data_de_carga"
    ]
}

# Função para ajustar o esquema dinamicamente
def ajustar_esquema(df, esquema_base):
    colunas_atuais = set(df.columns)
    colunas_esperadas = set([campo.name for campo in esquema_base])
    colunas_faltantes = colunas_esperadas - colunas_atuais
    for coluna in colunas_faltantes:
        df = df.withColumn(coluna, lit(None).cast(StringType()))
    df = df.select([coluna.name for coluna in esquema_base])
    return df

# Função para aplicar tratamentos no DataFrame
def tratar_tabela(df):
    df = df.select([lower(col(c)).alias(c.lower().replace(" ", "_").replace("/", "_")) for c in df.columns])
    for c in df.columns:
        df = df.withColumn(
            c, regexp_replace(col(c), r"(?i)#ne|#ne#|#nulo|nulo#|não divulgável", "nao informado")
        )
    return df

# Processar os arquivos no bucket S3
for arquivo in arquivos:
    try:
        # Caminhos de entrada e saída
        s3_input_path = f"s3://{s3_bucket}/{arquivo}"
        tabela_nome = arquivo.split("/")[-1].replace(".csv", "")  # Nome da tabela baseado no arquivo
        s3_output_path = f"s3://{output_bucket}/{output_prefix}{tabela_nome}/"

        # Ler o arquivo CSV
        df = spark.read.csv(s3_input_path, header=True, sep=";")

        # Excluir colunas irrelevantes
        colunas_excluir = colunas_excluir_por_tabela.get(tabela_nome, [])
        for coluna in colunas_excluir:
            if coluna in df.columns:
                df = df.drop(coluna)

        # Tratar e ajustar a tabela
        df_tratada = tratar_tabela(df)
        df_ajustada = ajustar_esquema(df_tratada, esquema_base)

        # Salvar no bucket de saída
        df_ajustada.write.format("csv") \
            .option("header", True) \
            .option("delimiter", ";") \
            .mode("overwrite") \
            .save(s3_output_path)

        print(f"Tabela '{tabela_nome}' processada e salva com sucesso em {s3_output_path}.")
    except Exception as e:
        print(f"Erro ao processar o arquivo '{arquivo}': {e}")

# Finalizar o job
job.commit()
