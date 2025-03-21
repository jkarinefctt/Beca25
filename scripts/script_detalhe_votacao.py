import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# Mapeamento para renomear as colunas
mapa_renomeacao = {
    "Ano de eleição": "ano_eleicao",
    "Região": "regiao",
    "Zona": "zona",
    "Cargo": "cargo",
    "Código município": "codigo_municipio",
    "Município": "municipio",
    "Turno": "turno",
    "UF": "uf",
    "Quantidade de votos brancos": "quantidade_votos_brancos",
    "Quantidade de votos de legenda válidos": "quantidade_votos_legenda_validos",
    "Quantidade de comparecimento": "quantidade_comparecimento",
    "Quantidade de votos anulados": "quantidade_votos_anulados",
    "Quantidade de votos nominais válidos": "quantidade_votos_nominais_validos",
    "Quantidade de seções agregadas": "quantidade_secoes_agregadas",
    "Quantidade de votos válidos": "quantidade_votos_validos",
    "Quantidade de aptos totalizados": "quantidade_aptos_totalizados",
    "Quantidade total de seções": "quantidade_total_secoes",
    "Quantidade de votos nulos": "quantidade_votos_nulos",
    "Quantidade de votos totais": "quantidade_votos_totais",
    "Quantidade de aptos": "quantidade_aptos",
    "Quantidade de seções principais": "quantidade_secoes_principais",
    "Quantidade de abstenções": "quantidade_abstencoes",
    "Quantidade total de votos de legenda válidos": "quantidade_total_votos_legenda_validos",
    "Quantidade de votos anulados apurados em separado": "quantidade_votos_anulados_separado",
    "Quantidade de votos nulos técnico": "quantidade_votos_nulos_tecnico",
    "Quantidade total de votos anulados subjudice": "quantidade_total_votos_anulados_subjudice",
    "Quantidade de votos concorrentes": "quantidade_votos_concorrentes",
    "Quantidade total de votos nulos": "quantidade_total_votos_nulos",
    "Quantidade de votos nominais anulados": "quantidade_votos_nominais_anulados",
    "Quantidade de votos de legenda anulados": "quantidade_votos_legenda_anulados",
    "Quantidade de votos de nominais anulados subjudice": "quantidade_votos_nominais_anulados_subjudice",
    "Quantidade de votos de legenda anulados subjudice": "quantidade_votos_legenda_anulados_subjudice",
    "Quantidade de seções não instaladas": "quantidade_secoes_nao_instaladas",
    "Quantidade de seções instaladas": "quantidade_secoes_instaladas",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/detalhe_votacao.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando todas as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("regiao", StringType(), True),
    StructField("zona", IntegerType(), True),
    StructField("cargo", StringType(), True),
    StructField("codigo_municipio", IntegerType(), True),
    StructField("municipio", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("quantidade_votos_brancos", IntegerType(), True),
    StructField("quantidade_votos_legenda_validos", IntegerType(), True),
    StructField("quantidade_comparecimento", IntegerType(), True),
    StructField("quantidade_votos_anulados", IntegerType(), True),
    StructField("quantidade_votos_nominais_validos", IntegerType(), True),
    StructField("quantidade_secoes_agregadas", IntegerType(), True),
    StructField("quantidade_votos_validos", IntegerType(), True),
    StructField("quantidade_aptos_totalizados", IntegerType(), True),
    StructField("quantidade_total_secoes", IntegerType(), True),
    StructField("quantidade_votos_nulos", IntegerType(), True),
    StructField("quantidade_votos_totais", IntegerType(), True),
    StructField("quantidade_aptos", IntegerType(), True),
    StructField("quantidade_secoes_principais", IntegerType(), True),
    StructField("quantidade_abstencoes", IntegerType(), True),
    StructField("quantidade_total_votos_legenda_validos", IntegerType(), True),
    StructField("quantidade_votos_anulados_separado", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_nulos_tecnico", IntegerType(), True),
    StructField("quantidade_total_votos_anulados_subjudice", IntegerType(), True),
    StructField("quantidade_votos_concorrentes", IntegerType(), True),
    StructField("quantidade_total_votos_nulos", IntegerType(), True),
    StructField("quantidade_votos_nominais_anulados", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_legenda_anulados", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_nominais_anulados_subjudice", IntegerType(), True),  # Será removida
    StructField("quantidade_votos_legenda_anulados_subjudice", IntegerType(), True),  # Será removida
    StructField("quantidade_secoes_nao_instaladas", IntegerType(), True),
    StructField("quantidade_secoes_instaladas", IntegerType(), True),
    StructField("data_carga", TimestampType(), True)  # Será removida
])

# Aplicando o esquema ao DataFrame
data_frame = spark.read.csv(
    s3_input_path,
    header=True,
    schema=schema,
    sep=";",
    encoding="UTF-8"
)

# Lista de colunas para remover (já renomeadas)
colunas_para_remover = [
    "quantidade_votos_anulados_separado",
    "quantidade_votos_nominais_anulados",
    "quantidade_votos_legenda_anulados",
    "quantidade_votos_nominais_anulados_subjudice",
    "quantidade_votos_legenda_anulados_subjudice",
    "data_carga"
]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_detalhe_votacao"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
