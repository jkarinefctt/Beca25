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
    "Município": "municipio",
    "País": "pais",
    "Região": "regiao",
    "UF": "uf",
    "Local de votação": "local_votacao",
    "Zona": "zona",
    "Quantidade de município": "quantidade_municipio",
    "Quantidade de zona": "quantidade_zona",
    "Quantidade de local de votação": "quantidade_local_votacao",
    "Quantidade de seção": "quantidade_secao",
    "Quantidade de aptos": "quantidade_aptos",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/eleitorado_eleicao_capilaridade.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando todas as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("municipio", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("regiao", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("local_votacao", StringType(), True),
    StructField("zona", IntegerType(), True),
    StructField("quantidade_municipio", IntegerType(), True),
    StructField("quantidade_zona", IntegerType(), True),
    StructField("quantidade_local_votacao", IntegerType(), True),
    StructField("quantidade_secao", IntegerType(), True),
    StructField("quantidade_aptos", IntegerType(), True),
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
colunas_para_remover = ["data_carga"]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_eleitorado_eleicao_capilaridade"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
