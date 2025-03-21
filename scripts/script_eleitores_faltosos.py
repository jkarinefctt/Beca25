import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
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
    "Estado civil": "estado_civil",
    "Faixa etária": "faixa_etaria",
    "Região": "regiao",
    "UF": "uf",
    "Grau de instrução": "grau_instrucao",
    "Ano": "ano",
    "Deficiente": "deficiente",
    "Gênero": "genero",
    "Município": "municipio",
    "Nome social": "nome_social",
    "País": "pais",
    "Quantidade de faltosos": "quantidade_faltosos",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/eleitores_faltosos.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando todas as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("estado_civil", StringType(), True),  # Será removida depois
    StructField("faixa_etaria", StringType(), True),
    StructField("regiao", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("grau_instrucao", StringType(), True),
    StructField("ano", IntegerType(), True),
    StructField("deficiente", BooleanType(), True),
    StructField("genero", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("nome_social", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("quantidade_faltosos", IntegerType(), True),
    StructField("data_carga", TimestampType(), True)  # Será removida depois
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
colunas_para_remover = ["estado_civil", "data_carga"]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_eleitores_faltosos"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
