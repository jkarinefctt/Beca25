import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/filiado_eleicao.csv"

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("raca_cor", StringType(), True),  # Será removida depois
    StructField("estado_civil", StringType(), True),
    StructField("faixa_etaria", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("interprete_libras", BooleanType(), True),  # Será removida depois
    StructField("municipio", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("quilombola", BooleanType(), True),  # Será removida depois
    StructField("regiao", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("ano_eleicao", IntegerType(), True),
    StructField("grau_escolaridade", StringType(), True)
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
colunas_para_remover = ["raca_cor", "interprete_libras", "quilombola"]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_filiado_eleicao"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
