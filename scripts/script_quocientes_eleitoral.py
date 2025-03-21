import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType
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
    "Cargo": "cargo",
    "Município": "municipio",
    "Turno": "turno",
    "UF": "uf",
    "Coligação": "coligacao",
    "Quantidade de vagas QE": "qtd_vagas_qe",
    "Quantidade de votos de legenda QE": "qtd_votos_legenda_qe",
    "Quantidade de votos nominais QE": "qtd_votos_nominais_qe",
    "Quantidade de votos válidos QE": "qtd_votos_validos_qe",
    "Valor do quociente eleitoral": "valor_qe",
    "Quantidade de votos de legenda QP": "qtd_votos_legenda_qp",
    "Quantidade de votos nominais QP": "qtd_votos_nominais_qp",
    "Quantidade de vagas preenchidas QP": "qtd_vagas_preenchidas_qp",
    "Quantidade de vagas média QP": "qtd_vagas_media_qp",
    "Quantidade de vagas obtidas QP": "qtd_vagas_obtidas_qp",
    "Quantidade votos coligação QP": "qtd_votos_coligacao_qp",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/quociente-eleitoral.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("regiao", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("coligacao", StringType(), True),
    StructField("qtd_vagas_qe", IntegerType(), True),
    StructField("qtd_votos_legenda_qe", IntegerType(), True),
    StructField("qtd_votos_nominais_qe", IntegerType(), True),
    StructField("qtd_votos_validos_qe", IntegerType(), True),
    StructField("valor_qe", FloatType(), True),
    StructField("qtd_votos_legenda_qp", IntegerType(), True),
    StructField("qtd_votos_nominais_qp", IntegerType(), True),
    StructField("qtd_vagas_preenchidas_qp", IntegerType(), True),  # Será removida
    StructField("qtd_vagas_media_qp", IntegerType(), True),
    StructField("qtd_vagas_obtidas_qp", IntegerType(), True),  # Será removida
    StructField("qtd_votos_coligacao_qp", IntegerType(), True),  # Será removida
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
    "qtd_vagas_preenchidas_qp",
    "qtd_vagas_obtidas_qp",
    "qtd_votos_coligacao_qp",
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
s3_output_path = "s3://elections-silver-data/output/db_quociente_eleitoral"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
