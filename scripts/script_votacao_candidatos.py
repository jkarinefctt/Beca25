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
    "Cargo": "cargo",
    "Município": "municipio",
    "Turno": "turno",
    "UF": "uf",
    "Código município": "codigo_municipio",
    "Cor/raça": "cor_raca",
    "Estado civil": "estado_civil",
    "Faixa etária": "faixa_etaria",
    "Gênero": "genero",
    "Grau de instrução": "grau_instrucao",
    "Nome candidato": "nome_candidato",
    "Número candidato": "numero_candidato",
    "Ocupação": "ocupacao",
    "Partido": "partido",
    "Situação totalização": "situacao_totalizacao",
    "Zona": "zona",
    "Votos válidos": "votos_validos",
    "Votos nominais": "votos_nominais",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/votacao_candidatos.csv"

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
    StructField("cargo", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("codigo_municipio", IntegerType(), True),
    StructField("cor_raca", StringType(), True),
    StructField("estado_civil", StringType(), True),
    StructField("faixa_etaria", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("grau_instrucao", StringType(), True),
    StructField("nome_candidato", StringType(), True),
    StructField("numero_candidato", IntegerType(), True),
    StructField("ocupacao", StringType(), True),
    StructField("partido", StringType(), True),
    StructField("situacao_totalizacao", StringType(), True),
    StructField("zona", IntegerType(), True),
    StructField("votos_validos", IntegerType(), True),
    StructField("votos_nominais", IntegerType(), True),
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
colunas_para_remover = ["data_carga"]

# Removendo a coluna específica
data_frame = data_frame.drop(*colunas_para_remover)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_votacao_candidatos"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
