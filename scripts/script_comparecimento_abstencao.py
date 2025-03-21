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
    "Cor / Raça": "cor_raca",
    "Estado civil": "estado_civil",
    "Faixa etária": "faixa_etaria",
    "Gênero": "genero",
    "Grau de instrução": "grau_instrucao",
    "Identidade de gênero": "identidade_genero",
    "Intérprete de libras": "interprete_libras",
    "Município": "municipio",
    "Nome social": "nome_social",
    "País": "pais",
    "Quilombola": "quilombola",
    "Região": "regiao",
    "Turno": "turno",
    "UF": "UF",
    "Zona": "zona",
    "Quantidade de eleitores deficientes abstenção": "quant_eleitores_deficientes_abstencao",
    "Quantidade de eleitores comparecimento TTE": "quant_eleitores_comparecimento_TTE",
    "Quantidade de eleitores abstenção TTE": "quant_eleitores_eleitores_abstencao_TTE",
    "Quantidade de eleitores aptos": "quant_eleitores_aptos",
    "Quantidade de eleitores comparecimento": "quant_eleitores_comparecimento",
    "Quantidade de eleitores abstenção": "quant_eleitores_abstencao",
    "Quantidade de eleitores deficientes comparecimento": "quant_eleitores_deficientes_comparecimento",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/comparecimento_abstencao.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando todas as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("ano_eleicao", IntegerType(), True),
    StructField("cor_raca", StringType(), True),  # Será removida depois
    StructField("estado_civil", StringType(), True),
    StructField("faixa_etaria", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("grau_instrucao", StringType(), True),
    StructField("identidade_genero", StringType(), True),  # Será removida depois
    StructField("interprete_libras", StringType(), True),  # Será removida depois
    StructField("municipio", StringType(), True),
    StructField("nome_social", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("quilombola", StringType(), True),  # Será removida depois
    StructField("regiao", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("UF", StringType(), True),
    StructField("zona", IntegerType(), True),
    StructField("quant_eleitores_deficientes_abstencao", IntegerType(), True),
    StructField("quant_eleitores_comparecimento_TTE", IntegerType(), True),
    StructField("quant_eleitores_eleitores_abstencao_TTE", IntegerType(), True),
    StructField("quant_eleitores_aptos", IntegerType(), True),
    StructField("quant_eleitores_comparecimento", IntegerType(), True),
    StructField("quant_eleitores_abstencao", IntegerType(), True),
    StructField("quant_eleitores_deficientes_comparecimento", IntegerType(), True),
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
colunas_para_remover = ["cor_raca", "identidade_genero", "interprete_libras", "quilombola", "data_carga"]

# Removendo as colunas específicas
data_frame = data_frame.drop(*colunas_para_remover)

# Substituindo valores nulos por "NA"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_comparecimento_abstencao"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
