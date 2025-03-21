import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
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
    "UF": "uf",
    "Ano de eleição": "ano_eleicao",
    "Município": "municipio",
    "Protocolo de registro": "protocolo_registro",
    "Região": "regiao",
    "Código do registro no CONRE": "codigo_registro_conre",
    "Dados municípios": "dados_municipios",
    "Data de fim da pesquisa": "data_fim_pesquisa",
    "Data de início da pesquisa": "data_inicio_pesquisa",
    "Data de registro": "data_registro",
    "Estatístico responsável": "estatistico_responsavel",
    "Metodologia da pesquisa": "metodologia_pesquisa",
    "Nome fantasia da empresa": "nome_fantasia_empresa",
    "Plano amostral": "plano_amostral",
    "Sistema de controle": "sistema_controle",
    "Quantidade de pesquisa": "quantidade_pesquisa",
    "Quantidade de entrevistados": "quantidade_entrevistados",
    "Valor da pesquisa": "valor_pesquisa",
    "Data de carga": "data_carga"
}

# Caminho para o arquivo de entrada no S3
s3_input_path = "s3://elections-bronze-data/pesquisas_eleitorais.csv"

# Lendo o arquivo CSV bruto
data_frame = spark.read.csv(s3_input_path, header=True, sep=";", encoding="UTF-8")

# Renomeando as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)

# Esquema completo, incluindo colunas que serão removidas
schema = StructType([
    StructField("uf", StringType(), True),
    StructField("ano_eleicao", IntegerType(), True),
    StructField("municipio", StringType(), True),
    StructField("protocolo_registro", StringType(), True),
    StructField("regiao", StringType(), True),
    StructField("codigo_registro_conre", StringType(), True),  # Será removida
    StructField("dados_municipios", StringType(), True),  # Será removida
    StructField("data_fim_pesquisa", DateType(), True),
    StructField("data_inicio_pesquisa", DateType(), True),
    StructField("data_registro", DateType(), True),
    StructField("estatistico_responsavel", StringType(), True),  # Será removida
    StructField("metodologia_pesquisa", StringType(), True),  # Será removida
    StructField("nome_fantasia_empresa", StringType(), True),  # Será removida
    StructField("plano_amostral", StringType(), True),  # Será removida
    StructField("sistema_controle", StringType(), True),  # Será removida
    StructField("quantidade_pesquisa", IntegerType(), True),  # Será removida
    StructField("quantidade_entrevistados", IntegerType(), True),
    StructField("valor_pesquisa", StringType(), True),  # Será removida
    StructField("data_carga", DateType(), True)  # Será removida
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
    "codigo_registro_conre",
    "dados_municipios",
    "estatistico_responsavel",
    "metodologia_pesquisa",
    "nome_fantasia_empresa",
    "plano_amostral",
    "sistema_controle",
    "quantidade_pesquisa",
    "valor_pesquisa",
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
s3_output_path = "s3://elections-silver-data/output/db_pesquisas_eleitorais"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato, como "csv" ou "orc", se preferir
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
