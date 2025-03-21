import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id
from awsglue.job import Job
import boto3

# Configuração do Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)

# # Carregando os argumentos do job
# args = getResolvedOptions(sys.argv, ["silver_transform_candidato_job"])
# job.init(args['silver_transform_candidato_job'], args)


# Renomeando as colunas de acordo com o mapeamento
mapa_renomeacao = {
    "Cargo": "cargo",
    "Ano de eleição": "ano_eleicao",
    "Coligação": "coligacao",
    "Cor/raça": "cor_raca",
    "Detalhe da situação de candidatura": "detalhe_situacao_candidatura",
    "Estado civil": "estado_civil",
    "Etnia indígena": "etnia_indigena",
    "Faixa etária": "faixa_etaria",
    "Federação": "federacao",
    "Gênero": "genero",
    "Grau de instrução": "grau_instrucao",
    "Identidade de gênero": "identidade_genero",
    "Município": "municipio",
    "Nacionalidade": "nacionalidade",
    "Nome social": "nome_social",
    "Ocupação": "ocupacao",
    "Orientação sexual": "orientacao_sexual",
    "Quilombola": "quilombola",
    "Reeleição": "reeleicao",
    "Região": "regiao",
    "Sigla partido": "sigla_partido",
    "Situação de cadastramento": "situacao_cadastramento",
    "Situação de candidatura": "situacao_candidatura",
    "Situação de cassação": "situacao_cassacao",
    "Situação de desconstituição": "situacao_desconstituicao",
    "Situação de julgamento": "situacao_julgamento",
    "Situação de totalização": "situacao_totalizacao",
    "Tipo eleição": "tipo_eleicao",
    "Turno": "turno",
    "UF": "uf",
    "Quantidade de candidatos": "quantidade_candidatos",
    "Quantidade de candidatos eleitos": "quantidade_candidatos_eleitos",
    "Quantidade de candidatos para o 2º turno": "quantidade_candidatos_2_turno",
    "Quantidade de candidatos não eleitos": "quantidade_candidatos_nao_eleitos",
    "Quantidade de candidatos suplentes": "quantidade_candidatos_suplentes",
    "Quantidade de candidatos não informados": "quantidade_candidatos_nao_informados",
    "Data de carga": "data_carga"
}


# Lendo o arquivo CSV bruto
s3_input_path = "s3://elections-bronze-data/candidatos.csv"
data_frame = spark.read.csv(s3_input_path, header=True, sep=";")

# Renomeando as colunas de acordo com o mapeamento
for nome_antigo, nome_novo in mapa_renomeacao.items():
    if nome_antigo in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(nome_antigo, nome_novo)


# Agora que as colunas foram renomeadas, aplicamos o esquema correspondente
schema = StructType([
    StructField("cargo", StringType(), True),
    StructField("ano_eleicao", IntegerType(), True),
    StructField("coligacao", StringType(), True),
    StructField("cor_raca", StringType(), True),
    StructField("detalhe_situacao_candidatura", StringType(), True),
    StructField("estado_civil", StringType(), True),
    StructField("etnia_indigena", BooleanType(), True),
    StructField("faixa_etaria", StringType(), True),
    StructField("federacao", StringType(), True),
    StructField("genero", StringType(), True),
    StructField("grau_instrucao", StringType(), True),
    StructField("identidade_genero", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("nacionalidade", StringType(), True),
    StructField("nome_social", StringType(), True),
    StructField("ocupacao", StringType(), True),
    StructField("orientacao_sexual", StringType(), True),
    StructField("quilombola", BooleanType(), True),
    StructField("reeleicao", BooleanType(), True),
    StructField("regiao", StringType(), True),
    StructField("sigla_partido", StringType(), True),
    StructField("situacao_cadastramento", StringType(), True),
    StructField("situacao_candidatura", StringType(), True),
    StructField("situacao_cassacao", StringType(), True),
    StructField("situacao_desconstituicao", StringType(), True),
    StructField("situacao_julgamento", StringType(), True),
    StructField("situacao_totalizacao", StringType(), True),
    StructField("tipo_eleicao", StringType(), True),
    StructField("turno", IntegerType(), True),
    StructField("uf", StringType(), True),
    StructField("quantidade_candidatos", IntegerType(), True),
    StructField("quantidade_candidatos_eleitos", IntegerType(), True),
    StructField("quantidade_candidatos_2_turno", IntegerType(), True),
    StructField("quantidade_candidatos_nao_eleitos", IntegerType(), True),
    StructField("quantidade_candidatos_suplentes", IntegerType(), True),
    StructField("quantidade_candidatos_nao_informados", IntegerType(), True),
    StructField("data_carga", TimestampType(), True)
])

# Aplicando o esquema ao DataFrame
# Lendo o arquivo CSV aplicando o esquema diretamente
# Lendo o arquivo CSV aplicando o esquema diretamente e definindo a codificação como UTF-8
data_frame = spark.read.csv(
    s3_input_path,
    header=True,
    schema=schema,
    sep=";",
    encoding="UTF-8"  # Define a codificação do arquivo
)

# Lista de colunas para remover
# Lista de colunas para remover
colunas_para_remover = [
    "etnia_indigena",
    "identidade_genero",
    "orientacao_sexual",
    "quilombola",
    "situacao_cassacao",
    "situacao_desconstituicao",
    "situacao_julgamento",
    "quantidade_candidatos",
    "quantidade_candidatos_2_turno",
    "data_carga"
]
# Verificar quais colunas realmente existem no DataFrame
colunas_existentes = [coluna for coluna in colunas_para_remover if coluna in data_frame.columns]

# Remover as colunas existentes
data_frame = data_frame.drop(*colunas_existentes)

# Removendo dados duplicados
data_frame = data_frame.dropDuplicates()

# Adicionando a coluna id_candidato
data_frame = data_frame.withColumn("id_candidato", monotonically_increasing_id() + 1)

# Reorganizando as colunas para que id_candidato seja a primeira
colunas = ["id_candidato"] + [coluna for coluna in data_frame.columns if coluna != "id_candidato"]
data_frame = data_frame.select(*colunas)

# Substituindo valores nulos por "nulo"
data_frame = data_frame.na.fill("NA")

# Convertendo para DynamicFrame
dyf = DynamicFrame.fromDF(data_frame, glueContext, "dyf")

# Salvando os dados no bucket S3
s3_output_path = "s3://elections-silver-data/output/db_candidatos"

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",  # Pode usar outro formato como "csv" ou "orc", se necessário
    connection_options={"path": s3_output_path},
    transformation_ctx="datasink"
)

# Finalizando o job
job.commit()
