import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from awsglue.utils import getResolvedOptions
import boto3
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define input and output paths
input_path = "s3://julia-elections-bronze-data/"  # Replace with your input S3 bucket and folder
#output_path = "s3://julia-elections-silver-data/output-folder/"  # Replace with your output S3 bucket and folder
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
output_path = f"s3://julia-elections-silver-data/output-folder-{timestamp}/"

# List files in the S3 folder
s3_client = boto3.client('s3')
response = s3_client.list_objects_v2(Bucket="julia-elections-bronze-data")

files = [file['Key'] for file in response.get('Contents', []) if file['Key'].endswith('.csv')]

# Define a mapping for renaming columns
rename_mapping = {"Ano de eleição": "ano_eleicao", "Quantidade de eleitores abstenção": "quantidade_de_eleitores_abstencao"}  # Add your mappings

# Define a dictionary with file-specific columns to drop (colocar nomes antigos)
file_specific_columns_to_drop = {
    "candidatos.csv": ["cor_raca", "identidade_genero", "interprete_libras", "quilombola", "data_carga"],  # Columns to drop for file1.csv
    "comparacimento_abstencao.csv": ["Quilombola"]   
    # Add more file-specific configurations here
}

# Process each file individually
for file in files:
    file_path = f"s3://julia-elections-bronze-data/{file}"
    print(f"Processing file: {file_path}")

    # Read the CSV file with specific encoding and delimiter
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("sep", ";") \
        .option("encoding", "iso-8859-1") \
        .option("mode", "DROPMALFORMED") \
        .option("mode", "PERMISSIVE") \
        .load(file_path)


    # Write the CSV file with new encoding
    df.write \
        .format("csv") \
        .option("header", "true") \
        .option("encoding", "utf-8") \
        .mode("overwrite") \
        .save(f"{output_path}{file.split('/')[-1]}")

    # Read the file into a DynamicFrame
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [file_path]},
        format="csv",
        format_options={"withHeader": True, "separator": ";"}
    )

    # Convert to DataFrame for transformations
    df = datasource.toDF()

    # Get the schema (columns) of the current file
    file_columns = df.columns

    # Dynamically drop columns based on the current file
    file_name = file.split('/')[-1]  # Extract file name
    if file_name in file_specific_columns_to_drop:
        columns_to_drop_in_file = [
            col for col in file_specific_columns_to_drop[file_name] if col in file_columns
        ]
        if columns_to_drop_in_file:
            df = df.drop(*columns_to_drop_in_file)

    # Rename columns dynamically
    for old_col, new_col in rename_mapping.items():
        if old_col in file_columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Drop duplicate rows
    df = df.dropDuplicates()

    # Convert back to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # Write the transformed data back to S3 with the same file name
    output_file_path = f"{output_path}{file_name}"  # Keep the original file name
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_file_path},
        format="parquet"
    )

# Commit the job
job.commit()