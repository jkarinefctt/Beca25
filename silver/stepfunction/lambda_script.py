import boto3
import json

# Cria um cliente para interagir com o serviço Step Functions da AWS
stepfunctions = boto3.client('stepfunctions')

# Define o manipulador principal da função Lambda
def lambda_handler(event, context):
    # 'event' contém informações sobre o evento que acionou a função Lambda.
    # Aqui, extraímos a lista de registros associados ao evento (ex.: novos arquivos no S3).
    records = event.get("Records", [])
    
    # Itera sobre cada registro presente no evento
    for record in records:
        # Extrai o nome do bucket onde o arquivo foi criado
        bucket_name = record["s3"]["bucket"]["name"]
        # Extrai a chave (nome completo do arquivo no bucket) do objeto criado
        object_key = record["s3"]["object"]["key"]

        # Inicia uma execução da Step Function associada
        response = stepfunctions.start_execution(
            # ARN da Step Function (deve ser substituído pelo valor real no código final) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            stateMachineArn="<ARN_DA_STEP_FUNCTION>", 
            # Converte os detalhes do bucket e do arquivo em um formato JSON como entrada para a Step Function
            input=json.dumps({"bucket": bucket_name, "key": object_key})
        )
        
        # Loga no console da Lambda que a Step Function foi iniciada com sucesso
        print(f"Step Function iniciada: {response['executionArn']}")

