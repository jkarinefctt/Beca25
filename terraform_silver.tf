# Configuração do provedor AWS
provider "aws" {
  region = "us-east-1" # Ajuste para sua região
}

###################
# Camada BRONZE  #
###################
# Buckets S3 para armazenar os dados brutos
resource "aws_s3_bucket" "bronze_data" {
  bucket = "elections-bronze-data"
  acl    = "private"
}

# Notificação para novos arquivos no bucket Bronze
resource "aws_s3_bucket_notification" "bronze_notification" {
  bucket = aws_s3_bucket.bronze_data.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.trigger_step_function.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "novo_arquivo/"
  }
}

###################
# Camada SILVER   #
###################
# Buckets S3 para armazenar os dados parcialmente transformados
resource "aws_s3_bucket" "silver_data" {
  bucket = "elections-silver-data"
  acl    = "private"
}

# Glue Job para processar e transformar dados para a camada Silver
resource "aws_glue_job" "silver_transform" {
  name     = "silver_transform_job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://elections-bronze-data/scripts/transform_silver.py"
    name            = "glueetl"
  }

  default_arguments = {
    "--TempDir"               = "s3://elections-silver-data/temp/"
    "--job-language"          = "python"
    "--enable-glue-datacatalog" = "true"  # Ativa o uso do catálogo de dados
    "--job-bookmark-option"   = "job-bookmark-enable" # Habilita bookmarks
  }

  tags = {
    environment = "development"
    layer       = "silver"
  }
}

###################
# Camada GOLD     #
###################
# Buckets S3 para armazenar os dados otimizados para análises (Star Schema)
resource "aws_s3_bucket" "gold_data" {
  bucket = "elections-gold-data"
  acl    = "private"
}

# Glue Job para transformar dados Silver no formato Star Schema
resource "aws_glue_job" "gold_transform" {
  name     = "gold_transform_job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = "s3://elections-silver-data/scripts/transform_gold.py"
    name            = "glueetl"
  }

  default_arguments = {
    "--TempDir"               = "s3://elections-gold-data/temp/"
    "--job-language"          = "python"
    "--enable-glue-datacatalog" = "true"  # Ativa o uso do catálogo de dados
    "--job-bookmark-option"   = "job-bookmark-enable" # Habilita bookmarks
  }


  tags = {
    environment = "production"
    layer       = "gold"
  }
}

###################
# Step Function
###################
# Definição da Step Function para pipeline ETL
resource "aws_sfn_state_machine" "etl_pipeline" {
  name     = "ETLStepFunction"
  role_arn = aws_iam_role.step_function_role.arn

  definition = <<JSON
{
  "Comment": "Step Function para pipeline ETL",
  "StartAt": "SilverTransform",
  "States": {
    "SilverTransform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun",
      "Parameters": {
        "JobName": "silver_transform_job"
      },
      "Next": "GoldTransform"
    },
    "GoldTransform": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun",
      "Parameters": {
        "JobName": "gold_transform_job"
      },
      "End": true
    }
  }
}
JSON
}

###################
# Lambda para acionar a Step Function
###################
resource "aws_lambda_function" "trigger_step_function" {
  filename         = "lambda.zip"
  function_name    = "TriggerStepFunction"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.9"

  environment {
    variables = {
      STEP_FUNCTION_ARN = aws_sfn_state_machine.etl_pipeline.arn
    }
  }
}

###################
# Permissões (IAM)
###################
# Função IAM para Glue
resource "aws_iam_role" "glue_role" {
  name = "GlueETLRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Função IAM para Lambda
resource "aws_iam_role" "lambda_role" {
  name = "LambdaETLTriggerRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_step_function_policy" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
}

# Função IAM para Step Function
resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "step_function_policy" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSStepFunctionsFullAccess"
}


