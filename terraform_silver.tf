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


