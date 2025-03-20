# Map com nomes e scripts para os jobs
locals {
  jobs = {
    candidato = "s3://elections-bronze-data/scripts/script_candidato.py"
    comparecimento_abstencao = "s3://elections-bronze-data/scripts/script_comparecimento_abstencao.py"
  }
}

# Criação dinâmica de jobs
resource "aws_glue_job" "silver_transform" {
  for_each = local.jobs

  name     = "silver_transform_${each.key}_job"
  role_arn = aws_iam_role.glue_role.arn

  command {
    script_location = each.value
    name            = "glueetl"
  }

  default_arguments = {
    "--TempDir"               = "s3://elections-silver-data/temp/"
    "--job-language"          = "python"
    "--enable-glue-datacatalog" = "true"
    "--job-bookmark-option"   = "job-bookmark-enable"
  }

  tags = {
    environment = "development"
    layer       = "silver"
    table       = each.key
  }
}
