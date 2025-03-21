resource "aws_iam_role" "glue_access_role" {
  name = "GlueAccessRole"

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

resource "aws_iam_policy" "glue_access_policy" {
  name        = "GlueAccessPolicy"
  description = "Policy to allow Glue access"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "s3:GetObject",
          "s3:PutObject",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "s3:ListBucket"  # Permissão ampla adicionada
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_access_attachment" {
  role       = aws_iam_role.glue_access_role.name
  policy_arn = aws_iam_policy.glue_access_policy.arn
}
