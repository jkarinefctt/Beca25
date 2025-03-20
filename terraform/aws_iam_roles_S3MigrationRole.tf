resource "aws_iam_role" "S3MigrationRole" {
  name = "S3MigrationRole"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "AWS": "arn:aws:iam::008376430615:root"
        },
        "Action": "sts:AssumeRole",
        "Condition": {}
      }
    ]
  })
}

