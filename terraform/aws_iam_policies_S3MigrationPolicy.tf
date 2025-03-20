resource "aws_iam_policy" "S3MigrationPolicy" {
  name        = "S3MigrationPolicy"
  description = "Policy for S3 migration with specific permissions"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:GetObjectVersion",
          "s3:GetObjectVersionTagging"
        ],
        "Resource": [
          "arn:aws:s3:::amazon-s3-demo-source-bucket",
          "arn:aws:s3:::amazon-s3-demo-source-bucket/*"
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "s3:ListBucket",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:PutObjectTagging",
          "s3:GetObjectTagging",
          "s3:GetObjectVersion",
          "s3:GetObjectVersionTagging"
        ],
        "Resource": [
          "arn:aws:s3:::amazon-s3-demo-destination-bucket",
          "arn:aws:s3:::amazon-s3-demo-destination-bucket/*"
        ]
      }
    ]
  })
}
