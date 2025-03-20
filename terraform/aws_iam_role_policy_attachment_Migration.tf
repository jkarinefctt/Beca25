resource "aws_iam_role_policy_attachment" "S3MigrationPolicyAttachment" {
  role       = aws_iam_role.S3MigrationRole.name
  policy_arn = aws_iam_policy.S3MigrationPolicy.arn
}
