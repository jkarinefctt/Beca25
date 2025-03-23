resource "aws_iam_role_policy_attachment" "athena_policy_attach" {
  role       = aws_iam_role.athena_role.name
  policy_arn = aws_iam_policy.athena_glue_policy.arn
}