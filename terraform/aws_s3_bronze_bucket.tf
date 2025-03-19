resource "aws_s3_bucket" "my_bucket" {
  bucket = "elections-bronze-data"  # Replace with your desired bucket name
  acl    = "private"
}

resource "aws_s3_bucket_object" "files" {
  for_each = fileset("C:/Users/jkarinef/Beca25/extracted_files", "**")  # Replace with the path to your local folder

  bucket = aws_s3_bucket.my_bucket.id
  key    = each.value
  source = "C:/Users/jkarinef/Beca25/extracted_files/${each.value}"  # Replace with the path to your local folder
}
