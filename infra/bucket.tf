resource "aws_s3_bucket" "bucket" {
  bucket = "mysparkbucketayman"

  tags = local.tags
}