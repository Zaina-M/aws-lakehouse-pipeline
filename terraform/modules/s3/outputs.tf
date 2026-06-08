output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "scripts_bucket_name" {
  value = aws_s3_bucket.scripts.bucket
}

output "scripts_bucket_arn" {
  value = aws_s3_bucket.scripts.arn
}

output "dwh_bucket_name" {
  value = aws_s3_bucket.dwh.bucket
}

output "dwh_bucket_arn" {
  value = aws_s3_bucket.dwh.arn
}

output "archive_bucket_name" {
  value = aws_s3_bucket.archive.bucket
}

output "archive_bucket_arn" {
  value = aws_s3_bucket.archive.arn
}

output "rejects_bucket_name" {
  value = aws_s3_bucket.rejects.bucket
}

output "rejects_bucket_arn" {
  value = aws_s3_bucket.rejects.arn
}
