# Terraform script to provision Trino, S3, and Airflow
provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "telemetry_lakehouse" {
  bucket = "telemetry-lakehouse-data"
  force_destroy = true
}

resource "aws_iam_role" "trino_role" {
  name = "TrinoS3AccessRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy_attachment" "s3_access_attach" {
  name       = "AttachS3AccessToTrino"
  roles      = [aws_iam_role.trino_role.name]
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
