data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_role" {
  name               = "city-data-project-redshift-role"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
}

data "aws_iam_policy_document" "redshift_access_policy" {
  statement {
    sid    = "S3DataAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::bright-first-s3-bucket-demo-563649768086-us-east-1-an",
      "arn:aws:s3:::bright-first-s3-bucket-demo-563649768086-us-east-1-an/*",
    ]
  }

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "redshift_role_policy" {
  name   = "city-data-project-redshift-role-policy"
  role   = aws_iam_role.redshift_role.id
  policy = data.aws_iam_policy_document.redshift_access_policy.json
}

resource "aws_redshiftserverless_namespace" "ny_taxi" {
  namespace_name       = "ny-taxi-namespace"
  db_name              = "ny_taxi_dw"
  admin_username       = "ny_taxi_admin"
  admin_user_password  = var.redshift_admin_password
  iam_roles            = [aws_iam_role.redshift_role.arn]
  default_iam_role_arn = aws_iam_role.redshift_role.arn
}

resource "aws_redshiftserverless_workgroup" "ny_taxi" {
  namespace_name = aws_redshiftserverless_namespace.ny_taxi.namespace_name
  workgroup_name = "ny-taxi-workgroup"
  base_capacity  = 8 # RPUs; 8 is the minimum
  publicly_accessible = true
}

