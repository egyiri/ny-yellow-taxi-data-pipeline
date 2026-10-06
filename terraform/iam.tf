resource "aws_iam_role" "glue_role" {
  name               = "city-data-project-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_base_policy.json
}

resource "aws_iam_role_policy" "task_role_policy" {
  name   = "city-data-project-glue-role-policy"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_access_policy.json
}

data "aws_iam_policy_document" "glue_base_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_access_policy" {
  statement {
  sid    = "S3DataAccess"
  effect = "Allow"
  actions = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
  ]
  resources = [
    "arn:aws:s3:::bright-first-s3-bucket-demo-563649768086-us-east-1-an",
    "arn:aws:s3:::bright-first-s3-bucket-demo-563649768086-us-east-1-an/*",
    "arn:aws:s3:::scripts-bucket-563649768086-us-east-1-an",
    "arn:aws:s3:::scripts-bucket-563649768086-us-east-1-an/*",
  ]
}

  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:UpdateTable",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }

  statement {
  sid    = "GlueDataQualityAccess"
  effect = "Allow"
  actions = [
    "glue:StartDataQualityRulesetEvaluationRun",
    "glue:GetDataQualityRulesetEvaluationRun",
    "glue:CancelDataQualityRulesetEvaluationRun",
    "glue:ListDataQualityRulesetEvaluationRuns",
    "glue:GetDataQualityResult",
    "glue:BatchGetDataQualityResult",
    "glue:ListDataQualityResults",
    "glue:GetDataQualityRuleset",
    "glue:ListDataQualityRulesets",
    "glue:CreateDataQualityRuleset",
    "glue:UpdateDataQualityRuleset",
    "glue:PublishDataQuality",
    "glue:StartDataQualityRuleRecommendationRun",
    "glue:GetDataQualityRuleRecommendationRun",
  ]
  resources = ["*"]
}

  statement {
  sid       = "GlueDQAssetsAccess"
  effect    = "Allow"
  actions   = ["s3:GetObject"]
  resources = ["arn:aws:s3:::aws-glue-ml-data-quality-assets-us-east-1/*"]
}
  

}



