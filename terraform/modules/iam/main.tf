data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-${var.environment}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role" "sfn" {
  name               = "${var.project}-${var.environment}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid     = "S3BucketAccess"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      var.raw_bucket_arn,
      var.scripts_bucket_arn,
      var.dwh_bucket_arn,
      var.archive_bucket_arn,
      var.rejects_bucket_arn,
    ]
  }

  statement {
    sid     = "S3ObjectAccess"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${var.raw_bucket_arn}/*",
      "${var.scripts_bucket_arn}/*",
      "${var.dwh_bucket_arn}/*",
      "${var.archive_bucket_arn}/*",
      "${var.rejects_bucket_arn}/*",
    ]
  }

  statement {
    sid       = "GlueCatalog"
    actions   = ["glue:*"]
    resources = ["*"]
  }

  statement {
    sid     = "CloudWatchLogs"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

resource "aws_iam_policy" "glue_s3" {
  name   = "${var.project}-${var.environment}-glue-s3-policy"
  policy = data.aws_iam_policy_document.glue_s3.json
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = aws_iam_policy.glue_s3.arn
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid     = "GlueJobControl"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
      "glue:StartCrawler",
      "glue:GetCrawler",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "SNSPublish"
    actions   = ["sns:Publish"]
    resources = [var.alert_topic_arn]
  }

  statement {
    sid       = "CloudWatchEvents"
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["*"]
  }

  statement {
    sid       = "XRay"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
    resources = ["*"]
  }

  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutLogEvents",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "sfn" {
  name   = "${var.project}-${var.environment}-sfn-policy"
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_iam_role_policy_attachment" "sfn" {
  role       = aws_iam_role.sfn.name
  policy_arn = aws_iam_policy.sfn.arn
}
