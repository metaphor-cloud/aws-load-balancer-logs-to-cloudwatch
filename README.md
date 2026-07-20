> [!IMPORTANT]
> This repo is archived as AWS have added native [CloudWatch integration on Application Load Balancers (ALBs)](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-cloudwatch-logs.html) and [Network Load Balancers (NLBs)](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-cloudwatch-logs.html).

# AWS Load Balancer S3 Logs to CloudWatch Logs
![unit tests](https://github.com/rupertbg/aws-load-balancer-logs-to-cloudwatch/actions/workflows/tests.yml/badge.svg?branch=master)

Latest release [available on ECR Public](https://gallery.ecr.aws/metaphor/awslb2cwlogs):

- `public.ecr.aws/metaphor/awslb2cwlogs:arm64-latest`
- `public.ecr.aws/metaphor/awslb2cwlogs:amd64-latest`


Stream AWS Load Balancer Logs that are delivered to S3 into CloudWatch Logs for use with features like CloudWatch Logs Insights.

![Architecture Diagram](img/arch.png)

Logs are loaded from S3 as they are created using an [S3 Event Notification](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html), which can be optionally buffered via an SQS Queue. The logs are then parsed into JSON and shipped to Cloudwatch Logs.

# Usage
`cfn/example.yml` shows how to deploy the Lambda alongside Classic, Application or Network Load Balancers. Use this template as a starting point for your deployment.

## ECR Public
To access the public ECR image directly in a Lambda you will either need to create a [pull through cache](https://docs.aws.amazon.com/AmazonECR/latest/userguide/pull-through-cache.html) for ECR Public, or use the public image URI as the `FROM` directive in your own private dockerfile.

If you are using a pull through cache, use the following format to reference the image in your Lambda Function, assuming your ECR Public pull through cache was prefixed "ecr-public".

`${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/ecr-public/metaphor/awslb2cwlogs:arm64-latest`

## Environment Variables
The Lambda takes the following environment variables:
  - `LOG_GROUP_NAME`: The name of the Log Group to ship to
  - `LOAD_BALANCER_TYPE`: The load balancer type. Must be `classic`, `application` or `network`
  - `PLAINTEXT_LOGS`: If set to anything will ship the plaintext log line instead of parsing it to JSON

## Identity & Access Management
The Lambda requires access similar to the following IAM Policy:

```json
{
  "Version": "2012-10-17T00:00:00.000Z",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "logs:DescribeLogGroups",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogStreams",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:*:logs:*:REPLACE_WITH_AWS_ACCOUNT_ID:log-group:REPLACE_WITH_LOG_GROUP_NAME"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:*:s3:::REPLACE_WITH_LOG_BUCKET_NAME/*"
    }
  ]
}
```

## Buffering S3 events with an SQS Queue
The Lambda supports invocation via S3 events or SQS events. To use with SQS you will need a Queue, Queue Policy and Event Source similar to the following Cloudformation snippet:

```yaml
LogDeliveryQueue:
  Type: AWS::SQS::Queue
  DeletionPolicy: Delete
  UpdateReplacePolicy: Delete
  Properties:
    QueueName: !Sub ${AWS::StackName}-logs
    VisibilityTimeout: 180
    MessageRetentionPeriod: 345600

SQSQueuePolicy:
  Type: AWS::SQS::QueuePolicy
  Properties:
    Queues:
      - !Ref LogDeliveryQueue
    PolicyDocument:
      Version: 2012-10-17
      Statement:
        - Effect: Allow
          Principal:
            AWS: "*"
          Action: sqs:SendMessage
          Resource: "*"
          Condition:
            ArnLike:
              aws:SourceArn: !Sub arn:${AWS::Partition}:s3:::${AWS::AccountId}-${AWS::StackName}

LoggingSQSEventSource:
  Type: AWS::Lambda::EventSourceMapping
  Properties:
    BatchSize: 10
    Enabled: true
    EventSourceArn: !GetAtt LogDeliveryQueue.Arn
    FunctionName: !Ref LoadBalancerLogsToCloudWatchLambda
    ScalingConfig:
      MaximumConcurrency: 2
    FunctionResponseTypes:
      - ReportBatchItemFailures
```

# Credits
Maintained by [Rupert Bryant-Greene](https://github.com/rupertbg) of [Metaphor](https://github.com/metaphor-cloud)

Originally derived from https://github.com/amazon-archives/cloudwatch-logs-centralize-logs
