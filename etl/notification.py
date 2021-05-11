import boto3


def send_notification(msg):
    ssm = boto3.client('ssm', 'us-east-1')
    parameter = ssm.get_parameter(Name='covid-etl-TopicArn', WithDecryption=False)
    topic_arn = (parameter['Parameter']['Value'])
    sns = boto3.client("sns", "us-east-1")
    sns.publish(TopicArn=topic_arn, Message=msg)
