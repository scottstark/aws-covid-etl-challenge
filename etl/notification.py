import boto3

from m_logger import get_logger

logger = get_logger(__name__)


def send_notification(msg):
    try:
        ssm = boto3.client('ssm', 'us-east-1')
        parameter = ssm.get_parameter(Name='covid-etl-TopicArn', WithDecryption=False)
        topic_arn = (parameter['Parameter']['Value'])
        sns = boto3.client("sns", "us-east-1")
        sns.publish(TopicArn=topic_arn, Message=msg)
    except Exception as ex:
        logger.error("Error sending notification message! - {}".format(ex))
