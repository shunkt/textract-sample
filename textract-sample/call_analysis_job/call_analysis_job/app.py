import os
import boto3
from dataclasses import dataclass
from typing import Optional


@dataclass
class S3:
    bucket: str
    key: Optional[str] = None

    @staticmethod
    def from_put_event(event: dict):
        return S3(
            bucket=event["Records"][0]["s3"]["bucket"]["name"],
            key=event["Records"][0]["s3"]["object"]["key"]
        )


@dataclass
class AwsResource:
    arn: str

    @staticmethod
    def from_str(arn: str):
        return AwsResource(arn=arn)


@dataclass
class SnsTopic(AwsResource):
    arn: str

    @staticmethod
    def from_str(arn: str):
        return SnsTopic(arn=arn)


@dataclass
class IamRole(AwsResource):
    arn: str

    @staticmethod
    def from_str(arn: str):
        return IamRole(arn=arn)


class TextractRepository:
    def __init__(self) -> None:
        self.client = boto3.client("textract")

    def start_document_analysis(self, input_s3: S3, output_s3: S3, topic: SnsTopic, role: IamRole) -> str:
        return self.client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": input_s3.bucket,
                                           "Name": self.__validate_file_ext(input_s3).key}},
            FeatureTypes=["FORMS"],
            NotificationChannel={
                "SNSTopicArn": topic.arn, "RoleArn": role.arn},
            OutputConfig={"S3Bucket": output_s3.bucket}
        )["JobId"]

    @staticmethod
    def __validate_file_ext(s3: S3) -> S3:
        return s3


def lambda_handler(event, context):
    """Start analysis job
    Environment Args:
        TOPIC: SNS topic's arn that is pubsished when job finiesd
        ROLE: IAM role's arn deligated to start textract job
        OUTPUT_BUCKET: S3 bucket name to store extracted documents
    """
    topic = os.getenv("TOPIC")
    if topic:
        topic = SnsTopic.from_str(topic)
    else:
        raise Exception()

    role = os.getenv("ROLE")
    if role:
        role = IamRole.from_str(role)
    else:
        raise Exception()

    output_bucket = os.getenv("OUTPUT_BUCKET")
    if output_bucket:
        output_bucket = S3(bucket=output_bucket)
    else:
        raise Exception()

    input_s3 = S3.from_put_event(event)

    textract = TextractRepository()
    job_id = textract.start_document_analysis(
        input_s3=input_s3, output_s3=output_bucket, topic=topic, role=role
    )
    return job_id
