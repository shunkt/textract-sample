import os
import json
import boto3
from logging import getLogger
from pathlib import Path
Id = str


class TextractRepository:
    def __init__(self) -> None:
        self.__client = boto3.client("textract")

    def get_document(self, job_id: str) -> dict:
        ret = self.__client.get_document_analysis(JobId=job_id)
        if ret["JobStatus"] == "SUCCEEDED":
            return ret
        else:
            raise Exception(ret)


class S3Repository:
    def __init__(self) -> None:
        self.__client = boto3.client("s3")

    def save(self, bucket: str, key: str, content: str):
        body = content.encode("utf-8")
        self.__client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body
        )


class HandleKV:
    def __init__(self) -> None:
        self.__obj = {}
        self.__keys = []

    def parse(self, data: dict):
        for block in data["Blocks"]:
            self.__obj[block["Id"]] = block
            if block["BlockType"] == "KEY_VALUE_SET" and "KEY" in block["EntityTypes"]:
                self.__keys.append(block["Id"])

    def find_kvs(self):
        for kv in self.__keys:
            temp_k = []
            temp_v = []
            for rel in self.__obj[kv].get("Relationships", []):
                if rel["Type"] == "VALUE":
                    for id in rel["Ids"]:
                        temp_v.extend([self.__obj[i]
                                      for i in self.__find_root(id)])
                if rel["Type"] == "CHILD":
                    for id in rel["Ids"]:
                        temp_k.extend(self.__obj[i]
                                      for i in self.__find_root(id))
            yield (temp_k, temp_v, self.__obj[kv]["Confidence"])

    def get_kvs_dict(self):
        kvs = []
        for key, val, conf in self.find_kvs():
            kvs.append({
                "key": " ".join([k["Text"] for k in key]),
                "value": " ".join([v["Text"] for v in val]),
                "confidence": conf
            })
        return {"data": kvs}

    def __find_root(self, id: Id) -> list[Id]:
        ans = []
        stack = [id]
        while stack:
            tmp = stack.pop()
            if self.__obj[tmp]["BlockType"] == "WORD":
                ans.append(tmp)
            else:
                for rel in self.__obj[tmp].get("Relationships", []):
                    if rel["Type"] == "CHILD":
                        for _id in rel.get("Ids", []):
                            stack.append(_id)

        return ans

    @property
    def kvs(self):
        return self.__keys


logger = getLogger(__name__)


def lambda_handler(event, context):

    OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")
    if not OUTPUT_BUCKET:
        raise Exception("Not find OUTPUT_BUCKET env. var.")

    textract_repo = TextractRepository()
    s3_repo = S3Repository()

    for record in event["Records"]:
        body = json.loads(record["body"])
        if body["Status"] == "SUCCEEDED":
            doc = textract_repo.get_document(body["JobId"])
            kv = HandleKV()
            kv.parse(doc)

            p = Path(body["DocumentLocation"]["S3ObjectName"])
            print(kv.get_kvs_dict())
            s3_repo.save(
                OUTPUT_BUCKET, f"{p.stem}.kv.json", json.dumps(kv.get_kvs_dict()))
            logger.info(f"{p.name} was processed.")
        else:
            logger.error(body)
