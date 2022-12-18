import json

import pytest

from call_analysis_job.call_analysis_job import app


@pytest.fixture()
def s3_put_event():
    with open("") as fp:
        return fp.read()


def test_S3_class(s3_put_event):
    s3 = app.S3.from_put_event(s3_put_event)
    assert s3.bucket == "DOC-EXAMPLE-BUCKET"
    assert s3.key == "example.txt"
