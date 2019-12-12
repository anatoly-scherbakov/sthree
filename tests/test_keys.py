import operator
import uuid
from pathlib import Path

import boto3
from moto import mock_s3

from sthree import Keys


def cats_bucket() -> str:
    s3 = boto3.resource('s3', region_name='us-east-1')

    bucket_name = f'cats-{uuid.uuid4().hex}'

    bucket = s3.Bucket(bucket_name)
    bucket.create()

    for cat in Path('cats_bucket').iterdir():
        bucket.put_object(
            Bucket=bucket_name,
            Key=f'cats/{cat.name}',
            Body=cat.read_text()
        )

    return bucket_name


@mock_s3
def test_keys():
    # This is not a fixture: it must be under the same @mock_s3 environment as
    # the test itself.
    bucket_name = cats_bucket()

    keys = Keys(bucket_name=bucket_name)

    filenames = list(map(
        operator.attrgetter('key'),
        keys
    ))

    assert filenames == [
        'cats/begemot.txt',
        'cats/james.txt',
        'cats/kitty.txt'
    ]
