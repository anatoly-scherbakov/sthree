import operator
import uuid
from pathlib import Path

import boto3
from moto import mock_s3

from sthree import Keys


def bucket() -> str:
    s3 = boto3.resource('s3', region_name='us-east-1')

    bucket_name = f'animals-{uuid.uuid4().hex}'

    bucket = s3.Bucket(bucket_name)
    bucket.create()

    for f in Path('animals').rglob('*.txt'):
        bucket.put_object(
            Bucket=bucket_name,
            Key=str(f),
            Body=f.read_text()
        )

    return bucket_name


@mock_s3
def test_all_keys():
    # This is not a fixture: it must be under the same @mock_s3 environment as
    # the test itself.
    bucket_name = bucket()

    keys = Keys(bucket_name=bucket_name)

    filenames = list(map(
        operator.attrgetter('key'),
        keys
    ))

    assert filenames == [
        'animals/cats/begemot.txt',
        'animals/cats/james.txt',
        'animals/cats/kitty.txt',
        'animals/dogs/rex.txt'
    ]


@mock_s3
def test_keys_dogs_only():
    # This is not a fixture: it must be under the same @mock_s3 environment as
    # the test itself.
    bucket_name = bucket()

    keys = Keys(bucket_name=bucket_name, prefix='animals/dogs')

    filenames = list(map(
        operator.attrgetter('key'),
        keys
    ))

    assert filenames == [
        'animals/dogs/rex.txt'
    ]
