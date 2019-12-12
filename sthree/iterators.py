from typing import Iterator, Optional
from urllib.parse import urlparse

import boto3
from boto3_type_annotations import s3
from botocore.paginate import PageIterator
from typing import Iterable

from . import models


class S3RecursiveKeyStream(Iterable[models.Key]):
    url: Optional[str] = None

    def __init__(self, url: Optional[str] = None):
        if url is not None:
            self.url = url

        if self.url is None:
            raise ValueError(f'{self} does not have `url` defined.')

    def _recurse(self) -> Iterator[models.Key]:
        """Stream of all file URLs on Data Lake S3 bucket."""

        client: s3.Client = boto3.client('s3')

        decoded_url = urlparse(self.url)
        bucket_name = decoded_url.netloc

        paginator = client.get_paginator('list_objects_v2')

        page_iterator: PageIterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=decoded_url.path.lstrip('/'),
        )

        for page in page_iterator:
            records = page.get('Contents', [])

            record: dict
            for record in records:
                yield models.Key(
                    key=record['Key'],
                    last_modified=record['LastModified'],
                    size=record['Size'],
                    e_tag=models.ETag(record['ETag'].strip('"')),
                    storage_class=models.StorageClass(
                        record['StorageClass']
                    )
                )

    def __iter__(self):
        return self._recurse()
