from typing import Iterator, Optional
from urllib.parse import urlparse

import boto3
from boto3_type_annotations import s3
from botocore.paginate import PageIterator
from typing import Iterable

from . import models


class Keys(Iterable[models.Key]):
    url: Optional[str] = None
    include_pseudo_directories = False

    def __init__(
            self,
            url: Optional[str] = None,
            include_pseudo_directories: bool = None
    ):
        if url is not None:
            self.url = url

        if include_pseudo_directories is not None:
            self.include_pseudo_directories = include_pseudo_directories

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
                key = models.Key(
                    key=record['Key'],
                    last_modified=record['LastModified'],
                    size=record['Size'],
                    e_tag=models.ETag(record['ETag'].strip('"')),
                    storage_class=models.StorageClass(
                        record['StorageClass']
                    )
                )

                if (
                    self.include_pseudo_directories
                    or not key.is_pseudo_directory
                ):
                    yield key

    def __iter__(self):
        return self._recurse()
