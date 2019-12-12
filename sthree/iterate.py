import dataclasses
from typing import Iterable, Iterator, Union, Optional

import boto3
from boto3_type_annotations import s3
from boto3_type_annotations.s3.paginator import ListObjectsV2

from . import models


@dataclasses.dataclass(eq=False, order=False, frozen=True)
class Keys(Iterable[models.Key]):
    """
    Iterate over keys in an Amazon S3 bucket. The bucket is treated as a flat
    storage, meaning its keys are returned recursively. Pseudo-directories are
    by default excluded from the output.

    ### Usage
    ```python
    Keys(bucket_name='my-bucket', prefix='my-files/2019/')
    ```

    ### Ordering
    Keys are sorted in Unicode binary order, which is not exactly equivalent
    to lexicographic sorting. For example, lexicographically, 'B' > 'a',
    but this iterator will return 'Boo.txt' before 'alpha.csv'.

    """

    bucket_name: Union[str, bytes]
    prefix: str = ''

    page_size: int = 1000
    include_pseudo_directories: bool = False

    aws_region: Optional[str] = None

    def _recurse(self) -> Iterator[models.Key]:
        """Stream of all file URLs on Data Lake S3 bucket."""

        client: s3.Client = boto3.client('s3', region_name=self.aws_region)

        # noinspection PyTypeChecker
        paginator: ListObjectsV2 = client.get_paginator('list_objects_v2')

        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            PaginationConfig={
                'PageSize': self.page_size
            }
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

