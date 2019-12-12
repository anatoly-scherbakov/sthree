import dataclasses

import datetime
from enum import Enum
from typing import NewType

ETag = NewType('ETag', str)


class StorageClass(str, Enum):
    """
    AWS S3 Storage classes

    https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html
    """

    STANDARD = 'STANDARD'
    REDUCED_REDUNDANCY = 'REDUCED_REDUNDANCY'
    INTELLIGENT_TIERING = 'INTELLIGENT_TIERING'
    STANDARD_IA = 'STANDARD_IA'
    ONEZONE_IA = 'ONEZONE_IA'

    GLACIER = 'GLACIER'
    DEEP_ARCHIVE = 'DEEP_ARCHIVE'


@dataclasses.dataclass(frozen=True)
class Key:
    key: str
    last_modified: datetime
    e_tag: ETag
    size: int
    storage_class: StorageClass

    @property
    def is_pseudo_directory(self):
        return self.key.endswith('/') and self.size == 0
