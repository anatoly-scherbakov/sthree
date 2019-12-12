import dataclasses

import datetime
from enum import Enum
from typing import NewType

ETag = NewType('ETag', str)


class StorageClass(str, Enum):
    STANDARD = 'STANDARD'


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
