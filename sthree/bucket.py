import dataclasses
from . import models
from .iterate import Keys


@dataclasses.dataclass(order=False, frozen=True)
class Bucket:
    name: str

    def keys(self):
        return Keys(bucket_name=self.name)
