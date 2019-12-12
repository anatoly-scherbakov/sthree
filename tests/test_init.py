import itertools

import pytest

from sthree import Keys


@pytest.mark.skip('Integration test')
def test_initialize():
    piece = itertools.islice(
        Keys(bucket_name='homo-yetiensis', page_size=15),
        10
    )

    for item in piece:
        print(item)
