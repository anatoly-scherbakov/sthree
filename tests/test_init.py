import itertools

import pytest

from sthree import Keys


class TestKeyStream(Keys):
    url = 's3://homo-yetiensis'


@pytest.mark.skip('Integration test')
def test_initialize():
    piece = itertools.islice(
        TestKeyStream(),
        10
    )

    for item in piece:
        print(item)
