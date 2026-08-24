# SPDX-License-Identifier: Apache-2.0
"""fsspec's own abstract suite, against FlintS3FileSystem.

The Hadoop contract suite found six real defects in JVM code that every suite
we wrote had passed. The Python path has only ever been tested by suites we
wrote, which is precisely the condition that turned out to be inadequate.

fsspec's suite is less targeted than Hadoop's -- most of it exercises
put/get/copy rather than the read path we override -- but get and copy pull
every byte through _fetch_range, which IS our seam. That is 20-odd tests
written by someone whose model of a filesystem is not ours.
"""

from __future__ import annotations

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(__file__))
import flint_accel  # noqa: E402

from fsspec.tests.abstract import (  # noqa: E402
    AbstractFixtures,
    AbstractCopyTests,
    AbstractGetTests,
    AbstractOpenTests,
)

# The origin here is moto, not tools/counting_s3.py, and that split is
# deliberate. counting_s3 exists to COUNT -- it answers "how many origin
# requests did this cost", which is the product's economic claim. It is a
# minimal S3 and cannot serve DeleteObjects, CopyObject or directory-shaped
# listings, so running a correctness suite against it measures the fixture.
# The first attempt did exactly that: 47 failed, 90 errors, and the first one
# examined was NotImplemented on DeleteObjects.
#
# So: counting_s3 for economics, moto for correctness. Each instrument used
# for the question it can answer.
ENDPOINT = os.environ.get("FLINT_TEST_ENDPOINT", "http://127.0.0.1:9810")
TIER = os.environ.get("FLINT_TEST_TIER", "redis://127.0.0.1:6399")
BUCKET = os.environ.get("FLINT_TEST_BUCKET", "bucket")


@pytest.fixture(scope="session", autouse=True)
def _bucket():
    """moto starts empty; s3fs needs the bucket to exist."""
    import boto3
    c = boto3.client("s3", endpoint_url=ENDPOINT,
                     aws_access_key_id="t", aws_secret_access_key="t",
                     region_name="us-east-1")
    try:
        c.create_bucket(Bucket=BUCKET)
    except Exception:
        pass
    return BUCKET


class FlintFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self):
        return flint_accel.FlintS3FileSystem(
            skip_instance_cache=True,
            anon=False, key="t", secret="t",
            client_kwargs={"endpoint_url": ENDPOINT, "region_name": "us-east-1"},
            tier_uri=TIER,
        )

    @pytest.fixture
    def fs_path(self, fs):
        # A fresh prefix per test: object stores have no directories, and
        # leftovers between tests read as defects in whatever runs next.
        p = f"{BUCKET}/fsspec-{uuid.uuid4().hex[:10]}"
        fs.mkdirs(p, exist_ok=True)
        return p

    @pytest.fixture
    def supports_empty_directories(self):
        # S3 has no directories. Saying otherwise would turn inherited tests
        # into false failures -- the same trap as the Hadoop contract XML.
        return False


class TestFlintGet(FlintFixtures, AbstractGetTests):
    """14 download tests; every byte crosses _fetch_range."""


class TestFlintCopy(FlintFixtures, AbstractCopyTests):
    """Server-side and read-through copies."""


class TestFlintOpen(FlintFixtures, AbstractOpenTests):
    """The open path itself.

    test_open_exclusive is deselected in the gate, not because it is
    inconvenient but because BASELINE s3fs fails it identically against the
    same moto: exclusive-create is not implemented in that stack, with Flint
    nowhere in the picture. Verified by running the same inherited test class
    against a plain S3FileSystem. Without that control it would have looked
    like our defect, and the honest record is that we checked rather than
    assumed.
    """
