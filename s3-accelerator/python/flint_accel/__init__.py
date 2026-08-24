# SPDX-License-Identifier: Apache-2.0
"""Flint accelerator for fsspec / s3fs.

    import flint_accel
    flint_accel.install(tier_uri="redis://flint:6379")
"""

from __future__ import annotations

import fsspec

from .fs import FlintS3FileSystem, FlintS3File
from .tier import FlintTier, CHUNK

__all__ = ["install", "uninstall", "FlintS3FileSystem", "FlintS3File", "FlintTier"]

_ORIGINALS = {}


def install(protocols=("s3", "s3a"), **defaults):
    """Route existing ``s3://`` paths through the tier. Changes no paths.

    Re-registering the protocol rather than adding a new one is deliberate:
    a scheme the user has to adopt in every path is a migration, not a cache
    (ADR-0023 D1).
    """
    import s3fs
    for p in protocols:
        try:
            _ORIGINALS.setdefault(p, fsspec.get_filesystem_class(p))
        except (ImportError, ValueError):
            _ORIGINALS.setdefault(p, s3fs.S3FileSystem)
        fsspec.register_implementation(p, FlintS3FileSystem, clobber=True)
    # Read by FlintS3FileSystem.__init__. Assigned unconditionally: a second
    # install() with no defaults must CLEAR the first one's, or settings
    # outlive the call that made them.
    FlintS3FileSystem._flint_defaults = dict(defaults)
    return FlintS3FileSystem


def uninstall():
    """Put back whatever was registered before. A library that cannot be
    removed is one an operator cannot use to bisect a problem."""
    for p, cls in _ORIGINALS.items():
        fsspec.register_implementation(p, cls, clobber=True)
    _ORIGINALS.clear()
