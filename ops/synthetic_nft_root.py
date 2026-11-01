"""Fail-closed path selection for isolated nft fixture tooling."""
from pathlib import Path


def checked_nft_root(value):
    if not isinstance(value,str) or not value:
        raise ValueError('absolute canonical synthetic nft root required')
    path=Path(value)
    try:
        if str(path)!=value or not path.is_absolute() or not path.is_dir() or path.resolve(strict=True)!=path:
            raise ValueError('absolute canonical synthetic nft root required')
    except (OSError,RuntimeError):
        raise ValueError('absolute canonical synthetic nft root required') from None
    return str(path)
