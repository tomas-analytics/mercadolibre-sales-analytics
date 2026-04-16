from __future__ import annotations

import hashlib
from pathlib import Path


def calculate_file_hash(
    file_path: str | Path,
    algorithm: str = "sha256",
    chunk_size: int = 1024 * 1024,
) -> str:
    """
    Calculate a deterministic hash for a local file.

    Parameters
    ----------
    file_path:
        Path to the file on disk.
    algorithm:
        Hash algorithm supported by hashlib. Default: sha256.
    chunk_size:
        Number of bytes read per iteration.

    Returns
    -------
    str
        Hex digest of the file contents.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    hasher = hashlib.new(algorithm)

    with file_path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest()