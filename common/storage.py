"""Upload / list / read dans MinIO (boto3), avec une copie locale de même arborescence.

Tant que l'infra n'est pas prête (MINIO_ENABLED=false), tout est écrit dans data/<couche>/...
`sync_local_to_minio()` pousse ensuite cet historique local dans MinIO sans rien perdre.
"""
from functools import lru_cache
from pathlib import Path
from typing import Iterator, List

from common import config


@lru_cache(maxsize=1)
def s3():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


def ensure_bucket(bucket: str):
    from botocore.exceptions import ClientError
    try:
        s3().head_bucket(Bucket=bucket)
    except ClientError:
        s3().create_bucket(Bucket=bucket)


def put_bytes(layer: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    """Écrit un objet dans la couche `layer` (raw / clean / curated). Renvoie l'emplacement écrit."""
    local = config.LOCAL_LAKE / layer / key
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(data)
    if config.MINIO_ENABLED:
        bucket = getattr(config, f"BUCKET_{layer.upper()}")
        s3().put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{bucket}/{key}"
    return str(local)


def get_bytes(layer: str, key: str) -> bytes:
    local = config.LOCAL_LAKE / layer / key
    if local.exists():
        return local.read_bytes()
    bucket = getattr(config, f"BUCKET_{layer.upper()}")
    return s3().get_object(Bucket=bucket, Key=key)["Body"].read()


def list_keys(layer: str, prefix: str = "") -> List[str]:
    """Clés d'une couche (MinIO si actif, sinon disque local), triées."""
    if config.MINIO_ENABLED:
        bucket = getattr(config, f"BUCKET_{layer.upper()}")
        keys = []
        for page in s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return sorted(keys)
    base = config.LOCAL_LAKE / layer
    return sorted(str(p.relative_to(base)) for p in (base / prefix).rglob("*") if p.is_file()) if (base / prefix).exists() else []


def _local_files(layer: str, prefix: str) -> Iterator[Path]:
    base = config.LOCAL_LAKE / layer / prefix
    return (p for p in base.rglob("*") if p.is_file()) if base.exists() else iter(())


def sync_local_to_minio(layer: str = "raw", prefix: str = "") -> int:
    """Envoie dans MinIO les fichiers locaux qui n'y sont pas encore. Renvoie le nombre de fichiers envoyés."""
    bucket = getattr(config, f"BUCKET_{layer.upper()}")
    ensure_bucket(bucket)
    remote = set()
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        remote |= {o["Key"] for o in page.get("Contents", [])}
    sent = 0
    base = config.LOCAL_LAKE / layer
    for p in _local_files(layer, prefix):
        key = str(p.relative_to(base))
        if key not in remote:
            s3().upload_file(str(p), bucket, key)
            sent += 1
    return sent


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Pousse l'historique local dans MinIO")
    ap.add_argument("--layer", default="raw")
    ap.add_argument("--prefix", default="")
    a = ap.parse_args()
    print(f"{sync_local_to_minio(a.layer, a.prefix)} fichier(s) envoyé(s) dans {a.layer}/{a.prefix}")
