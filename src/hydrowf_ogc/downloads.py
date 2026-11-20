"""Download links for job results: presigned S3 GETs.

A run's outputs are s3://<bucket>/<key> objects in Argo's artifact repository (one .tgz per output),
which only the cluster can read. A presigned URL is a plain HTTPS GET, signed with the S3 keys and
valid for `expires` seconds, so a client downloads the result without S3 credentials.

Signing happens offline: no request reaches S3 here. The signature covers the host, so the URL is
signed for `endpoint`, the address clients use (an ingress, or http://localhost:9000 behind a
port-forward on kind), not the in-cluster minio:9000 Argo writes to.

pygeoapi config, under server.manager.connection:

    downloads:
      endpoint: https://s3.example.org   # or HYDROWF_S3_PUBLIC_ENDPOINT
      expires: 3600                      # seconds, at most 7 days (SigV4's limit)
      region: us-east-1

The keys come from HYDROWF_S3_ACCESS_KEY and HYDROWF_S3_SECRET_KEY, never from the config file;
a read-only key is enough. Without `downloads`, no links are signed.

The service builds ArgoManager once, at startup (hydrowf_ogc.app), and the manager checks the
section there: a broken one stops the service instead of failing every /ogc request. presigner()
caches the client per configuration.
"""
from __future__ import annotations

import os
from functools import lru_cache

import boto3
from botocore.config import Config

MAX_EXPIRES = 7 * 24 * 3600          # SigV4 rejects longer presigned URLs


def split_s3_url(url: str) -> tuple[str, str]:
    """s3://<bucket>/<key> -> (bucket, key). Plain text, not URL parsing: ArgoRuntime writes the key
    unquoted, so a "?" or "#" in it is part of the key, never a query that would change the object."""
    bucket, _, key = url.removeprefix("s3://").partition("/") if url.startswith("s3://") else ("", "", "")
    if not bucket or not key or any(c in bucket for c in "@:?#"):
        raise ValueError(f"{url!r} is not an s3://<bucket>/<key> URL")
    return bucket, key


class Presigner:
    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 expires: int = 3600, region: str = "us-east-1"):
        if not 0 < expires <= MAX_EXPIRES:
            raise ValueError(f"expires must be 1..{MAX_EXPIRES} seconds, not {expires}")
        self.expires = expires
        self.client = boto3.session.Session().client(       # its own session: boto3's default isn't thread-safe
            "s3", endpoint_url=endpoint, region_name=region,
            aws_access_key_id=access_key, aws_secret_access_key=secret_key,
            # path style (host/bucket/key): MinIO, RustFS and an ingress don't serve bucket subdomains
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def url(self, s3_url: str) -> str:
        """A GET URL for the object, valid for `expires` seconds."""
        bucket, key = split_s3_url(s3_url)
        return self.client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=self.expires)


@lru_cache(maxsize=None)
def presigner(endpoint: str, access_key: str, secret_key: str, expires: int, region: str) -> Presigner:
    """One Presigner (one boto3 client) per configuration, shared by the per-request managers."""
    return Presigner(endpoint, access_key, secret_key, expires=expires, region=region)


def presigner_from(downloads: dict | None) -> Presigner | None:
    """The Presigner the manager config asks for, or None when it has no `downloads` section.
    An empty section is checked like any other: the endpoint can come from the environment."""
    if downloads is None:
        return None
    endpoint = os.environ.get("HYDROWF_S3_PUBLIC_ENDPOINT") or downloads.get("endpoint")
    access_key = os.environ.get("HYDROWF_S3_ACCESS_KEY")
    secret_key = os.environ.get("HYDROWF_S3_SECRET_KEY")
    if not (endpoint and access_key and secret_key):
        # A half-configured section is a deployment mistake: the manager is built at startup, so it fails there
        raise ValueError("downloads needs an endpoint, HYDROWF_S3_ACCESS_KEY and HYDROWF_S3_SECRET_KEY")
    return presigner(endpoint, access_key, secret_key,
                     int(downloads.get("expires", 3600)), downloads.get("region", "us-east-1"))
