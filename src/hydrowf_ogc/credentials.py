"""Read keys for a job's outputs: short-lived, read-only, scoped to the job's output keys.

Cloud-native outputs are read in place (range requests), not downloaded. A presigned URL covers one
object, so a Zarr store or a folder of Parquet files needs S3 keys instead: S3 STS AssumeRole with a
session policy that allows GetObject on the job's outputs and ListBucket under them, nothing else
(hydrowf-core docs/design/formats-and-data-inputs.md, Part D).

The parent of those keys is a read-only identity, never the bucket's admin key, so a session can only
ever narrow it:
- kind (RustFS): a read-only user, its keys in HYDROWF_S3_READER_ACCESS_KEY / HYDROWF_S3_READER_SECRET_KEY
  (hydrowf-deploy local/s3-reader.sh);
- EKS (AWS S3): no keys here; boto3 takes the pod's IAM role (EKS Pod Identity or IRSA), which may assume
  `role_arn`, a role that may only read the bucket.

pygeoapi config, under server.manager.connection:

    credentials:
      role_arn: arn:aws:iam::123456789012:role/hydrowf-reader   # RustFS takes any ARN
      sts_endpoint: http://minio:9000      # or HYDROWF_STS_ENDPOINT; omitted on AWS (the regional STS)
      endpoint: https://s3.example.org     # the S3 address clients use (default: downloads' endpoint)
      duration: 900                        # seconds: 15 minutes (STS's minimum; role chaining caps it at 3600)
      region: us-east-1                    # the bucket's region; readers pass it (sessions can't look it up)

Without the section, the credentials route answers 404.
"""
from __future__ import annotations

import json
import logging
import os
import re

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

DURATION = 900
MAX_POLICY = 2048                  # characters: AWS STS's limit for a packed inline session policy
SESSION_NAME = re.compile(r"[^\w+=,.@-]", re.ASCII)   # characters STS refuses in a session name


class IssuerUnavailable(Exception):
    """STS can't be reached, or refuses (e.g. a role the service may not assume): keys can't be issued now."""


def session_policy(bucket: str, keys: list[str]) -> dict:
    """Read-only access to `keys` in `bucket`: a key ending in "/" is a folder (everything under it), any
    other key one object. ListBucket only under those keys, so the keys can't list other jobs. A key with
    * or ? would widen the policy (they are wildcards in it): ValueError."""
    if any("*" in key or "?" in key for key in keys):
        raise ValueError("an output key holds a policy wildcard (* or ?)")
    objects = [f"arn:aws:s3:::{bucket}/{key}*" if key.endswith("/") else f"arn:aws:s3:::{bucket}/{key}"
               for key in keys]
    prefixes = [f"{key}*" if key.endswith("/") else key for key in keys]
    return {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": objects},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{bucket}"],
         "Condition": {"StringLike": {"s3:prefix": prefixes}}},
    ]}


class Vendor:
    def __init__(self, sts, role_arn: str, duration: int = DURATION, region: str = "us-east-1",
                 endpoint: str | None = None):
        if not 900 <= duration <= 3600:
            raise ValueError(f"credentials: duration {duration!r} is not 900 to 3600 seconds")
        self.sts, self.role_arn, self.duration, self.region, self.endpoint = sts, role_arn, duration, region, endpoint

    def issue(self, bucket: str, keys: list[str], session: str) -> dict:
        """Keys that may read `keys` in `bucket` for `duration` seconds. ValueError when the policy is over
        STS's size limit (too many outputs); IssuerUnavailable when STS can't be reached or refuses."""
        policy = json.dumps(session_policy(bucket, keys), separators=(",", ":"))
        if len(policy) > MAX_POLICY:
            raise ValueError(f"{len(keys)} outputs need a session policy over {MAX_POLICY} characters")
        try:
            found = self.sts.assume_role(RoleArn=self.role_arn, RoleSessionName=SESSION_NAME.sub("-", session)[:64],
                                         Policy=policy, DurationSeconds=self.duration)["Credentials"]
        except (BotoCoreError, ClientError) as err:
            raise IssuerUnavailable(str(err)) from err
        return {"access_key_id": found["AccessKeyId"], "secret_access_key": found["SecretAccessKey"],
                "session_token": found["SessionToken"], "expiration": found["Expiration"].isoformat()}


def vendor_from(section: object) -> Vendor | None:
    """The Vendor the manager config asks for; None without a `credentials` section. ValueError for a
    section that isn't one, at startup."""
    if section is None:
        return None
    fields = {"role_arn", "sts_endpoint", "endpoint", "duration", "region"}
    if not isinstance(section, dict) or not set(section) <= fields:
        raise ValueError(f"credentials: {section!r} is not {{{', '.join(sorted(fields))}}}")
    for name in ("sts_endpoint", "endpoint", "region"):
        if section.get(name) is not None and not isinstance(section[name], str):
            raise ValueError(f"credentials: {name} {section[name]!r} is not text")
    role_arn = section.get("role_arn")
    if not isinstance(role_arn, str) or not role_arn.startswith("arn:"):
        raise ValueError("credentials: role_arn is an ARN (RustFS takes any, e.g. arn:aws:iam::0:role/reader)")
    duration = section.get("duration", DURATION)
    if isinstance(duration, bool) or not isinstance(duration, int):
        raise ValueError(f"credentials: duration {duration!r} is not a whole number of seconds")
    keys = {}
    access, secret = os.environ.get("HYDROWF_S3_READER_ACCESS_KEY"), os.environ.get("HYDROWF_S3_READER_SECRET_KEY")
    if bool(access) != bool(secret):                              # half a key: fail here, not on every request
        raise ValueError("credentials: HYDROWF_S3_READER_ACCESS_KEY and HYDROWF_S3_READER_SECRET_KEY go together")
    if access:                                                    # kind: the read-only user; EKS: the pod's role
        keys = {"aws_access_key_id": access, "aws_secret_access_key": secret}
    # botocore logs whole responses at DEBUG, STS's keys included: never let it, whatever the service's level
    botocore_log = logging.getLogger("botocore")
    botocore_log.setLevel(max(botocore_log.getEffectiveLevel(), logging.INFO))
    sts = boto3.session.Session().client(
        "sts", endpoint_url=os.environ.get("HYDROWF_STS_ENDPOINT") or section.get("sts_endpoint"),
        region_name=section.get("region", "us-east-1"),
        config=Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2}), **keys)
    return Vendor(sts, role_arn, duration, section.get("region", "us-east-1"), section.get("endpoint"))
