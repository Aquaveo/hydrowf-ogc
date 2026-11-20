"""Read keys for a job's outputs (hydrowf_ogc.credentials, ArgoManager.get_job_credentials, the route)."""
import json
from datetime import datetime, timezone

import pytest
import yaml
from botocore.exceptions import ClientError, EndpointConnectionError
from pygeoapi.config import get_config
from pygeoapi.openapi import generate_openapi_document
from pygeoapi.process.base import JobNotFoundError, JobResultNotFoundError
from starlette.testclient import TestClient
from test_app import AUDIENCE, CONFIG, ISSUER, REPO, URL, StubKeys, bearer
from test_manager import DONE, ME, RUNNING, SOMEONE, THEIRS, FakeRuntime, manager

from hydrowf_core import auth
from hydrowf_ogc import app as service
from hydrowf_ogc import caller
from hydrowf_ogc.credentials import MAX_POLICY, Vendor, session_policy, vendor_from
from hydrowf_ogc.manager import CredentialsUnavailable

CNG = "s3://my-bucket/job-done/job-done-step-to-cng-1/cng/"
TGZ = "s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz"
EXPIRES = datetime(2026, 11, 20, 22, 0, tzinfo=timezone.utc)


class StubSTS:
    """An STS client: records what assume_role is asked, answers fixed keys, or raises `error`."""

    def __init__(self, error=None):
        self.calls, self.error = [], error

    def assume_role(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return {"Credentials": {"AccessKeyId": "AK", "SecretAccessKey": "SK", "SessionToken": "ST",
                                "Expiration": EXPIRES}}


def policy_of(sts):
    return json.loads(sts.calls[-1]["Policy"])


# ---- the session policy


def test_a_folder_output_is_everything_under_it_and_a_file_output_one_object():
    policy = session_policy("my-bucket", ["job/pod/cng/", "job/pod/ngen-run.tgz"])
    get, listing = policy["Statement"]
    assert get["Action"] == ["s3:GetObject"]
    assert get["Resource"] == ["arn:aws:s3:::my-bucket/job/pod/cng/*", "arn:aws:s3:::my-bucket/job/pod/ngen-run.tgz"]
    assert listing["Action"] == ["s3:ListBucket"] and listing["Resource"] == ["arn:aws:s3:::my-bucket"]
    assert listing["Condition"] == {"StringLike": {"s3:prefix": ["job/pod/cng/*", "job/pod/ngen-run.tgz"]}}
    assert not any("Put" in a or "Delete" in a for s in policy["Statement"] for a in s["Action"])


@pytest.mark.parametrize("key", ["job/pod/*", "job/pod/a?b"])
def test_a_key_with_a_policy_wildcard_is_refused(key):
    with pytest.raises(ValueError, match="wildcard"):
        session_policy("my-bucket", [key])


# ---- the vendor


def test_keys_last_fifteen_minutes_by_default_and_the_session_is_named_after_the_job():
    sts = StubSTS()
    issued = Vendor(sts, "arn:aws:iam::0:role/reader").issue("my-bucket", ["job/pod/cng/"], session="job-a b/c")
    assert issued == {"access_key_id": "AK", "secret_access_key": "SK", "session_token": "ST",
                      "expiration": "2026-11-20T22:00:00+00:00"}
    call = sts.calls[0]
    assert call["DurationSeconds"] == 900 and call["RoleArn"] == "arn:aws:iam::0:role/reader"
    assert call["RoleSessionName"] == "job-a-b-c"                       # what STS accepts
    assert policy_of(sts) == session_policy("my-bucket", ["job/pod/cng/"])


def test_a_policy_over_the_sts_limit_is_refused_before_asking():
    sts = StubSTS()
    keys = [f"job/pod-{n}/output-{n}/" for n in range(60)]
    with pytest.raises(ValueError, match=f"over {MAX_POLICY} characters"):
        Vendor(sts, "arn:aws:iam::0:role/reader").issue("my-bucket", keys, session="job")
    assert sts.calls == []


@pytest.mark.parametrize("section, message", [
    ({"role_arn": "reader"}, "role_arn is an ARN"),
    ({"role_arn": "arn:aws:iam::0:role/r", "duration": 600}, "900 to 3600"),
    ({"role_arn": "arn:aws:iam::0:role/r", "duration": "900"}, "whole number of seconds"),
    ({"role_arn": "arn:aws:iam::0:role/r", "keys": "x"}, "is not"),
    ("yes", "is not"),
])
def test_a_bad_credentials_section_fails_at_startup(section, message):
    with pytest.raises(ValueError, match=message):
        vendor_from(section)


def test_without_a_credentials_section_there_is_no_vendor():
    assert vendor_from(None) is None


def clients_built(monkeypatch):
    """The keyword arguments every boto3 client is built with, from here on."""
    import boto3
    built = []

    def client(self, service, **kwargs):
        built.append(kwargs)
        return StubSTS()
    monkeypatch.setattr(boto3.session.Session, "client", client)
    return built


def test_the_reader_keys_come_from_the_environment(monkeypatch):
    built = clients_built(monkeypatch)
    monkeypatch.setenv("HYDROWF_S3_READER_ACCESS_KEY", "reader")
    monkeypatch.setenv("HYDROWF_S3_READER_SECRET_KEY", "secret")
    monkeypatch.setenv("HYDROWF_STS_ENDPOINT", "http://minio:9000")
    vendor = vendor_from({"role_arn": "arn:aws:iam::0:role/r", "region": "us-west-2", "endpoint": "https://s3.x.org"})
    [kwargs] = built
    assert (kwargs["aws_access_key_id"], kwargs["aws_secret_access_key"]) == ("reader", "secret")
    assert kwargs["endpoint_url"] == "http://minio:9000" and kwargs["region_name"] == "us-west-2"
    assert (vendor.region, vendor.endpoint) == ("us-west-2", "https://s3.x.org")


def test_without_reader_keys_the_default_chain_is_used(monkeypatch):
    built = clients_built(monkeypatch)
    for name in ("HYDROWF_S3_READER_ACCESS_KEY", "HYDROWF_S3_READER_SECRET_KEY", "HYDROWF_STS_ENDPOINT"):
        monkeypatch.delenv(name, raising=False)
    vendor_from({"role_arn": "arn:aws:iam::0:role/r"})
    assert "aws_access_key_id" not in built[0] and built[0]["endpoint_url"] is None     # EKS: the pod's role


def test_half_a_reader_key_fails_at_startup(monkeypatch):
    clients_built(monkeypatch)
    monkeypatch.setenv("HYDROWF_S3_READER_ACCESS_KEY", "reader")
    monkeypatch.delenv("HYDROWF_S3_READER_SECRET_KEY", raising=False)
    with pytest.raises(ValueError, match="go together"):
        vendor_from({"role_arn": "arn:aws:iam::0:role/r"})


def test_botocore_never_logs_at_debug_once_keys_are_issued(monkeypatch):
    import logging
    clients_built(monkeypatch)
    monkeypatch.setattr(logging.getLogger("botocore"), "level", logging.DEBUG)
    vendor_from({"role_arn": "arn:aws:iam::0:role/r"})
    assert logging.getLogger("botocore").getEffectiveLevel() >= logging.INFO


# ---- the manager: the caller's succeeded job, scoped to what Argo recorded


def with_vendor(runtime, sts=None):
    m = manager(runtime)
    m.vendor = Vendor(sts or StubSTS(), "arn:aws:iam::0:role/reader")
    return m


def test_keys_cover_exactly_the_jobs_recorded_outputs():
    sts = StubSTS()
    m = with_vendor(FakeRuntime([DONE], outputs={"job-done": {"cng": CNG, "run": TGZ}}), sts)
    with caller.acting_as(ME):
        found = m.get_job_credentials("job-done")
    assert found["prefixes"] == ["job-done/job-done-step-to-cng-1/cng/", "job-done/job-done-step-run-1/ngen-run.tgz"]
    assert (found["bucket"], found["region"], found["access_key_id"]) == ("my-bucket", "us-east-1", "AK")
    assert policy_of(sts) == session_policy("my-bucket", found["prefixes"])
    assert sts.calls[0]["RoleSessionName"] == "job-job-done"


@pytest.mark.parametrize("forged", [
    "s3://my-bucket/job-theirs/job-theirs-step-x-1/cng/",       # another job's outputs
    "s3://my-bucket/managed/",                                  # managed data
    "s3://my-bucket/job-done/",                                 # the whole job (logs too)
    "s3://other-bucket/job-done/job-done-step-x-1/cng/",       # another bucket
    "s3://my-bucket/job-done/job-done-step-x-1/*",              # a policy wildcard
], ids=["other-job", "managed", "whole-job", "other-bucket", "wildcard"])
def test_an_output_key_outside_the_jobs_own_keys_gets_no_keys(forged):
    sts = StubSTS()                         # Argo records keys from the pods' reports: a tool could forge one
    m = with_vendor(FakeRuntime([DONE], outputs={"job-done": {"cng": CNG, "forged": forged}}), sts)
    with caller.acting_as(ME), pytest.raises(CredentialsUnavailable, match="outside the job's own keys"):
        m.get_job_credentials("job-done")
    assert sts.calls == []


def test_someone_elses_job_gets_no_keys_and_sts_is_never_asked():
    sts = StubSTS()
    m = with_vendor(FakeRuntime([THEIRS], outputs={"job-theirs": {"cng": CNG}}), sts)
    with caller.acting_as(ME), pytest.raises(JobNotFoundError):
        m.get_job_credentials("job-theirs")
    assert sts.calls == []


def test_a_job_that_has_not_succeeded_gets_no_keys():
    m = with_vendor(FakeRuntime([RUNNING], outputs={"job-running": {"cng": CNG}}))
    with caller.acting_as(ME), pytest.raises(JobResultNotFoundError):
        m.get_job_credentials("job-running")


def test_without_a_vendor_or_outputs_there_are_no_keys():
    with caller.acting_as(ME), pytest.raises(CredentialsUnavailable, match="no read keys"):
        manager(FakeRuntime([DONE])).get_job_credentials("job-done")
    with caller.acting_as(ME), pytest.raises(CredentialsUnavailable, match="no outputs"):
        with_vendor(FakeRuntime([DONE])).get_job_credentials("job-done")


# ---- the route


@pytest.fixture
def runtime():
    return FakeRuntime([DONE, THEIRS], outputs={"job-done": {"cng": CNG}, "job-theirs": {"cng": CNG}})


def app_client(runtime, monkeypatch, vendor):
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(CONFIG))
    monkeypatch.setenv("PYGEOAPI_SERVER_URL", URL)
    monkeypatch.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
    config = get_config()
    config["server"]["manager"]["runtime"] = runtime
    config["server"]["manager"]["vendor"] = vendor
    openapi = yaml.safe_load(generate_openapi_document(CONFIG, "yaml"))
    app = service.create_app(config, openapi, auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys()))
    return TestClient(app, base_url="http://testserver")


def test_the_owner_gets_keys_that_nothing_may_cache(runtime, monkeypatch):
    client = app_client(runtime, monkeypatch, Vendor(StubSTS(), "arn:aws:iam::0:role/reader"))
    response = client.get("/ogc/jobs/job-done/credentials", headers=bearer())
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    assert response.json()["prefixes"] == ["job-done/job-done-step-to-cng-1/cng/"]


@pytest.mark.parametrize("headers, status", [
    ({}, 401),                                    # no token
    (bearer(SOMEONE), 404),                       # someone else's job looks like no job
], ids=["anonymous", "someone-else"])
def test_only_the_owner_gets_keys(runtime, monkeypatch, headers, status):
    sts = StubSTS()
    client = app_client(runtime, monkeypatch, Vendor(sts, "arn:aws:iam::0:role/reader"))
    response = client.get("/ogc/jobs/job-done/credentials", headers=headers)
    assert response.status_code == status and sts.calls == []


@pytest.mark.parametrize("error", [
    ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "AssumeRole"),
    EndpointConnectionError(endpoint_url="http://minio:9000"),
], ids=["refused", "unreachable"])
def test_an_sts_failure_is_a_503_not_a_500(runtime, monkeypatch, error):
    client = app_client(runtime, monkeypatch, Vendor(StubSTS(error=error), "arn:aws:iam::0:role/reader"))
    response = client.get("/ogc/jobs/job-done/credentials", headers=bearer())
    assert response.status_code == 503 and response.headers["cache-control"] == "no-store"
    assert "SK" not in response.text


@pytest.mark.parametrize("request_, status", [
    (lambda c: c.get("/ogc/jobs/job-done/credentials"), 401),
    (lambda c: c.get("/ogc/jobs/job-theirs/credentials", headers=bearer()), 404),
    (lambda c: c.get("/ogc/jobs/job-done/credentials", params={"f": "nope"}, headers=bearer()), 400),
    (lambda c: c.head("/ogc/jobs/job-done/credentials", headers=bearer()), 405),
    (lambda c: c.delete("/ogc/jobs/job-done/credentials", headers=bearer()), 405),
], ids=["no-token", "not-theirs", "bad-format", "head", "delete"])
def test_every_answer_is_no_store(runtime, monkeypatch, request_, status):
    sts = StubSTS()
    client = app_client(runtime, monkeypatch, Vendor(sts, "arn:aws:iam::0:role/reader"))
    response = request_(client)
    assert response.status_code == status and response.headers["cache-control"] == "no-store"
    assert sts.calls == []


def test_a_service_without_credentials_answers_404(runtime, monkeypatch):
    client = app_client(runtime, monkeypatch, None)
    response = client.get("/ogc/jobs/job-done/credentials", headers=bearer())
    assert response.status_code == 404 and "no read keys" in response.json()["error"]
    assert response.headers["cache-control"] == "no-store"
