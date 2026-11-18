"""Presigned result links. Signing is offline, so these use real boto3 and no S3."""
from urllib.parse import parse_qs, urlsplit

import pytest

from hydrowf_platform.downloads import MAX_EXPIRES, Presigner, presigner_from, split_s3_url

KEY = "job-1/job-1-step-run-1/ngen-run.tgz"
KEYS = {"HYDROWF_S3_ACCESS_KEY": "reader", "HYDROWF_S3_SECRET_KEY": "secret"}


@pytest.fixture
def s3_keys(monkeypatch):
    monkeypatch.delenv("HYDROWF_S3_PUBLIC_ENDPOINT", raising=False)
    for name, value in KEYS.items():
        monkeypatch.setenv(name, value)


def test_an_s3_url_splits_into_bucket_and_key():
    assert split_s3_url(f"s3://my-bucket/{KEY}") == ("my-bucket", KEY)


def test_a_question_mark_or_hash_is_part_of_the_key():
    assert split_s3_url("s3://my-bucket/a?x#y") == ("my-bucket", "a?x#y")     # never a query: same object


@pytest.mark.parametrize("url", ["https://my-bucket/k", "s3://my-bucket", "s3://my-bucket/", "s3:///k",
                                 "s3://user@my-bucket/k", "s3://my-bucket:9000/k"])
def test_anything_else_is_not_an_s3_url(url):
    with pytest.raises(ValueError, match="is not an s3://"):
        split_s3_url(url)


def test_the_url_is_signed_for_the_public_endpoint_path_style():
    url = Presigner("https://s3.example.org", "reader", "secret", expires=600).url(f"s3://my-bucket/{KEY}")
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    assert (parts.scheme, parts.netloc, parts.path) == ("https", "s3.example.org", f"/my-bucket/{KEY}")
    assert query["X-Amz-Expires"] == ["600"]
    assert query["X-Amz-Credential"][0].startswith("reader/")
    assert "X-Amz-Signature" in query


@pytest.mark.parametrize("expires", [0, -1, MAX_EXPIRES + 1])
def test_expiry_is_within_sigv4s_limit(expires):
    with pytest.raises(ValueError, match="expires must be"):
        Presigner("https://s3.example.org", "reader", "secret", expires=expires)


def test_no_downloads_section_means_no_presigner():
    assert presigner_from(None) is None


def test_an_empty_section_is_checked_not_ignored(monkeypatch):
    for name in ("HYDROWF_S3_PUBLIC_ENDPOINT", *KEYS):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="downloads needs"):
        presigner_from({})


def test_an_empty_section_takes_the_endpoint_from_the_environment(s3_keys, monkeypatch):
    monkeypatch.setenv("HYDROWF_S3_PUBLIC_ENDPOINT", "https://s3.example.org")
    assert urlsplit(presigner_from({}).url(f"s3://my-bucket/{KEY}")).netloc == "s3.example.org"


def test_an_empty_endpoint_variable_leaves_the_configs(s3_keys, monkeypatch):
    monkeypatch.setenv("HYDROWF_S3_PUBLIC_ENDPOINT", "")             # how Helm renders an unset value
    url = presigner_from({"endpoint": "http://localhost:9000"}).url(f"s3://my-bucket/{KEY}")
    assert urlsplit(url).netloc == "localhost:9000"


def test_one_client_per_configuration(s3_keys):
    section = {"endpoint": "http://localhost:9000"}
    assert presigner_from(section) is presigner_from(dict(section))   # pygeoapi rebuilds the manager per request
    assert presigner_from(section) is not presigner_from({**section, "expires": 60})


def test_the_config_builds_a_presigner(s3_keys):
    presigner = presigner_from({"endpoint": "http://localhost:9000", "expires": 120})
    assert presigner.expires == 120
    assert presigner.url(f"s3://my-bucket/{KEY}").startswith(f"http://localhost:9000/my-bucket/{KEY}?")


def test_the_environment_overrides_the_endpoint(s3_keys, monkeypatch):
    monkeypatch.setenv("HYDROWF_S3_PUBLIC_ENDPOINT", "https://s3.example.org")
    url = presigner_from({"endpoint": "http://localhost:9000"}).url(f"s3://my-bucket/{KEY}")
    assert urlsplit(url).netloc == "s3.example.org"


@pytest.mark.parametrize("missing", sorted(KEYS))
def test_a_half_configured_section_is_refused(s3_keys, monkeypatch, missing):
    monkeypatch.delenv(missing)
    with pytest.raises(ValueError, match="downloads needs"):
        presigner_from({"endpoint": "http://localhost:9000"})
