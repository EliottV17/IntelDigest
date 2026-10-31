from __future__ import annotations

import re
import socket
from dataclasses import fields
from pathlib import Path

import pytest

from inteldigest.config import load_api, load_worker

BASE = {"DATABASE_URL": "postgres://user:pass@db/app", "REDIS_URL": "redis://cache:6379/0"}


def clean_env(**overrides: str) -> dict[str, str]:
    return BASE | overrides


def test_api_defaults_and_exact_fields() -> None:
    config = load_api(clean_env())

    assert config.database_url == BASE["DATABASE_URL"]
    assert config.api_port == "8080"
    assert config.redis_url == BASE["REDIS_URL"]
    assert config.redis_stream == "inteldigest:jobs"
    assert config.redis_dial_timeout_seconds == 5
    assert config.redis_publish_timeout_seconds == 2
    assert {field.name for field in fields(config)} == {
        "database_url", "api_port", "redis_url", "redis_stream",
        "redis_dial_timeout_seconds", "redis_publish_timeout_seconds",
    }


def test_api_seconds_and_worker_settings_are_ignored() -> None:
    config = load_api(clean_env(
        API_PORT="", REDIS_STREAM="", REDIS_DIAL_TIMEOUT="0.5",
        REDIS_PUBLISH_TIMEOUT="0.75", WORKER_POOL_SIZE="nonsense",
        WORKER_CONSUMER_GROUP="", WORKER_CONSUMER_NAME="", SCRAPER_MAX_BODY_BYTES="0",
    ))

    assert config.api_port == "8080"
    assert config.redis_stream == "inteldigest:jobs"
    assert config.redis_dial_timeout_seconds == 0.5
    assert config.redis_publish_timeout_seconds == 0.75


def test_api_preserves_nonempty_port_stream_and_redis_url() -> None:
    url = "rediss://user:secret@cache:6380/2"
    config = load_api(clean_env(API_PORT=" 0 ", REDIS_STREAM=" ", REDIS_URL=url))
    assert config.api_port == " 0 "
    assert config.redis_stream == " "
    assert config.redis_url == url


@pytest.mark.parametrize("key", ["REDIS_DIAL_TIMEOUT", "REDIS_PUBLISH_TIMEOUT"])
@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "1s"])
def test_api_rejects_nonpositive_nonfinite_or_non_numeric_seconds(key: str, value: str) -> None:
    with pytest.raises(ValueError, match=key):
        load_api(clean_env(**{key: value}))


@pytest.mark.parametrize("environment", [
    {"REDIS_URL": BASE["REDIS_URL"]},
    {"DATABASE_URL": BASE["DATABASE_URL"]},
])
def test_api_requires_both_urls(environment: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="is required"):
        load_api(environment)


@pytest.mark.parametrize("url", ["http://cache", "redis:///0", "redis://cache/not-a-db", "redis://cache/-1"])
def test_api_rejects_invalid_redis_urls(url: str) -> None:
    with pytest.raises(ValueError, match="invalid REDIS_URL"):
        load_api(clean_env(REDIS_URL=url))


def test_worker_defaults_and_generated_consumer_name() -> None:
    config = load_worker(clean_env())

    assert config.database_url == BASE["DATABASE_URL"]
    assert config.redis_url == BASE["REDIS_URL"]
    assert config.redis_stream == "inteldigest:jobs"
    assert config.redis_dial_timeout_seconds == 5
    assert config.pool_size == 4
    assert config.consumer_group == "inteldigest:workers"
    assert config.stream_block_seconds == 2
    assert config.queue_operation_timeout_seconds == 5
    assert config.queue_read_timeout_seconds == 7
    assert config.db_timeout_seconds == 5
    assert config.shutdown_timeout_seconds == 10
    assert config.scraper_connect_timeout_seconds == 5
    assert config.scraper_tls_timeout_seconds == 5
    assert config.scraper_header_timeout_seconds == 10
    assert config.scraper_read_timeout_seconds == 10
    assert config.scraper_request_timeout_seconds == 30
    assert config.scraper_max_body_bytes == 5_242_880
    assert config.scraper_max_body_read_bytes == 5_242_881
    assert config.scraper_max_redirects == 5
    assert re.fullmatch(r".+-[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", config.consumer_name)


def test_generated_consumer_names_include_hostname_and_are_unique() -> None:
    first = load_worker(clean_env()).consumer_name
    second = load_worker(clean_env()).consumer_name
    assert first.startswith(socket.gethostname() + "-")
    assert second.startswith(socket.gethostname() + "-")
    assert first != second


def test_worker_explicit_numeric_seconds_and_unset_vs_empty() -> None:
    config = load_worker(clean_env(
        WORKER_POOL_SIZE="64", WORKER_CONSUMER_GROUP=" workers:test ", WORKER_CONSUMER_NAME=" operator-1 ",
        WORKER_STREAM_BLOCK="0.001", WORKER_QUEUE_OPERATION_TIMEOUT="0.5", WORKER_DB_TIMEOUT="3.25",
        WORKER_SHUTDOWN_TIMEOUT="11", SCRAPER_CONNECT_TIMEOUT="1", SCRAPER_TLS_TIMEOUT="2",
        SCRAPER_HEADER_TIMEOUT="3", SCRAPER_READ_TIMEOUT="4", SCRAPER_REQUEST_TIMEOUT="30.5",
        SCRAPER_MAX_BODY_BYTES="1", SCRAPER_MAX_REDIRECTS="0",
    ))
    assert (config.pool_size, config.consumer_group, config.consumer_name) == (64, "workers:test", "operator-1")
    assert config.stream_block_seconds == 0.001
    assert config.queue_operation_timeout_seconds == 0.5
    assert config.queue_read_timeout_seconds == 0.501
    assert (config.db_timeout_seconds, config.shutdown_timeout_seconds) == (3.25, 11)
    assert (config.scraper_connect_timeout_seconds, config.scraper_tls_timeout_seconds) == (1, 2)
    assert (config.scraper_header_timeout_seconds, config.scraper_read_timeout_seconds, config.scraper_request_timeout_seconds) == (3, 4, 30.5)
    assert (config.scraper_max_body_bytes, config.scraper_max_body_read_bytes, config.scraper_max_redirects) == (1, 2, 0)


@pytest.mark.parametrize(("key", "value"), [
    ("WORKER_CONSUMER_NAME", ""), ("WORKER_CONSUMER_GROUP", " \t"),
    ("WORKER_POOL_SIZE", "0"), ("WORKER_POOL_SIZE", "65"), ("WORKER_POOL_SIZE", "many"),
    ("WORKER_STREAM_BLOCK", "0"), ("WORKER_STREAM_BLOCK", "0.0009"),
    ("WORKER_QUEUE_OPERATION_TIMEOUT", "0"), ("WORKER_DB_TIMEOUT", "-1"),
    ("WORKER_SHUTDOWN_TIMEOUT", "nan"), ("SCRAPER_CONNECT_TIMEOUT", "0"),
    ("SCRAPER_TLS_TIMEOUT", "-1"), ("SCRAPER_HEADER_TIMEOUT", "bad"),
    ("SCRAPER_READ_TIMEOUT", "inf"), ("SCRAPER_REQUEST_TIMEOUT", "1s"),
    ("SCRAPER_MAX_BODY_BYTES", "0"), ("SCRAPER_MAX_BODY_BYTES", "many"),
    ("SCRAPER_MAX_REDIRECTS", "-1"), ("SCRAPER_MAX_REDIRECTS", "11"),
    ("SCRAPER_MAX_REDIRECTS", "many"),
])
def test_worker_rejects_invalid_settings(key: str, value: str) -> None:
    overrides = {key: value}
    if key != "WORKER_CONSUMER_NAME":
        overrides["WORKER_CONSUMER_NAME"] = "fixed"
    with pytest.raises(ValueError):
        load_worker(clean_env(**overrides))


def test_worker_does_not_sum_scraper_stage_and_redirect_budgets() -> None:
    config = load_worker(clean_env(
        WORKER_CONSUMER_NAME="fixed", SCRAPER_CONNECT_TIMEOUT="10", SCRAPER_TLS_TIMEOUT="10",
        SCRAPER_HEADER_TIMEOUT="10", SCRAPER_READ_TIMEOUT="10", SCRAPER_REQUEST_TIMEOUT="1",
        SCRAPER_MAX_REDIRECTS="10",
    ))
    assert config.scraper_request_timeout_seconds == 1


def test_api_and_worker_settings_are_isolated() -> None:
    api = load_api(clean_env(API_PORT="3000", WORKER_POOL_SIZE="invalid", REDIS_PUBLISH_TIMEOUT="2"))
    worker = load_worker(clean_env(API_PORT="nonsense", REDIS_PUBLISH_TIMEOUT="invalid", WORKER_CONSUMER_NAME="fixed"))
    assert api.api_port == "3000"
    assert api.redis_publish_timeout_seconds == 2
    assert worker.consumer_name == "fixed"


def test_failure_does_not_mutate_environment_or_expose_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    environment = {"DATABASE_URL": "postgres://u:db-secret@db/app", "REDIS_URL": "redis://u:redis-secret@"}
    before = environment.copy()
    with pytest.raises(ValueError) as error:
        load_worker(environment)
    assert environment == before
    assert "db-secret" not in str(error.value)
    assert "redis-secret" not in str(error.value)
    assert capsys.readouterr() == ("", "")


def test_explicit_mapping_is_isolated_from_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "inherited")
    with pytest.raises(ValueError, match="DATABASE_URL is required"):
        load_api({"REDIS_URL": BASE["REDIS_URL"]})


def test_omitted_environment_reads_process_environment_passively(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", BASE["DATABASE_URL"])
    monkeypatch.setenv("REDIS_URL", BASE["REDIS_URL"])
    monkeypatch.setenv("REDIS_PUBLISH_TIMEOUT", "0.25")
    for name in ("API_PORT", "REDIS_STREAM", "REDIS_DIAL_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    assert load_api().redis_publish_timeout_seconds == 0.25


def test_readmes_document_public_api_and_seconds_syntax() -> None:
    readme = Path(__file__).parents[1].joinpath("README.md").read_text()
    for required in ("load_api", "load_worker", "seconds", "0.5", "REDIS_PUBLISH_TIMEOUT", "SCRAPER_MAX_REDIRECTS"):
        assert required in readme
    assert "nanosecond" not in readme.lower()
    root_readme = Path(__file__).parents[2].joinpath("README.md").read_text()
    assert "configuration" in root_readme.lower()
