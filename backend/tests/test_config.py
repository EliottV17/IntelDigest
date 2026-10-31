from __future__ import annotations

import re
import socket
from dataclasses import fields

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
    assert config.redis_dial_timeout_ns == 5_000_000_000
    assert config.redis_publish_timeout_ns == 2_000_000_000
    assert {field.name for field in fields(config)} == {
        "database_url", "api_port", "redis_url", "redis_stream",
        "redis_dial_timeout_ns", "redis_publish_timeout_ns",
    }


def test_api_explicit_values_and_worker_settings_are_ignored() -> None:
    config = load_api(clean_env(
        API_PORT="", REDIS_STREAM="", REDIS_DIAL_TIMEOUT="1500ms",
        REDIS_PUBLISH_TIMEOUT="750ms", WORKER_POOL_SIZE="nonsense",
        WORKER_CONSUMER_GROUP="", WORKER_CONSUMER_NAME="", SCRAPER_MAX_BODY_BYTES="0",
    ))

    assert config.api_port == "8080"
    assert config.redis_stream == "inteldigest:jobs"
    assert config.redis_dial_timeout_ns == 1_500_000_000
    assert config.redis_publish_timeout_ns == 750_000_000


def test_api_preserves_nonempty_port_and_stream_verbatim() -> None:
    config = load_api(clean_env(API_PORT=" 0 ", REDIS_STREAM=" "))
    assert config.api_port == " 0 "
    assert config.redis_stream == " "


@pytest.mark.parametrize(("environment", "message"), [
    ({"REDIS_URL": BASE["REDIS_URL"]}, "config: DATABASE_URL is required"),
    ({"DATABASE_URL": BASE["DATABASE_URL"]}, "config: REDIS_URL is required"),
    ({**BASE, "REDIS_URL": "http://cache"}, "config: invalid REDIS_URL: expected redis:// or rediss:// URL with a host"),
    ({**BASE, "REDIS_DIAL_TIMEOUT": "0s"}, "config: REDIS_DIAL_TIMEOUT must be a positive duration"),
    ({**BASE, "REDIS_PUBLISH_TIMEOUT": "bad"}, "config: REDIS_PUBLISH_TIMEOUT must be a positive duration"),
])
def test_api_rejects_invalid_settings(environment: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        load_api(environment)


def test_worker_defaults_and_generated_consumer_name() -> None:
    config = load_worker(clean_env())

    assert config.database_url == BASE["DATABASE_URL"]
    assert config.redis_url == BASE["REDIS_URL"]
    assert config.redis_stream == "inteldigest:jobs"
    assert config.redis_dial_timeout_ns == 5_000_000_000
    assert config.pool_size == 4
    assert config.consumer_group == "inteldigest:workers"
    assert config.stream_block_ns == 2_000_000_000
    assert config.queue_operation_timeout_ns == 5_000_000_000
    assert config.queue_read_timeout_ns == 7_000_000_000
    assert config.db_timeout_ns == 5_000_000_000
    assert config.shutdown_timeout_ns == 10_000_000_000
    assert config.scraper_connect_timeout_ns == 5_000_000_000
    assert config.scraper_tls_timeout_ns == 5_000_000_000
    assert config.scraper_header_timeout_ns == 10_000_000_000
    assert config.scraper_read_timeout_ns == 10_000_000_000
    assert config.scraper_request_timeout_ns == 30_000_000_000
    assert config.scraper_max_body_bytes == 5_242_880
    assert config.scraper_max_body_read_bytes == 5_242_881
    assert config.scraper_max_redirects == 5
    assert re.fullmatch(r".+-[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", config.consumer_name)


def test_generated_consumer_names_include_hostname_and_are_unique() -> None:
    first = load_worker(clean_env()).consumer_name
    second = load_worker(clean_env()).consumer_name
    hostname = socket.gethostname()
    assert first.startswith(hostname + "-")
    assert second.startswith(hostname + "-")
    assert first != second


def test_worker_explicit_values_and_unset_vs_empty() -> None:
    config = load_worker(clean_env(
        WORKER_POOL_SIZE="64", WORKER_CONSUMER_GROUP=" workers:test ", WORKER_CONSUMER_NAME=" operator-1 ",
        WORKER_STREAM_BLOCK="1ms", WORKER_QUEUE_OPERATION_TIMEOUT="7s", WORKER_DB_TIMEOUT="3s",
        WORKER_SHUTDOWN_TIMEOUT="11s", SCRAPER_CONNECT_TIMEOUT="1s", SCRAPER_TLS_TIMEOUT="2s",
        SCRAPER_HEADER_TIMEOUT="3s", SCRAPER_READ_TIMEOUT="4s", SCRAPER_REQUEST_TIMEOUT="30s",
        SCRAPER_MAX_BODY_BYTES="1", SCRAPER_MAX_REDIRECTS="0",
    ))
    assert (config.pool_size, config.consumer_group, config.consumer_name) == (64, "workers:test", "operator-1")
    assert config.stream_block_ns == 1_000_000
    assert config.queue_operation_timeout_ns == 7_000_000_000
    assert config.queue_read_timeout_ns == 7_001_000_000
    assert (config.db_timeout_ns, config.shutdown_timeout_ns) == (3_000_000_000, 11_000_000_000)
    assert (config.scraper_connect_timeout_ns, config.scraper_tls_timeout_ns) == (1_000_000_000, 2_000_000_000)
    assert (config.scraper_header_timeout_ns, config.scraper_read_timeout_ns, config.scraper_request_timeout_ns) == (3_000_000_000, 4_000_000_000, 30_000_000_000)
    assert (config.scraper_max_body_bytes, config.scraper_max_body_read_bytes, config.scraper_max_redirects) == (1, 2, 0)


@pytest.mark.parametrize(("key", "value", "message"), [
    ("WORKER_CONSUMER_NAME", "", "config: WORKER_CONSUMER_NAME must be nonblank when set"),
    ("WORKER_CONSUMER_GROUP", " \t", "config: WORKER_CONSUMER_GROUP must be nonblank when set"),
    ("WORKER_POOL_SIZE", "0", "config: WORKER_POOL_SIZE must be an integer from 1 through 64"),
    ("WORKER_POOL_SIZE", "65", "config: WORKER_POOL_SIZE must be an integer from 1 through 64"),
    ("WORKER_POOL_SIZE", "many", "config: WORKER_POOL_SIZE must be an integer from 1 through 64"),
    ("WORKER_STREAM_BLOCK", "0s", "config: WORKER_STREAM_BLOCK must be at least 1ms"),
    ("WORKER_STREAM_BLOCK", "1ns", "config: WORKER_STREAM_BLOCK must be at least 1ms"),
    ("WORKER_STREAM_BLOCK", "nope", "config: WORKER_STREAM_BLOCK must be at least 1ms"),
    ("WORKER_QUEUE_OPERATION_TIMEOUT", "0s", "config: WORKER_QUEUE_OPERATION_TIMEOUT must be a positive duration"),
    ("WORKER_DB_TIMEOUT", "-1s", "config: WORKER_DB_TIMEOUT must be a positive duration"),
    ("WORKER_SHUTDOWN_TIMEOUT", "bad", "config: WORKER_SHUTDOWN_TIMEOUT must be a positive duration"),
    ("SCRAPER_CONNECT_TIMEOUT", "0s", "config: SCRAPER_CONNECT_TIMEOUT must be a positive duration"),
    ("SCRAPER_TLS_TIMEOUT", "-1s", "config: SCRAPER_TLS_TIMEOUT must be a positive duration"),
    ("SCRAPER_HEADER_TIMEOUT", "bad", "config: SCRAPER_HEADER_TIMEOUT must be a positive duration"),
    ("SCRAPER_READ_TIMEOUT", "0s", "config: SCRAPER_READ_TIMEOUT must be a positive duration"),
    ("SCRAPER_REQUEST_TIMEOUT", "-1s", "config: SCRAPER_REQUEST_TIMEOUT must be a positive duration"),
    ("SCRAPER_MAX_BODY_BYTES", "0", "config: SCRAPER_MAX_BODY_BYTES must be a positive integer"),
    ("SCRAPER_MAX_BODY_BYTES", "many", "config: SCRAPER_MAX_BODY_BYTES must be a positive integer"),
    ("SCRAPER_MAX_BODY_BYTES", "9223372036854775807", "config: SCRAPER_MAX_BODY_BYTES must allow a safe max+1 read and int conversion"),
    ("SCRAPER_MAX_REDIRECTS", "-1", "config: SCRAPER_MAX_REDIRECTS must be an integer from 0 through 10"),
    ("SCRAPER_MAX_REDIRECTS", "11", "config: SCRAPER_MAX_REDIRECTS must be an integer from 0 through 10"),
    ("SCRAPER_MAX_REDIRECTS", "many", "config: SCRAPER_MAX_REDIRECTS must be an integer from 0 through 10"),
])
def test_worker_rejects_invalid_settings(key: str, value: str, message: str) -> None:
    overrides = {key: value}
    if key != "WORKER_CONSUMER_NAME":
        overrides["WORKER_CONSUMER_NAME"] = "fixed"
    with pytest.raises(ValueError, match=re.escape(message)):
        load_worker(clean_env(**overrides))


def test_worker_rejects_queue_timeout_addition_overflow() -> None:
    with pytest.raises(ValueError, match="config: WORKER_STREAM_BLOCK plus WORKER_QUEUE_OPERATION_TIMEOUT overflows"):
        load_worker(clean_env(
            WORKER_STREAM_BLOCK="9223372036854775807ns",
            WORKER_QUEUE_OPERATION_TIMEOUT="1ns",
            WORKER_CONSUMER_NAME="fixed",
        ))


def test_worker_does_not_sum_scraper_stage_and_redirect_budgets() -> None:
    config = load_worker(clean_env(
        WORKER_CONSUMER_NAME="fixed", SCRAPER_CONNECT_TIMEOUT="10s", SCRAPER_TLS_TIMEOUT="10s",
        SCRAPER_HEADER_TIMEOUT="10s", SCRAPER_READ_TIMEOUT="10s", SCRAPER_REQUEST_TIMEOUT="1s",
        SCRAPER_MAX_REDIRECTS="10",
    ))
    assert config.scraper_request_timeout_ns == 1_000_000_000


def test_api_does_not_read_worker_settings_and_worker_does_not_read_api_settings() -> None:
    api = load_api(clean_env(API_PORT="3000", WORKER_POOL_SIZE="invalid", REDIS_PUBLISH_TIMEOUT="2s"))
    worker = load_worker(clean_env(API_PORT="nonsense", REDIS_PUBLISH_TIMEOUT="invalid", WORKER_CONSUMER_NAME="fixed"))
    assert api.api_port == "3000"
    assert worker.consumer_name == "fixed"


def test_failure_does_not_mutate_environment_and_never_exposes_credentials(capsys: pytest.CaptureFixture[str]) -> None:
    environment = {"DATABASE_URL": "postgres://u:db-secret@db/app", "REDIS_URL": "redis://u:redis-secret@"}
    before = environment.copy()
    with pytest.raises(ValueError) as error:
        load_worker(environment)
    assert environment == before
    assert "db-secret" not in str(error.value)
    assert "redis-secret" not in str(error.value)
    assert capsys.readouterr() == ("", "")


def test_explicit_mapping_is_isolated_from_inherited_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "inherited")
    with pytest.raises(ValueError, match="DATABASE_URL is required"):
        load_api({"REDIS_URL": BASE["REDIS_URL"]})


def test_omitted_environment_defaults_to_os_environ(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", BASE["DATABASE_URL"])
    monkeypatch.setenv("REDIS_URL", BASE["REDIS_URL"])
    for name in ("API_PORT", "REDIS_STREAM", "REDIS_DIAL_TIMEOUT", "REDIS_PUBLISH_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)
    assert load_api().redis_publish_timeout_ns == 2_000_000_000


@pytest.mark.parametrize(("text", "expected"), [
    ("1ns", 1), ("-1ns", -1), ("1.5us", 1500), ("2µs", 2000), ("2μs", 2000),
    ("1h2m3.25s", 3_723_250_000_000), ("+2ms", 2_000_000),
])
def test_go_duration_parser_exact_nanoseconds(text: str, expected: int) -> None:
    if expected > 0:
        assert load_api(clean_env(REDIS_DIAL_TIMEOUT=text)).redis_dial_timeout_ns == expected
    else:
        with pytest.raises(ValueError):
            load_api(clean_env(REDIS_DIAL_TIMEOUT=text))


@pytest.mark.parametrize("text", ["1s0", "2h0", "500ms0", "1m0"])
def test_go_duration_rejects_unitless_zero_after_component(text: str) -> None:
    with pytest.raises(ValueError) as error:
        load_api(clean_env(REDIS_DIAL_TIMEOUT=text))
    assert str(error.value) == "config: REDIS_DIAL_TIMEOUT must be a positive duration"


@pytest.mark.parametrize("text", ["1", "1d", "1sfoo", "1e3s", "9223372037s"])
def test_go_duration_parser_rejects_invalid_or_overflow(text: str) -> None:
    with pytest.raises(ValueError, match="REDIS_DIAL_TIMEOUT must be a positive duration"):
        load_api(clean_env(REDIS_DIAL_TIMEOUT=text))


@pytest.mark.parametrize("url", [
    "redis://cache", "rediss://cache:6380/2", "redis://cache/", "redis://cache/0",
    "redis://localhost/9223372036854775807", "redis://localhost:99999/0",
    "REDIS://localhost", "Redis://localhost", "REDISS://localhost",
])
def test_redis_url_accepted_as_go_reference(url: str) -> None:
    assert load_api(clean_env(REDIS_URL=url)).redis_url == url


@pytest.mark.parametrize("url", ["http://cache", "redis:///0", "redis://cache/a", "redis://cache/-1", "redis://cache/0/1"])
def test_redis_url_rejected_as_go_reference(url: str) -> None:
    with pytest.raises(ValueError, match="invalid REDIS_URL"):
        load_api(clean_env(REDIS_URL=url))


@pytest.mark.parametrize("path", [
    "a", "-1", "1a", "0/1", "/0", " ", "%61", "0 ",
    "9223372036854775808", "99999999999999999999999999999999",
])
def test_redis_invalid_database_path_has_exact_go_error_without_leaking_credentials(
    path: str, capsys: pytest.CaptureFixture[str],
) -> None:
    url = f"redis://user:pass@cache/{path}"
    environment = clean_env(REDIS_URL=url)
    before = environment.copy()
    with pytest.raises(ValueError) as error:
        load_api(environment)
    assert str(error.value) == "config: invalid REDIS_URL: database path must be a non-negative integer"
    assert environment == before
    assert "user" not in str(error.value) and "pass" not in str(error.value)
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("url", [
    "redis://localhost:badport/0", "redis://user:pass@cache:badport/0", "redis://[:6379]/0",
])
def test_redis_invalid_host_has_exact_go_error_without_output(url: str, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(ValueError) as error:
        load_api(clean_env(REDIS_URL=url))
    assert str(error.value) == "config: invalid REDIS_URL: expected redis:// or rediss:// URL with a host"
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("url", [
    "redis://localhost:6379/0?foo=bar baz", "redis://localhost:6379/0?foo=bar baz#frag",
])
def test_redis_url_allows_go_query_spaces_and_preserves_raw_value(url: str) -> None:
    assert load_api(clean_env(REDIS_URL=url)).redis_url == url


def test_readme_documents_config_public_api_and_precision() -> None:
    from pathlib import Path

    readme = Path(__file__).parents[1].joinpath("README.md").read_text()
    for required in ("load_api", "load_worker", "nanosecond", "REDIS_PUBLISH_TIMEOUT", "SCRAPER_MAX_REDIRECTS"):
        assert required in readme
    assert "settings loaders" not in readme
    root_readme = Path(__file__).parents[2].joinpath("README.md").read_text()
    assert "configuration" in root_readme.lower()
