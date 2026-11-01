"""Unit tests for initial non-DNS URL validation."""

import pytest

from inteldigest.jobs.validation import validate_url


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/article",
        "http://blog.dev/post/1",
        "https://sub.domain.io/path?q=1",
        "http://8.8.8.8/x",
        "https://93.184.216.34/x",
        "http://[2001:4860:4860::8888]/x",
        "http://[::ffff:8.8.8.8]/x",
        "https://example.com:8443/article",
        "http://example.com:8080/x",
    ],
)
def test_valid_urls_pass_without_dns(url: str) -> None:
    assert validate_url(url) is None


@pytest.mark.parametrize("raw", ["", "   ", "\t\n\r"])
def test_empty_or_whitespace_urls_raise_empty(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^url: empty$"):
        validate_url(raw)


@pytest.mark.parametrize(
    "raw",
    [
        " https://example.com",
        "https://example.com ",
        "https://example.com/foo bar",
        "http://example.com/path\twith\ttab",
    ],
)
def test_whitespace_in_nonblank_url_raises_invalid(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^url: invalid"):
        validate_url(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "https://example.com/\x00",
        "https://example.com/\x1f",
        "https://example.com/\x7f",
    ],
)
def test_control_characters_raise_invalid(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^url: invalid"):
        validate_url(raw)


@pytest.mark.parametrize(
    ("raw", "expected_msg"),
    [
        ("not-a-url", 'url: scheme must be http or https, got ""'),
        ("ftp://example.com", 'url: scheme must be http or https, got "ftp"'),
        ("javascript:alert(1)", 'url: scheme must be http or https, got "javascript"'),
    ],
)
def test_invalid_scheme_raises_exact_message(raw: str, expected_msg: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_url(raw)
    assert str(excinfo.value) == expected_msg


@pytest.mark.parametrize("raw", ["https://", "http://:8080/x"])
def test_missing_hostname_raises_host_empty(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^url: host is empty$"):
        validate_url(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "http://[::1/x",
        "http://example.com:99999/x",
        "http://example.com:abc/x",
    ],
)
def test_malformed_url_syntax_raises_invalid(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^url: invalid"):
        validate_url(raw)


@pytest.mark.parametrize(
    ("raw", "expected_host"),
    [
        ("http://localhost/path", "localhost"),
        ("http://localhost./path", "localhost."),
        ("http://LOCALHOST/path", "localhost"),
        ("https://localhost:8080/path", "localhost"),
    ],
)
def test_localhost_rejected(raw: str, expected_host: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_url(raw)
    assert str(excinfo.value) == f'url: private/reserved address "{expected_host}"'


@pytest.mark.parametrize(
    ("raw", "expected_host"),
    [
        ("http://127.0.0.1/path", "127.0.0.1"),
        ("http://10.0.0.1/x", "10.0.0.1"),
        ("http://172.16.0.1/x", "172.16.0.1"),
        ("http://192.168.1.1/x", "192.168.1.1"),
        ("http://169.254.169.254/latest", "169.254.169.254"),
        ("http://0.0.0.0/x", "0.0.0.0"),
        ("http://224.0.0.1/x", "224.0.0.1"),
        ("http://240.0.0.1/x", "240.0.0.1"),
        ("http://[::1]/path", "::1"),
        ("http://[::]/x", "::"),
        ("http://[fe80::1]/x", "fe80::1"),
        ("http://[fc00::1]/x", "fc00::1"),
        ("http://[::ffff:127.0.0.1]/x", "::ffff:127.0.0.1"),
        ("http://[::ffff:169.254.169.254]/x", "::ffff:169.254.169.254"),
        ("http://[::ffff:10.0.0.1]/x", "::ffff:10.0.0.1"),
    ],
)
def test_private_and_reserved_ips_rejected(raw: str, expected_host: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_url(raw)
    assert str(excinfo.value) == f'url: private/reserved address "{expected_host}"'


def test_caller_string_not_mutated() -> None:
    url = "https://example.com/article?query=1#frag"
    validate_url(url)
    assert url == "https://example.com/article?query=1#frag"
