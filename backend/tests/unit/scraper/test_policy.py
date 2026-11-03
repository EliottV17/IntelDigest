"""Unit tests for scraper public destination and URL validation policy (M10)."""

import ipaddress

import pytest

from inteldigest.scraper.models import ScrapeError, ScrapeErrorKind, ScrapeStage
from inteldigest.scraper.policy import (
     ERR_SECURITY_REJECTED_MESSAGE,
    SecurityRejectedError,
    is_legacy_ipv4_form,
    is_metadata_hostname,
    is_public_address,
    valid_dial_host,
    valid_dial_port,
    valid_dns_name,
    validate_url_policy,
)


@pytest.mark.parametrize(
    ("raw_url", "accepted"),
    [
        ("https://example.com/article?q=private", True),
        ("https://[::ffff:8.8.8.8]/", True),
        ("http://example.com:80/", True),
        ("https://example.com:443/", True),
        ("http://example.com:443/", False),
        ("https://example.com:80/", False),
        ("https://example.com:8443/", False),
        ("https://example.com:0443/", False),
        ("https://example.com:/", False),
        ("https://example.com:abc/", False),
        ("https://user:pass@example.com/", False),
        ("https://user@example.com/", False),
        ("//example.com/path", False),
        ("https:example.com/path", False),
        ("ftp://example.com/", False),
        ("https:///path", False),
        ("http://metadata.google.internal./", False),
        ("http://sub.metadata.google.internal/", False),
        ("http://metadata/", False),
        ("http://instance-data/", False),
        ("http://instance-data.ec2.internal/", False),
        ("http://metadata.azure.com/", False),
        ("http://metadata.aws.internal/", False),
        ("http://metadata.tencentyun.com/", False),
        ("http://exa%6dple.com/", False),
        ("http://0177.0.0.1/", False),
        ("http://127.1/", False),
        ("http://0x7f000001/", False),
        ("http://127.0.0.1/", False),
        ("https://10.1.2.3/", False),
        ("http://[::ffff:10.1.2.3]/", False),
        ("http://0X7F000001/", False),
        ("http://0x7f.0.0.1/", False),
        ("http://127.0x0.0.1/", False),
        ("http://0x7f.1/", False),
        ("http://0x08080808/", False),
        ("http://0X7F000001./", False),
        ("http://127.0X0.0.1./", False),
        ("http://[fe80::1%25eth0]/", False),
        ("http://[fe80::1%eth0]/", False),
        ("http://[::1]oops/", False),
        ("https:////evil.example", False),
        ("https://[2001:4860:4860::8888]:443@evil.example/", False),
        ("https://example.com/path with space", False),
        ("https://example.com/path\n", False),
        ("", False),
    ],
)
def test_validate_url_policy(raw_url: str, accepted: bool) -> None:
    if accepted:
        parsed = validate_url_policy(raw_url)
        assert parsed.scheme in {"http", "https"}
        assert parsed.hostname is not None
    else:
        with pytest.raises(ScrapeError) as exc_info:
            validate_url_policy(raw_url)
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.VALIDATION
        assert isinstance(err.__cause__, SecurityRejectedError)
        assert str(err.__cause__) == ERR_SECURITY_REJECTED_MESSAGE
        assert str(err) == "scrape security_rejected during validation"


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        ("8.8.8.8", True),
        ("1.1.1.1", True),
        ("0.1.2.3", False),
        ("10.1.2.3", False),
        ("100.64.0.1", False),
        ("127.0.0.1", False),
        ("169.254.169.254", False),
        ("169.254.170.2", False),
        ("100.100.100.200", False),
        ("168.63.129.16", False),
        ("168.63.129.15", True),
        ("168.63.129.17", True),
        ("168.63.0.1", True),
        ("172.31.0.1", False),
        ("192.0.0.8", False),
        ("192.0.2.1", False),
        ("192.31.196.1", False),
        ("192.52.193.1", False),
        ("192.88.99.1", False),
        ("192.168.1.1", False),
        ("192.175.48.1", False),
        ("198.18.0.1", False),
        ("198.51.100.1", False),
        ("203.0.113.1", False),
        ("224.0.0.1", False),
        ("240.0.0.1", False),
        ("255.255.255.255", False),
        ("::", False),
        ("::1", False),
        ("fc00::1", False),
        ("fe80::1", False),
        ("ff02::1", False),
        ("2001:db8::1", False),
        ("2001::1", False),
        ("3fff::1", False),
        ("2002:0808:0808::1", False),
        ("64:ff9b::808:808", False),
        ("64:ff9b:1::808:808", False),
        ("::ffff:8.8.8.8", True),
        ("::ffff:10.0.0.1", False),
        ("2001:4860:4860::8888", True),
        ("2606:4700:4700::1111", True),
    ],
)
def test_public_address_policy(address: str, expected: bool) -> None:
    parsed = ipaddress.ip_address(address)
    assert is_public_address(parsed) is expected
    assert is_public_address(address) is expected


def test_public_address_rejects_scoped_ipv6_and_invalid_strings() -> None:
    scoped = ipaddress.IPv6Address("fe80::1%eth0")
    assert is_public_address(scoped) is False
    assert is_public_address("2001:4860:4860::8888%eth0") is False
    assert is_public_address("not-an-ip") is False


def test_azure_wireserver_boundary_in_url_validation() -> None:
    for allowed in ("168.63.129.15", "168.63.129.17", "168.63.0.1"):
        parsed = validate_url_policy(f"http://{allowed}/")
        assert parsed.hostname == allowed

    with pytest.raises(ScrapeError) as exc_info:
        validate_url_policy("http://168.63.129.16/")
    assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED


@pytest.mark.parametrize(
    "host",
    [
        "0x.org",
        "100x.com",
        "0xdata.com",
        "fox0x.com",
        "node-0x1a.example.com",
        "0xdeadbeef.example.com",
        "0x123.example.com",
        "0XDEADBEEF.Example.COM.",
        "0X.Org.",
    ],
)
def test_hex_like_dns_names_are_accepted(host: str) -> None:
    assert valid_dns_name(host) is True
    assert is_legacy_ipv4_form(host.rstrip(".")) is False
    parsed = validate_url_policy(f"https://{host}/")
    assert parsed.scheme == "https"


@pytest.mark.parametrize(
    "host",
    [
        "0x7f000001",
        "0X7F000001",
        "0x7f.0.0.1",
        "127.0x0.0.1",
        "0x7f.1",
        "0x08080808",
        "0X7F000001.",
        "0X7F.0.0.1.",
        "127.0X0.0.1.",
        "0X7F.1.",
        "0X08080808.",
        "0177.0.0.1",
        "127.1",
        "2130706433",
    ],
)
def test_legacy_encoded_ipv4_forms_rejected(host: str) -> None:
    assert is_legacy_ipv4_form(host.rstrip(".")) is True
    assert valid_dns_name(host) is False
    with pytest.raises(ScrapeError) as exc_info:
        validate_url_policy(f"http://{host}/")
    assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED


def test_dns_name_label_and_length_bounds() -> None:
    assert valid_dns_name("") is False
    assert valid_dns_name(".") is False
    assert valid_dns_name("-leading.example") is False
    assert valid_dns_name("trailing-.example") is False
    assert valid_dns_name("double..dot.example") is False
    assert valid_dns_name("non-ascii-ñ.example") is False
    assert valid_dns_name(f"{'a' * 64}.example") is False
    assert valid_dns_name(f"{'a' * 63}.example") is True
    long_254 = ".".join(["a" * 63, "b" * 63, "c" * 63, "d" * 62])
    assert len(long_254) == 254
    assert valid_dns_name(long_254) is False


def test_metadata_hostname_matching() -> None:
    assert is_metadata_hostname("localhost") is True
    assert is_metadata_hostname("localhost.") is True
    assert is_metadata_hostname("sub.localhost") is True
    assert is_metadata_hostname("metadata.google.internal.") is True
    assert is_metadata_hostname("foo.metadata.aws.internal") is True
    assert is_metadata_hostname("example.com") is False


def test_dial_host_and_port_helpers() -> None:
    assert valid_dial_port("80") is True
    assert valid_dial_port("443") is True
    assert valid_dial_port(80) is True
    assert valid_dial_port(443) is True
    assert valid_dial_port("8080") is False
    assert valid_dial_port("0443") is False
    assert valid_dial_port(8443) is False

    assert valid_dial_host("news.example") is True
    assert valid_dial_host("8.8.8.8") is True
    assert valid_dial_host("2001:4860:4860::8888") is True
    assert valid_dial_host("fe80::1%eth0") is False
    assert valid_dial_host("[2001:4860:4860::8888]") is False
    assert valid_dial_host("news.example:443") is False
    assert valid_dial_host("") is False


def test_url_parser_preserves_query_and_fragment_without_mutating_caller_input() -> None:
    raw = "https://news.example/path?q=keep&x=1#fragment"
    parsed = validate_url_policy(raw)
    assert parsed.query == "q=keep&x=1"
    assert parsed.fragment == "fragment"
    assert parsed.geturl() == raw
