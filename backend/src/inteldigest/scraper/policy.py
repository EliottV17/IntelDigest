"""Public destination URL, hostname, port, and IP validation policy (no DNS)."""

import ipaddress
import string
from urllib.parse import SplitResult, urlsplit

from inteldigest.scraper.models import ScrapeError, ScrapeErrorKind, ScrapeStage

ERR_SECURITY_REJECTED_MESSAGE = "scrape destination rejected by public-network policy"


class SecurityRejectedError(Exception):
    """Underlying cause indicating a destination was rejected by security policy."""

    def __init__(self, message: str = ERR_SECURITY_REJECTED_MESSAGE) -> None:
        super().__init__(message)


# Static special-purpose blocks per IANA IPv4/IPv6 Special-Purpose Address Registries
# (RFC 6890 / RFC 8190), plus Azure WireServer 168.63.129.16/32.
# IPv6 is further restricted to 2000::/3 global unicast space.
_DENIED_IPV4_NETWORKS: tuple[ipaddress.IPv4Network, ...] = tuple(
    ipaddress.IPv4Network(cidr)
    for cidr in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "168.63.129.16/32",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.31.196.0/24",
        "192.52.193.0/24",
        "192.88.99.0/24",
        "192.168.0.0/16",
        "192.175.48.0/24",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
    )
)

_IPV6_GLOBAL_UNICAST = ipaddress.IPv6Network("2000::/3")

_DENIED_IPV6_NETWORKS: tuple[ipaddress.IPv6Network, ...] = tuple(
    ipaddress.IPv6Network(cidr)
    for cidr in (
        "2001::/23",       # IETF protocol assignments, including Teredo.
        "2002::/16",       # 6to4 embeds an IPv4 endpoint.
        "2001:db8::/32",   # Documentation.
        "3fff::/20",       # Documentation (RFC 9637).
        "64:ff9b::/96",    # IPv4/IPv6 translation.
        "64:ff9b:1::/48",  # IPv4/IPv6 translation.
    )
)

_METADATA_HOSTNAMES: tuple[str, ...] = (
    "localhost",
    "metadata",
    "metadata.google.internal",
    "metadata.google",
    "instance-data",
    "instance-data.ec2.internal",
    "metadata.tencentyun.com",
    "metadata.azure.com",
    "metadata.aws.internal",
)

_ALLOWED_DNS_CHARS: frozenset[str] = frozenset(
    string.ascii_letters + string.digits + "-."
)
_HEX_DIGITS: frozenset[str] = frozenset(string.hexdigits)


def is_public_address(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address | str,
) -> bool:
    """Return True only when address is a non-scoped, globally routable public IP."""
    if isinstance(address, str):
        if "%" in address:
            return False
        try:
            parsed: ipaddress.IPv4Address | ipaddress.IPv6Address = ipaddress.ip_address(
                address
            )
        except ValueError:
            return False
    elif isinstance(address, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        parsed = address
    else:
        return False

    if isinstance(parsed, ipaddress.IPv6Address):
        if parsed.scope_id is not None:
            return False
        if parsed.ipv4_mapped is not None:
            parsed = parsed.ipv4_mapped

    if isinstance(parsed, ipaddress.IPv4Address):
        if any(parsed in network for network in _DENIED_IPV4_NETWORKS):
            return False
        return (
            parsed.is_global
            and not parsed.is_loopback
            and not parsed.is_link_local
            and not parsed.is_multicast
            and not parsed.is_unspecified
        )

    if parsed not in _IPV6_GLOBAL_UNICAST:
        return False
    if any(parsed in network for network in _DENIED_IPV6_NETWORKS):
        return False
    return (
        parsed.is_global
        and not parsed.is_loopback
        and not parsed.is_link_local
        and not parsed.is_multicast
        and not parsed.is_unspecified
        and not parsed.is_site_local
    )


def is_legacy_ipv4_form(name: str) -> bool:
    """Return True when name matches a 1-to-4 component decimal/octal/hex IPv4 grammar.

    Rejects legacy/ambiguous numeric forms such as ``127.1``, ``0177.0.0.1``,
    ``0x7f000001``, or ``127.0x0.0.1`` while allowing ordinary DNS names whose
    labels merely contain ``0x`` (e.g., ``0x.org`` or ``0xdeadbeef.example.com``).
    """
    components = name.split(".")
    if not (1 <= len(components) <= 4):
        return False
    for component in components:
        if not component:
            return False
        if component.isascii() and component.isdigit():
            continue
        if len(component) < 3 or component[0] != "0" or component[1] not in {"x", "X"}:
            return False
        if not all(ch in _HEX_DIGITS for ch in component[2:]):
            return False
    return True


def valid_dns_name(host: str) -> bool:
    """Return True when host is a syntactically valid ASCII DNS name and not a legacy IP."""
    name = host.removesuffix(".")
    if not name or len(name) > 253:
        return False
    if not all(ch in _ALLOWED_DNS_CHARS for ch in name):
        return False
    if is_legacy_ipv4_form(name):
        return False
    for label in name.split("."):
        if not label or len(label) > 63 or label.startswith("-") or label.endswith("-"):
            return False
    return True


def is_metadata_hostname(host: str) -> bool:
    """Return True when host matches a known local or cloud metadata hostname."""
    normalized = host.lower().removesuffix(".")
    if not normalized:
        return False
    for blocked in _METADATA_HOSTNAMES:
        if normalized == blocked or normalized.endswith("." + blocked):
            return True
    return False


def valid_dial_port(port: str | int) -> bool:
    """Return True only when port is standard HTTP (80) or HTTPS (443)."""
    if isinstance(port, bool):
        return False
    if isinstance(port, int):
        return port in {80, 443}
    return port in {"80", "443"}


def valid_dial_host(host: str) -> bool:
    """Return True when a dial target host is an unbracketed, un-scoped IP or hostname."""
    if not host or "%" in host:
        return False
    try:
        parsed = ipaddress.ip_address(host)
    except ValueError:
        return not any(ch in host for ch in ":[]")
    return not (isinstance(parsed, ipaddress.IPv6Address) and parsed.scope_id is not None)


def _reject_url(message: str = "URL rejected by scraper policy") -> ScrapeError:
    cause = SecurityRejectedError(ERR_SECURITY_REJECTED_MESSAGE)
    return ScrapeError(
        kind=ScrapeErrorKind.SECURITY_REJECTED,
        stage=ScrapeStage.VALIDATION,
        message=message,
        cause=cause,
    )


def _extract_raw_host_and_port(netloc: str) -> tuple[str, str | None] | None:
    """Parse netloc strictly without userinfo or ambiguous bracket/port syntax."""
    if not netloc or "@" in netloc:
        return None
    if netloc.startswith("["):
        closing = netloc.find("]")
        if closing == -1:
            return None
        host_literal = netloc[1:closing]
        rest = netloc[closing + 1 :]
        if not host_literal:
            return None
        if not rest:
            return host_literal, None
        if not rest.startswith(":"):
            return None
        port_str = rest[1:]
        if not port_str:
            return None
        return host_literal, port_str

    if "]" in netloc:
        return None
    if ":" in netloc:
        host_part, sep, port_str = netloc.partition(":")
        if not sep or ":" in port_str or not host_part or not port_str:
            return None
        return host_part, port_str
    return netloc, None


def validate_url_policy(raw_url: str) -> SplitResult:
    """Enforce scraper URL syntax, authority, standard port, and IP/DNS policy.

    Does not perform DNS resolution or mutate the input string or query parameters.
    Raises ``ScrapeError(SECURITY_REJECTED, VALIDATION)`` with ``SecurityRejectedError``
    as ``__cause__`` on any violation.
    """
    if not isinstance(raw_url, str) or not raw_url:
        raise _reject_url()
    if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw_url):
        raise _reject_url()

    try:
        parsed = urlsplit(raw_url)
    except ValueError:
        raise _reject_url() from SecurityRejectedError(ERR_SECURITY_REJECTED_MESSAGE)

    if parsed.scheme not in {"http", "https"}:
        raise _reject_url()
    if not raw_url.lower().startswith(f"{parsed.scheme}://"):
        raise _reject_url()
    if parsed.username is not None or parsed.password is not None:
        raise _reject_url()

    extracted = _extract_raw_host_and_port(parsed.netloc)
    if extracted is None:
        raise _reject_url()
    raw_host, raw_port = extracted

    if "%" in raw_host:
        raise _reject_url()

    if raw_port is not None:
        if parsed.scheme == "http" and raw_port != "80":
            raise _reject_url()
        if parsed.scheme == "https" and raw_port != "443":
            raise _reject_url()

    try:
        _ = parsed.port
    except ValueError:
        raise _reject_url() from SecurityRejectedError(ERR_SECURITY_REJECTED_MESSAGE)

    host = parsed.hostname
    if not host or "%" in host:
        raise _reject_url()

    try:
        ip_literal = ipaddress.ip_address(raw_host)
    except ValueError:
        ip_literal = None

    if ip_literal is not None:
        if not is_public_address(ip_literal):
            raise _reject_url()
        return parsed

    if parsed.netloc.startswith("["):
        raise _reject_url()

    if not valid_dns_name(raw_host) or is_metadata_hostname(raw_host):
        raise _reject_url()

    return parsed
