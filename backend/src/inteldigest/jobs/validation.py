"""Initial non-DNS URL validation for digest job submission."""

import ipaddress
from urllib.parse import urlsplit


def validate_url(raw_url: str) -> None:
    """Validate that raw_url is a well-formed HTTP(S) URL targeting a non-private host.

    Zero DNS resolution is performed at this stage.
    """
    if not raw_url.strip():
        raise ValueError("url: empty")

    if any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in raw_url):
        raise ValueError("url: invalid URL syntax")

    try:
        parsed = urlsplit(raw_url)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError(f"url: invalid URL syntax: {exc}") from None

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f'url: scheme must be http or https, got "{parsed.scheme}"')

    if not parsed.hostname:
        raise ValueError("url: host is empty")

    host = parsed.hostname
    if host in {"localhost", "localhost."}:
        raise ValueError(f'url: private/reserved address "{host}"')

    ip_candidate = host.split("%")[0]
    try:
        ip = ipaddress.ip_address(ip_candidate)
    except ValueError:
        ip = None

    if ip is not None:
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        if (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_unspecified
            or ip.is_reserved
        ):
            raise ValueError(f'url: private/reserved address "{host}"')
