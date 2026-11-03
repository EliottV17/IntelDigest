"""Unit tests for safe scraper network layer: coupled resolution, IP pinning, TLS, and aiohttp connector."""

import asyncio
import ipaddress
import os
import ssl
import subprocess
import tempfile
import time
from typing import Any

import aiohttp
import pytest

from inteldigest.scraper.models import ScrapeError, ScrapeErrorKind, ScrapeStage
from inteldigest.scraper.network import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    SafeDialer,
    classify_network_error,
    create_safe_client_session,
    create_safe_connector,
    create_tls_context,
    dial_public_address,
    split_dial_address,
    unmap_ip_address,
)
from inteldigest.scraper.policy import SecurityRejectedError

# ---------------------------------------------------------------------------
# Helpers & fixtures
# ---------------------------------------------------------------------------


def _generate_self_signed_cert(
    temp_dir: str, hostname: str = "news.example"
) -> tuple[str, str]:
    """Generate a self-signed RSA certificate for hostname using OpenSSL CLI."""
    key_path = os.path.join(temp_dir, "key.pem")
    cert_path = os.path.join(temp_dir, "cert.pem")
    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        key_path,
        "-out",
        cert_path,
        "-days",
        "1",
        "-subj",
        f"/CN={hostname}",
        "-addext",
        f"subjectAltName=DNS:{hostname}",
    ]
    subprocess.check_call(cmd, stderr=subprocess.DEVNULL)
    return cert_path, key_path


# ---------------------------------------------------------------------------
# Unit tests: Address helpers & parsing
# ---------------------------------------------------------------------------


def test_unmap_ip_address() -> None:
    mapped = ipaddress.ip_address("::ffff:8.8.8.8")
    assert unmap_ip_address(mapped) == ipaddress.IPv4Address("8.8.8.8")

    regular_v4 = ipaddress.ip_address("8.8.8.8")
    assert unmap_ip_address(regular_v4) == regular_v4

    regular_v6 = ipaddress.ip_address("2001:4860:4860::8888")
    assert unmap_ip_address(regular_v6) == regular_v6


def test_split_dial_address_valid() -> None:
    assert split_dial_address("news.example:443") == ("news.example", "443")
    assert split_dial_address("8.8.8.8:80") == ("8.8.8.8", "80")
    assert split_dial_address("[::ffff:8.8.8.8]:443") == ("::ffff:8.8.8.8", "443")
    assert split_dial_address("[2001:4860:4860::8888]:80") == (
        "2001:4860:4860::8888",
        "80",
    )


def test_split_dial_address_invalid() -> None:
    for invalid in [
        "news.example",
        "news.example:",
        ":80",
        "[::1",
        "[::1]:",
        "[::1]80",
        "",
        "http://news.example:80",
    ]:
        with pytest.raises(ValueError):
            split_dial_address(invalid)


# ---------------------------------------------------------------------------
# Unit tests: Coupled resolution and IP pinning (SafeDialer / resolve_and_validate)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_safe_dialer_pins_approved_address_and_shares_deadline() -> None:
    resolved_time: float = 0.0
    dialed_address: str = ""
    dial_time: float = 0.0

    async def mock_resolver(host: str, port: int) -> list[str]:
        nonlocal resolved_time
        resolved_time = time.monotonic()
        assert host == "news.example"
        return ["8.8.8.8"]

    async def mock_dialer(
        host: str,
        port: int,
        *,
        ssl: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> tuple[str, int]:
        nonlocal dialed_address, dial_time
        dial_time = time.monotonic()
        dialed_address = f"{host}:{port}"
        assert server_hostname == "news.example"
        return ("mock_reader", 42)  # type: ignore[return-value]

    dialer = SafeDialer(
        connect_timeout_seconds=5.0,
        resolver=mock_resolver,
        dialer=mock_dialer,
    )

    result = await dialer.dial("news.example:443", ssl=create_tls_context())
    assert result == ("mock_reader", 42)
    assert dialed_address == "8.8.8.8:443"
    assert dial_time >= resolved_time


@pytest.mark.asyncio
async def test_safe_dialer_literal_ip_without_resolver() -> None:
    resolver_called = False
    dialed_address = ""

    async def mock_resolver(host: str, port: int) -> list[str]:
        nonlocal resolver_called
        resolver_called = True
        return ["8.8.8.8"]

    async def mock_dialer(host: str, port: int, **kwargs: Any) -> str:
        nonlocal dialed_address
        dialed_address = f"{host}:{port}"
        return "connected"

    dialer = SafeDialer(
        resolver=mock_resolver,
        dialer=mock_dialer,
    )

    # Public IPv4 literal
    res1 = await dialer.dial("8.8.8.8:80")
    assert res1 == "connected"
    assert dialed_address == "8.8.8.8:80"
    assert not resolver_called

    # Public mapped IPv6 literal unmaps to IPv4 literal
    res2 = await dialer.dial("[::ffff:8.8.8.8]:443")
    assert res2 == "connected"
    assert dialed_address == "8.8.8.8:443"
    assert not resolver_called


@pytest.mark.asyncio
async def test_safe_dialer_rejects_blocked_literal_without_dial() -> None:
    dial_calls = 0

    async def mock_dialer(host: str, port: int, **kwargs: Any) -> str:
        nonlocal dial_calls
        dial_calls += 1
        return "connected"

    dialer = SafeDialer(dialer=mock_dialer)

    blocked_targets = [
        "127.0.0.1:443",
        "10.0.0.1:80",
        "169.254.169.254:80",
        "168.63.129.16:80",
        "[::1]:80",
        "[fc00::1]:443",
        "[::ffff:127.0.0.1]:80",
    ]

    for target in blocked_targets:
        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial(target)
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.FETCH
        assert isinstance(err.cause, SecurityRejectedError)
        assert dial_calls == 0


@pytest.mark.asyncio
async def test_safe_dialer_rejects_blocked_or_mixed_dns_before_dial() -> None:
    dns_cases = [
        ["127.0.0.1"],
        ["8.8.8.8", "10.0.0.1"],
        ["10.0.0.1", "8.8.8.8"],
        [],
        ["169.254.169.254"],
    ]

    for answers in dns_cases:
        dials = 0

        async def mock_resolver(
            host: str, port: int, current_answers: list[str] = answers
        ) -> list[str]:
            return current_answers

        async def mock_dialer(host: str, port: int, **kwargs: Any) -> str:
            nonlocal dials
            dials += 1
            return "connected"

        dialer = SafeDialer(resolver=mock_resolver, dialer=mock_dialer)

        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial("news.example:443")
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.FETCH
        assert isinstance(err.cause, SecurityRejectedError)
        assert dials == 0


@pytest.mark.asyncio
async def test_safe_dialer_resolves_fresh_and_does_not_redial_blocked_rebinding() -> (
    None
):
    lookups = 0
    dials = 0

    async def mock_resolver(host: str, port: int) -> list[str]:
        nonlocal lookups
        lookups += 1
        if lookups == 1:
            return ["8.8.8.8"]
        return ["127.0.0.1"]

    async def mock_dialer(host: str, port: int, **kwargs: Any) -> str:
        nonlocal dials
        dials += 1
        assert host == "8.8.8.8"
        return "connected"

    dialer = SafeDialer(resolver=mock_resolver, dialer=mock_dialer)

    # First connection succeeds to 8.8.8.8
    res = await dialer.dial("news.example:443")
    assert res == "connected"
    assert lookups == 1
    assert dials == 1

    # Second connection resolves fresh, gets 127.0.0.1, and rejects before dialing
    with pytest.raises(ScrapeError) as exc_info:
        await dialer.dial("news.example:443")
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
    assert err.stage == ScrapeStage.FETCH
    assert lookups == 2
    assert dials == 1


@pytest.mark.asyncio
async def test_hex_like_dns_names_resolve_through_vetted_literal() -> None:
    hosts = [
        "0x.org",
        "100x.com",
        "0xdata.com",
        "fox0x.com",
        "node-0x1a.example.com",
        "0xdeadbeef.example.com",
        "0x123.example.com",
        "0XDEADBEEF.Example.COM.",
        "0X.Org.",
    ]

    for host in hosts:
        lookups = 0
        dials = 0

        async def mock_resolver(
            got_host: str, port: int, expected_host: str = host
        ) -> list[str]:
            nonlocal lookups
            lookups += 1
            assert got_host.rstrip(".").lower() == expected_host.rstrip(".").lower()
            return ["8.8.8.8"]

        async def mock_dialer(dial_host: str, port: int, **kwargs: Any) -> str:
            nonlocal dials
            dials += 1
            assert dial_host == "8.8.8.8"
            return "ok"

        dialer = SafeDialer(resolver=mock_resolver, dialer=mock_dialer)
        res = await dialer.dial(f"{host}:443")
        assert res == "ok"
        assert lookups == 1
        assert dials == 1


@pytest.mark.asyncio
async def test_legacy_encoded_ipv4_forms_reject_before_dns_or_dial() -> None:
    hosts = [
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
        "127.1",
        "0177.0.0.1",
    ]

    for host in hosts:
        lookups = 0
        dials = 0

        async def mock_resolver(got_host: str, port: int) -> list[str]:
            nonlocal lookups
            lookups += 1
            return ["8.8.8.8"]

        async def mock_dialer(dial_host: str, port: int, **kwargs: Any) -> str:
            nonlocal dials
            dials += 1
            return "ok"

        dialer = SafeDialer(resolver=mock_resolver, dialer=mock_dialer)

        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial(f"{host}:80")
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.FETCH
        assert lookups == 0
        assert dials == 0


@pytest.mark.asyncio
async def test_azure_wire_server_address_boundary() -> None:
    for octet in ["168.63.129.15", "168.63.129.17", "168.63.0.1"]:
        dials = 0

        async def mock_dialer(
            host: str, port: int, expected_octet: str = octet, **kwargs: Any
        ) -> str:
            nonlocal dials
            dials += 1
            assert host == expected_octet
            return "ok"

        dialer = SafeDialer(dialer=mock_dialer)
        res = await dialer.dial(f"{octet}:80")
        assert res == "ok"
        assert dials == 1

    # Exact WireServer 168.63.129.16 rejected
    dials_ws = 0

    async def mock_dialer_ws(host: str, port: int, **kwargs: Any) -> str:
        nonlocal dials_ws
        dials_ws += 1
        return "ok"

    dialer_ws = SafeDialer(dialer=mock_dialer_ws)
    with pytest.raises(ScrapeError) as exc_info:
        await dialer_ws.dial("168.63.129.16:80")
    assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED
    assert dials_ws == 0


@pytest.mark.asyncio
async def test_metadata_hostnames_rejected_before_dns_or_dial() -> None:
    metadata_hosts = [
        "metadata",
        "metadata.google.internal",
        "metadata.google",
        "instance-data",
        "instance-data.ec2.internal",
        "metadata.tencentyun.com",
        "metadata.azure.com",
        "metadata.aws.internal",
        "sub.metadata.google.internal",
    ]

    for host in metadata_hosts:
        lookups = 0
        dials = 0

        async def mock_resolver(got_host: str, port: int) -> list[str]:
            nonlocal lookups
            lookups += 1
            return ["8.8.8.8"]

        async def mock_dialer(dial_host: str, port: int, **kwargs: Any) -> str:
            nonlocal dials
            dials += 1
            return "ok"

        dialer = SafeDialer(resolver=mock_resolver, dialer=mock_dialer)
        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial(f"{host}:80")
        assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert lookups == 0
        assert dials == 0


@pytest.mark.asyncio
async def test_invalid_ports_and_networks_rejected() -> None:
    dialer = SafeDialer()

    # Invalid ports
    for bad_port in ["8080", "8443", "abc", "", "0"]:
        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial(f"news.example:{bad_port}")
        assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED

    # Invalid networks
    for bad_net in ["udp", "unix", "tcp/ip"]:
        with pytest.raises(ScrapeError) as exc_info:
            await dialer.dial("news.example:443", network=bad_net)
        assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED


# ---------------------------------------------------------------------------
# Unit tests: Error classification and secret scrubbing
# ---------------------------------------------------------------------------


def test_classify_network_error_scrubs_secrets() -> None:
    secret_cause = Exception("connect failed to https://private.example/?token=raw_secret_xyz")
    err = classify_network_error(secret_cause)

    assert err.kind == ScrapeErrorKind.NETWORK
    assert err.stage == ScrapeStage.FETCH
    assert str(err) == "scrape network during fetch"
    assert "private.example" not in str(err)
    assert "raw_secret_xyz" not in str(err)
    assert err.cause is secret_cause
    assert err.__cause__ is secret_cause


def test_classify_network_error_timeout_and_canceled() -> None:
    timeout_exc = TimeoutError("deadline expired")
    err_t = classify_network_error(timeout_exc)
    assert err_t.kind == ScrapeErrorKind.TIMEOUT
    assert err_t.stage == ScrapeStage.FETCH
    assert str(err_t) == "scrape timeout during fetch"

    cancel_exc = asyncio.CancelledError("task canceled")
    err_c = classify_network_error(cancel_exc)
    assert err_c.kind == ScrapeErrorKind.CANCELED
    assert err_c.stage == ScrapeStage.FETCH
    assert str(err_c) == "scrape canceled during fetch"


@pytest.mark.asyncio
async def test_safe_dialer_timeout_preserves_cause() -> None:
    async def slow_resolver(host: str, port: int) -> list[str]:
        await asyncio.sleep(0.5)
        return ["8.8.8.8"]

    dialer = SafeDialer(
        connect_timeout_seconds=0.05,
        resolver=slow_resolver,
    )

    with pytest.raises(ScrapeError) as exc_info:
        await dialer.dial("news.example:443")
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.TIMEOUT
    assert err.stage == ScrapeStage.FETCH
    assert isinstance(err.cause, TimeoutError)


@pytest.mark.asyncio
async def test_safe_dialer_cancellation_preserves_cause() -> None:
    async def blocking_resolver(host: str, port: int) -> list[str]:
        await asyncio.Event().wait()
        return ["8.8.8.8"]

    dialer = SafeDialer(resolver=blocking_resolver)

    task = asyncio.create_task(dialer.dial("news.example:443"))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(ScrapeError) as exc_info:
        await task
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.CANCELED
    assert err.stage == ScrapeStage.FETCH
    assert isinstance(err.cause, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Unit tests: TLS context creation and configuration
# ---------------------------------------------------------------------------


def test_create_tls_context_enforces_security_options() -> None:
    ctx = create_tls_context()
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_create_tls_context_with_custom_ca() -> None:
    with tempfile.TemporaryDirectory() as td:
        cert_path, _ = _generate_self_signed_cert(td, "news.example")
        ctx = create_tls_context(cafile=cert_path)
        assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is True


# ---------------------------------------------------------------------------
# Unit tests: Local TLS server with SNI verification and certificate failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tls_sni_and_certificate_verification_with_local_server() -> None:
    with tempfile.TemporaryDirectory() as td:
        cert_path, key_path = _generate_self_signed_cert(td, "news.example")

        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(cert_path, key_path)

        received_sni: list[str | None] = []
        server_ctx.sni_callback = lambda s, name, c: received_sni.append(name)

        async def handle_client(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            writer.write(b"OK\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(
            handle_client, "127.0.0.1", 0, ssl=server_ctx
        )
        local_port = server.sockets[0].getsockname()[1]

        try:
            # 1. Successful connection with trusted root CA and SNI = news.example
            trusted_client_ctx = create_tls_context(cafile=cert_path)

            async def custom_dialer_success(
                host: str,
                port: int,
                *,
                ssl: ssl.SSLContext | None = None,
                server_hostname: str | None = None,
            ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
                assert host == "8.8.8.8"
                assert port == 443
                assert server_hostname == "news.example"
                return await asyncio.open_connection(
                    "127.0.0.1",
                    local_port,
                    ssl=ssl,
                    server_hostname=server_hostname,
                )

            async def mock_resolver(host: str, port: int) -> list[str]:
                assert host == "news.example"
                return ["8.8.8.8"]

            dialer_success = SafeDialer(
                resolver=mock_resolver,
                dialer=custom_dialer_success,
            )

            reader, writer = await dialer_success.dial(
                "news.example:443", ssl=trusted_client_ctx
            )
            data = await reader.read(10)
            writer.close()
            await writer.wait_closed()

            assert data == b"OK\n"
            assert received_sni == ["news.example"]

            # 2. Untrusted certificate failure (default system CA, cert not trusted)
            untrusted_client_ctx = create_tls_context()

            async def custom_dialer_untrusted(
                host: str,
                port: int,
                *,
                ssl: ssl.SSLContext | None = None,
                server_hostname: str | None = None,
            ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
                return await asyncio.open_connection(
                    "127.0.0.1",
                    local_port,
                    ssl=ssl,
                    server_hostname=server_hostname,
                )

            dialer_untrusted = SafeDialer(
                resolver=mock_resolver,
                dialer=custom_dialer_untrusted,
            )

            with pytest.raises(ScrapeError) as exc_info:
                await dialer_untrusted.dial(
                    "news.example:443", ssl=untrusted_client_ctx
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.NETWORK
            assert err.stage == ScrapeStage.FETCH
            assert str(err) == "scrape network during fetch"
            assert isinstance(err.cause, ssl.SSLCertVerificationError)

            # 3. Wrong hostname failure
            async def wrong_resolver(host: str, port: int) -> list[str]:
                return ["8.8.8.8"]

            async def custom_dialer_wrong(
                host: str,
                port: int,
                *,
                ssl: ssl.SSLContext | None = None,
                server_hostname: str | None = None,
            ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
                return await asyncio.open_connection(
                    "127.0.0.1",
                    local_port,
                    ssl=ssl,
                    server_hostname=server_hostname,
                )

            dialer_wrong = SafeDialer(
                resolver=wrong_resolver,
                dialer=custom_dialer_wrong,
            )

            with pytest.raises(ScrapeError) as exc_info_wrong:
                await dialer_wrong.dial(
                    "wrong.example:443", ssl=trusted_client_ctx
                )
            err_wrong = exc_info_wrong.value
            assert err_wrong.kind == ScrapeErrorKind.NETWORK
            assert err_wrong.stage == ScrapeStage.FETCH
            assert isinstance(err_wrong.cause, ssl.SSLCertVerificationError)

        finally:
            server.close()
            await server.wait_closed()


# ---------------------------------------------------------------------------
# Unit tests: aiohttp SafeTCPConnector & ClientSession
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_safe_tcp_connector_disables_proxies_keepalive_and_dns_cache() -> None:
    connector = create_safe_connector()
    assert connector.force_close is True
    assert connector._use_dns_cache is False

    session = create_safe_client_session(connector=connector)
    assert isinstance(session.cookie_jar, aiohttp.DummyCookieJar)
    assert session.trust_env is False

    # Attempting to use a proxy must be rejected
    with pytest.raises(ScrapeError) as exc_info:
        async with session:
            await session.get("http://news.example:80/", proxy="http://127.0.0.1:8080")
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
    assert err.stage == ScrapeStage.FETCH
    assert isinstance(err.cause, SecurityRejectedError)


@pytest.mark.asyncio
async def test_safe_tcp_connector_rejects_ssrf_destinations() -> None:
    connector = create_safe_connector()
    session = create_safe_client_session(connector=connector)

    async with session:
        # Loopback IP
        with pytest.raises(ScrapeError) as exc1:
            await session.get("http://127.0.0.1:80/")
        assert exc1.value.kind == ScrapeErrorKind.SECURITY_REJECTED

        # Cloud metadata hostname
        with pytest.raises(ScrapeError) as exc2:
            await session.get("http://metadata.google.internal:80/")
        assert exc2.value.kind == ScrapeErrorKind.SECURITY_REJECTED

        # Legacy IPv4 hex form
        with pytest.raises(ScrapeError) as exc3:
            await session.get("http://0x7f000001:80/")
        assert exc3.value.kind == ScrapeErrorKind.SECURITY_REJECTED

        # Non-standard port
        with pytest.raises(ScrapeError) as exc4:
            await session.get("http://news.example:8080/")
        assert exc4.value.kind == ScrapeErrorKind.SECURITY_REJECTED


@pytest.mark.asyncio
async def test_safe_tcp_connector_rejects_mixed_dns() -> None:
    async def mock_resolver(host: str, port: int) -> list[str]:
        return ["8.8.8.8", "10.0.0.1"]

    connector = create_safe_connector(resolver=mock_resolver)
    session = create_safe_client_session(connector=connector)

    async with session:
        with pytest.raises(ScrapeError) as exc_info:
            await session.get("http://news.example:80/")
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.FETCH


@pytest.mark.asyncio
async def test_safe_tcp_connector_aiohttp_tls_success_and_sni_verification() -> None:
    with tempfile.TemporaryDirectory() as td:
        cert_path, key_path = _generate_self_signed_cert(td, "news.example")

        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(cert_path, key_path)

        received_sni: list[str | None] = []
        server_ctx.sni_callback = lambda s, name, c: received_sni.append(name)

        async def handle_http(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            line = await reader.readline()
            while line and line != b"\r\n":
                line = await reader.readline()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
            )
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(
            handle_http, "127.0.0.1", 0, ssl=server_ctx
        )
        local_port = server.sockets[0].getsockname()[1]

        try:
            # Inject resolver that returns a valid public IP
            async def mock_resolver(host: str, port: int) -> list[str]:
                assert host == "news.example"
                return ["8.8.8.8"]

            client_tls = create_tls_context(cafile=cert_path)
            connector = create_safe_connector(
                resolver=mock_resolver,
                ssl_context=client_tls,
                test_dial_target=("127.0.0.1", local_port),
            )

            async with create_safe_client_session(connector=connector) as session:
                resp = await session.get("https://news.example:443/article")
                text = await resp.text()
                assert resp.status == 200
                assert text == "OK"
                assert received_sni == ["news.example"]

            # Test untrusted CA fails with ScrapeError(NETWORK, FETCH)
            untrusted_tls = create_tls_context()
            untrusted_connector = create_safe_connector(
                resolver=mock_resolver,
                ssl_context=untrusted_tls,
                test_dial_target=("127.0.0.1", local_port),
            )
            async with create_safe_client_session(
                connector=untrusted_connector
            ) as session:
                with pytest.raises(ScrapeError) as exc_info:
                    await session.get("https://news.example:443/article")
                err = exc_info.value
                assert err.kind == ScrapeErrorKind.NETWORK
                assert err.stage == ScrapeStage.FETCH
                assert str(err) == "scrape network during fetch"

        finally:
            server.close()
            await server.wait_closed()


@pytest.mark.asyncio
async def test_dial_public_address_convenience_function() -> None:
    async def mock_resolver(host: str, port: int) -> list[str]:
        return ["8.8.8.8"]

    async def mock_dialer(host: str, port: int, **kwargs: Any) -> str:
        return f"connected:{host}:{port}"

    result = await dial_public_address(
        "news.example:443",
        resolver=mock_resolver,
        dialer=mock_dialer,
    )
    assert result == "connected:8.8.8.8:443"


def test_constructor_timeout_validation_and_defaults() -> None:
    for invalid_timeout in [-1.0, 0.0, float("nan"), float("inf"), "invalid"]:
        dialer = SafeDialer(connect_timeout_seconds=invalid_timeout)  # type: ignore[arg-type]
        assert dialer._connect_timeout_seconds == DEFAULT_CONNECT_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_resolver_returning_invalid_ip_format_rejected() -> None:
    async def malformed_resolver(host: str, port: int) -> list[str]:
        return ["not_an_ip_address"]

    dialer = SafeDialer(resolver=malformed_resolver)
    with pytest.raises(ScrapeError) as exc_info:
        await dialer.dial("news.example:443")
    assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED
    assert exc_info.value.stage == ScrapeStage.FETCH


@pytest.mark.asyncio
async def test_plaintext_dial_passes_none_server_hostname_to_asyncio() -> None:
    async def handle_echo(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(b"PLAIN_OK")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_echo, "127.0.0.1", 0)
    local_port = server.sockets[0].getsockname()[1]
    try:
        async def mock_resolver(host: str, port: int) -> list[str]:
            return ["8.8.8.8"]

        async def local_plaintext_dialer(
            host: str,
            port: int,
            *,
            ssl: ssl.SSLContext | None = None,
            server_hostname: str | None = None,
        ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            assert host == "8.8.8.8"
            assert port == 80
            assert ssl is None
            assert server_hostname is None
            return await asyncio.open_connection(
                "127.0.0.1", local_port, ssl=ssl, server_hostname=server_hostname
            )

        reader, writer = await dial_public_address(
            "news.example:80",
            resolver=mock_resolver,
            dialer=local_plaintext_dialer,
        )
        data = await reader.read(32)
        assert data == b"PLAIN_OK"
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()

