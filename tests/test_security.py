"""
Login (signed tokens, scrypt passwords), TLS, and the PROXY protocol.
"""
import asyncio
import base64
import json
import os
import shutil
import socket
import ssl
import struct
import time

import pytest

from app.client.client import ChatClient
from app.client.common import Credentials
from app.core.auth import (AuthError, UserStore, hash_password, issue_token, verify_password,
                           verify_token)
from app.core.config import ClientConfig
from app.server import proxy_protocol as pp
from tests.support import SECRET, Client, make_config, running

NOW = 1_700_000_000


def b64(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


# ------------------------------------------------------------------ tokens

def test_token_round_trip_and_claims():
    token = issue_token("alice", SECRET, ttl_seconds=60, issuer="login", now=NOW)
    claims = verify_token(token, SECRET, issuer="login", now=NOW + 30)
    assert claims["sub"] == "alice" and claims["aud"] == "chat" and claims["exp"] == NOW + 60


@pytest.mark.parametrize("mutate, reason", [
    (lambda t: t[:-2] + ("A" if t[-2] != "A" else "B") + t[-1], "signature"),
    (lambda t: t.rsplit(".", 1)[0], "segments"),
    (lambda t: "", "missing"),
    (lambda t: "a" * 5000, "oversized"),
    (lambda t: "!!!." + t.split(".", 1)[1], "malformed"),
])
def test_token_rejects_tampering(mutate, reason):
    token = issue_token("alice", SECRET, now=NOW)
    with pytest.raises(AuthError):
        verify_token(mutate(token), SECRET, now=NOW)


def test_token_rejects_other_algorithms_and_secrets():
    header, body, _ = issue_token("alice", SECRET, now=NOW).split(".")
    # alg "none" with an empty signature: the classic bypass.
    with pytest.raises(AuthError, match="algorithm"):
        verify_token(f"{b64({'alg': 'none', 'typ': 'JWT'})}.{body}.", SECRET, now=NOW)
    with pytest.raises(AuthError, match="algorithm"):
        verify_token(f"{b64({'alg': 'HS512'})}.{body}.sig", SECRET, now=NOW)
    with pytest.raises(AuthError, match="signature"):
        verify_token(issue_token("alice", "y" * 40, now=NOW), SECRET, now=NOW)


def test_token_time_audience_issuer_and_subject_checks():
    token = issue_token("alice", SECRET, ttl_seconds=60, issuer="login", now=NOW)
    assert verify_token(token, SECRET, now=NOW + 60 + 29)          # inside the 30 s leeway
    with pytest.raises(AuthError, match="expired"):
        verify_token(token, SECRET, now=NOW + 60 + 31)
    with pytest.raises(AuthError, match="future"):
        verify_token(token, SECRET, now=NOW - 120)
    early = issue_token("alice", SECRET, now=NOW, extra={"nbf": NOW + 300})
    with pytest.raises(AuthError, match="not yet"):
        verify_token(early, SECRET, now=NOW)
    with pytest.raises(AuthError, match="audience"):
        verify_token(token, SECRET, audience="admin", now=NOW)
    multi = issue_token("alice", SECRET, now=NOW, extra={"aud": ["chat", "admin"]})
    assert verify_token(multi, SECRET, audience="admin", now=NOW)["sub"] == "alice"
    with pytest.raises(AuthError, match="issuer"):
        verify_token(token, SECRET, issuer="someone-else", now=NOW)
    with pytest.raises(AuthError, match="subject"):
        verify_token(issue_token("bad name!", SECRET, now=NOW), SECRET, now=NOW)
    no_expiry = issue_token("alice", SECRET, now=NOW, extra={"exp": None})
    with pytest.raises(AuthError, match="expiry"):
        verify_token(no_expiry, SECRET, now=NOW)


# --------------------------------------------------------------- passwords

def test_scrypt_hashes_are_salted_and_verify():
    a, b = hash_password("correct horse"), hash_password("correct horse")
    assert a != b and a.startswith("scrypt$16384$8$1$")
    assert verify_password("correct horse", a) and verify_password("correct horse", b)
    assert not verify_password("wrong horse", a)
    assert not verify_password("x", "not-a-hash") and not verify_password("x", "md5$1$2$3$4$5")


def test_user_store_reloads_and_hides_unknown_users(tmp_path):
    path = str(tmp_path / "users.json")
    store = UserStore(path)
    assert not store.check("alice", "pw-123456")             # no file yet
    store.set_password("alice", "pw-123456")
    assert store.check("alice", "pw-123456") and store.check("ALICE", "pw-123456")
    assert not store.check("alice", "nope") and not store.check("bob", "pw-123456")
    other = UserStore(path)
    other.set_password("bob", "pw-abcdefg")
    later = time.time() + 5
    os.utime(path, (later, later))                            # make the change visible to mtime
    assert store.check("bob", "pw-abcdefg")
    assert not store.check("alice", "x" * 2000)              # oversized password refused cheaply
    with pytest.raises(ValueError):
        store.set_password("bad name!", "pw-123456")


# ------------------------------------------------------------------ config

@pytest.mark.parametrize("overrides, message", [
    (dict(auth_mode="magic"), "open, token or password"),
    (dict(auth_mode="token", allow_plaintext_auth=True), "CHAT_JWT_SECRET"),
    (dict(auth_mode="token", jwt_secret="short", allow_plaintext_auth=True), "32 bytes"),
    (dict(auth_mode="password", allow_plaintext_auth=True), "CHAT_USERS_FILE"),
    (dict(tls_cert="a.pem"), "both"),
    (dict(auth_mode="token", jwt_secret=SECRET), "requires TLS"),
    (dict(proxy_protocol=True), "CHAT_TRUSTED_PROXIES"),
    (dict(proxy_protocol=True, trusted_proxies="bad name!"), "CHAT_TRUSTED_PROXIES"),
    (dict(proxy_protocol=True, trusted_proxies="10.0.0.1", tls_cert="a", tls_key="b"), "not both"),
])
def test_config_refuses_unsafe_or_broken_settings(overrides, message):
    with pytest.raises(ValueError, match=message):
        make_config(**overrides).validate()


def test_config_defaults_are_valid():
    make_config().validate()
    make_config(auth_mode="token", jwt_secret=SECRET, allow_plaintext_auth=True).validate()


# ------------------------------------------------------ login over the wire

async def test_token_login_takes_the_name_from_the_token():
    async with running(auth_mode="token", jwt_secret=SECRET, allow_plaintext_auth=True) as srv:
        c = await Client.open(srv.test_port)
        reply = await c.login(token=issue_token("alice", SECRET), username="mallory")
        assert reply["type"] == "AUTH_OK" and reply["username"] == "alice"
        await c.close()

        for bad in (issue_token("bob", "z" * 40), issue_token("bob", SECRET, ttl_seconds=-120), None):
            c = await Client.open(srv.test_port)
            reply = await c.login(token=bad, username="bob")
            assert reply == {"type": "ERROR", "message": "Invalid credentials"}
            assert await c.expect_closed()
        assert srv.metrics.metrics.auth_failures == 3


async def test_password_login_gives_one_answer_for_every_failure(tmp_path):
    users = str(tmp_path / "users.json")
    UserStore(users).set_password("alice", "s3cret-pass")
    async with running(auth_mode="password", users_file=users, allow_plaintext_auth=True) as srv:
        c = await Client.open(srv.test_port)
        assert (await c.login(username="alice", password="s3cret-pass"))["type"] == "AUTH_OK"
        await c.close()
        replies = []
        for name, password in (("alice", "wrong"), ("nobody", "s3cret-pass"), ("alice", None)):
            c = await Client.open(srv.test_port)
            replies.append(await c.login(username=name, password=password))
            await c.close()
        assert all(r == {"type": "ERROR", "message": "Invalid credentials"} for r in replies)


async def test_login_attempts_are_limited_per_ip():
    async with running(auth_mode="token", jwt_secret=SECRET, allow_plaintext_auth=True,
                       max_auth_attempts_per_minute_per_ip=2) as srv:
        replies = []
        for _ in range(3):
            c = await Client.open(srv.test_port)
            replies.append(await c.login(token="garbage"))
            await c.close()
        assert [r["message"] for r in replies] == [
            "Invalid credentials", "Invalid credentials", "Too many login attempts; try again later"]


# --------------------------------------------------------------------- TLS

openssl = shutil.which("openssl")
needs_openssl = pytest.mark.skipif(openssl is None, reason="needs the openssl command")


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    from scripts.init_secrets import make_certs
    directory = str(tmp_path_factory.mktemp("certs"))
    make_certs(directory, "localhost,127.0.0.1", openssl=openssl)
    return directory


def client_context(certs):
    return ssl.create_default_context(cafile=os.path.join(certs, "ca.pem"))


@needs_openssl
async def test_tls_end_to_end_with_the_cli_client(certs):
    async with running(auth_mode="token", jwt_secret=SECRET,
                       tls_cert=os.path.join(certs, "server.pem"),
                       tls_key=os.path.join(certs, "server.key")) as srv:
        config = ClientConfig(tls=True, tls_ca=os.path.join(certs, "ca.pem"), tls_server_name="localhost")
        cli = ChatClient("127.0.0.1", srv.test_port, config)
        await cli.connect()
        assert cli.writer.get_extra_info("ssl_object").version() in ("TLSv1.2", "TLSv1.3")
        assert await cli.authenticate(Credentials(token=issue_token("alice", SECRET)))
        assert cli.username == "alice"

        bob = await Client.open(srv.test_port, ssl=client_context(certs), server_hostname="localhost")
        assert (await bob.login(token=issue_token("bob", SECRET)))["type"] == "AUTH_OK"
        await bob.join("tls-room")
        await cli._cmd_join("tls-room")
        frames = await bob.recv_until("USER_JOINED")
        assert frames[-1]["username"] == "alice"
        await cli._send_message("over tls")
        assert (await bob.recv())["text"] == "over tls"
        await bob.close()
        await cli.disconnect()


@needs_openssl
async def test_tls_rejects_plain_clients_and_untrusted_names(certs):
    async with running(tls_cert=os.path.join(certs, "server.pem"),
                       tls_key=os.path.join(certs, "server.key")) as srv:
        # Plain TCP to a TLS port: the handshake fails, no protocol frames.
        plain = await Client.open(srv.test_port)
        await plain.send({"type": "AUTH", "username": "eve"})
        with pytest.raises(Exception):
            await plain.recv(3)
        await plain.close()
        # A client that does not trust the private CA refuses the server.
        with pytest.raises(ssl.SSLCertVerificationError):
            await Client.open(srv.test_port, ssl=ssl.create_default_context(), server_hostname="localhost")
        # The certificate is for localhost/127.0.0.1, not another name.
        with pytest.raises(ssl.SSLCertVerificationError):
            await Client.open(srv.test_port, ssl=client_context(certs), server_hostname="chat.example.com")


# ---------------------------------------------------------- PROXY protocol

def test_proxy_v1_parsing():
    assert pp.parse_v1(b"PROXY TCP4 203.0.113.7 10.0.0.1 51234 8888\r\n") == ("203.0.113.7", 51234)
    assert pp.parse_v1(b"PROXY TCP6 2001:db8::1 2001:db8::2 1 2\r\n") == ("2001:db8::1", 1)
    assert pp.parse_v1(b"PROXY UNKNOWN\r\n") is None
    for bad in (b"PROXY TCP4 203.0.113.7 10.0.0.1 51234 8888\n",       # no CR
                b"PROXY TCP4 999.0.0.1 10.0.0.1 1 2\r\n",              # bad address
                b"PROXY TCP4 2001:db8::1 10.0.0.1 1 2\r\n",            # wrong family
                b"PROXY TCP4 1.2.3.4 10.0.0.1 070 2\r\n",              # leading zero
                b"PROXY TCP4 1.2.3.4 10.0.0.1 70000 2\r\n",            # out of range
                b"PROXY UDP4 1.2.3.4 10.0.0.1 1 2\r\n",
                b"PROXY TCP4 " + b"1" * 120 + b"\r\n"):
        with pytest.raises(pp.ProxyProtocolError):
            pp.parse_v1(bad)


def v2_header(command=0x1, family=0x11, body=b""):
    return pp.V2_SIGNATURE + bytes([0x20 | command, family]) + struct.pack("!H", len(body)) + body


def test_proxy_v2_parsing():
    ipv4 = socket.inet_aton("198.51.100.9") + socket.inet_aton("10.0.0.1") + struct.pack("!HH", 4242, 8888)
    head = v2_header(body=ipv4)
    assert pp.parse_v2(head[:16], head[16:]) == ("198.51.100.9", 4242)
    ipv6 = (socket.inet_pton(socket.AF_INET6, "2001:db8::7") + socket.inet_pton(socket.AF_INET6, "::1")
            + struct.pack("!HH", 7, 8888))
    head = v2_header(family=0x21, body=ipv6)
    assert pp.parse_v2(head[:16], head[16:]) == ("2001:db8::7", 7)
    assert pp.parse_v2(v2_header(command=0x0), b"") is None          # LOCAL (health check)
    with pytest.raises(pp.ProxyProtocolError):
        pp.parse_v2(v2_header(), b"\x00" * 4)                         # truncated


async def test_trusted_proxies_by_address_and_by_name():
    trusted = pp.TrustedProxies("10.0.0.0/8, 192.0.2.10, localhost")
    assert [str(n) for n in trusted.networks] == ["10.0.0.0/8", "192.0.2.10/32"]
    assert trusted.names == ["localhost"]
    assert await trusted.contains("10.9.9.9") and await trusted.contains("127.0.0.1")  # localhost resolves
    assert not await trusted.contains("203.0.113.1")
    with pytest.raises(ValueError):
        pp.split_spec("ok-name, bad name!")


def test_trusted_networks():
    nets = pp.parse_networks("10.0.0.0/8, 172.25.0.10")
    assert pp.is_trusted("10.1.2.3", nets) and pp.is_trusted("172.25.0.10", nets)
    assert pp.is_trusted("::ffff:10.1.2.3", nets)
    assert not pp.is_trusted("172.25.0.11", nets) and not pp.is_trusted(None, nets)
    assert not pp.is_trusted("garbage", nets)


async def proxied(port, header: bytes) -> Client:
    c = await Client.open(port)
    c.writer.write(header)
    await c.writer.drain()
    return c


async def test_server_uses_the_client_address_from_the_proxy():
    async with running(proxy_protocol=True, trusted_proxies="127.0.0.1",
                       auth_mode="token", jwt_secret=SECRET, allow_plaintext_auth=True,
                       max_auth_attempts_per_minute_per_ip=1) as srv:
        c = await proxied(srv.test_port, b"PROXY TCP4 203.0.113.7 10.0.0.1 5555 8888\r\n")
        assert (await c.login(token=issue_token("alice", SECRET)))["type"] == "AUTH_OK"
        assert [conn.ip for conn in srv._connections] == ["203.0.113.7"]
        await c.close()

        # The login limit is per real client: 203.0.113.7 has used its one
        # attempt, a different client behind the same proxy has not.
        c = await proxied(srv.test_port, b"PROXY TCP4 203.0.113.7 10.0.0.1 5556 8888\r\n")
        assert (await c.login(token="x"))["message"].startswith("Too many login attempts")
        await c.close()
        head = v2_header(body=socket.inet_aton("198.51.100.9") + socket.inet_aton("10.0.0.1")
                         + struct.pack("!HH", 1, 8888))
        c = await proxied(srv.test_port, head)
        assert (await c.login(token=issue_token("bob", SECRET)))["type"] == "AUTH_OK"
        await c.close()


async def test_server_refuses_untrusted_or_missing_proxy_headers():
    async with running(proxy_protocol=True, trusted_proxies="localhost") as srv:
        c = await Client.open(srv.test_port)            # no header: protocol frame first
        await c.send({"type": "AUTH", "username": "eve"})
        assert await c.expect_closed()
    async with running(proxy_protocol=True, trusted_proxies="10.9.9.9") as srv:
        c = await proxied(srv.test_port, b"PROXY TCP4 1.2.3.4 10.0.0.1 1 2\r\n")
        assert await c.expect_closed()                  # 127.0.0.1 is not a trusted proxy
        assert srv.metrics.metrics.failed_connections >= 1
