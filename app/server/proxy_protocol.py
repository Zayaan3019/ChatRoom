"""
PROXY protocol (v1 text and v2 binary) for servers behind a TCP load balancer.

A TCP proxy such as nginx `stream`, HAProxy or an AWS NLB opens its own
connection to the server, so the server sees the proxy's address, not the
client's. Every per-IP control (connection caps, the login limit, message
limits) would then lump all users together. With PROXY protocol the proxy
first sends one short header naming the real client:

    v1:  b"PROXY TCP4 203.0.113.7 10.0.0.5 51234 8888\\r\\n"
    v2:  a 16-byte binary header plus the addresses

The header is trusted only from configured proxy addresses; from anyone else
the connection is refused, otherwise any client could claim any IP.
Spec: https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt
"""
import asyncio
import ipaddress
import socket
import struct
from typing import Iterable, List, Optional, Tuple, Union

Network = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]

V2_SIGNATURE = b"\r\n\r\n\x00\r\nQUIT\n"
V1_MAX_LENGTH = 107  # the spec's limit, CRLF included


class ProxyProtocolError(Exception):
    """The header was missing, malformed or from an untrusted peer."""


def parse_networks(spec: Union[str, Iterable[str], None]) -> List[Network]:
    """'10.0.0.0/8, 172.25.0.1' -> networks. A bare address is a /32 or /128."""
    if not spec:
        return []
    items = spec.split(",") if isinstance(spec, str) else list(spec)
    return [ipaddress.ip_network(item.strip(), strict=False) for item in items if item.strip()]


def split_spec(spec: Union[str, Iterable[str], None]) -> Tuple[List[Network], List[str]]:
    """Separate addresses/CIDRs from host names ('nginx', 'lb.internal')."""
    if not spec:
        return [], []
    items = spec.split(",") if isinstance(spec, str) else list(spec)
    networks, names = [], []
    for item in (i.strip() for i in items):
        if not item:
            continue
        try:
            networks.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            if not all(part.isalnum() or "-" in part for part in item.split(".")) or not item[0].isalnum():
                raise ValueError(f"not an address, network or host name: {item!r}") from None
            names.append(item)
    return networks, names


class TrustedProxies:
    """Addresses allowed to send PROXY headers.

    Entries may be addresses, CIDRs or host names. Names are resolved at
    start and again (at most every `refresh` seconds) when an unknown peer
    connects, so a load balancer that restarts with a new address (a
    container, say) is trusted again without a restart. A name never widens
    trust beyond what it resolves to.
    """

    def __init__(self, spec: Union[str, Iterable[str], None], refresh: float = 5.0) -> None:
        self.networks, self.names = split_spec(spec)
        self.refresh = refresh
        self._resolved: List[Network] = []
        self._last = float("-inf")

    async def _resolve(self) -> None:
        loop = asyncio.get_running_loop()
        self._last = loop.time()
        found: List[Network] = []
        for name in self.names:
            try:
                infos = await loop.getaddrinfo(name, None, type=socket.SOCK_STREAM)
            except OSError:
                continue  # not resolvable right now: trust nothing for it
            for info in infos:
                found.append(ipaddress.ip_network(info[4][0], strict=False))
        self._resolved = found

    async def contains(self, ip: Optional[str]) -> bool:
        if is_trusted(ip, self.networks) or is_trusted(ip, self._resolved):
            return True
        if self.names and asyncio.get_running_loop().time() - self._last >= self.refresh:
            await self._resolve()
            return is_trusted(ip, self._resolved)
        return False


def is_trusted(ip: Optional[str], networks: List[Network]) -> bool:
    if not ip:
        return False
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    if address.version == 6 and address.ipv4_mapped:  # ::ffff:10.0.0.1
        address = address.ipv4_mapped
    return any(address in net for net in networks)


def _valid_ip(text: str, version: int) -> str:
    try:
        address = ipaddress.ip_address(text)
    except ValueError as e:
        raise ProxyProtocolError(f"bad address {text!r}") from e
    if address.version != version:
        raise ProxyProtocolError(f"address {text!r} is not IPv{version}")
    return str(address)


def _valid_port(text: str) -> int:
    if not text.isdigit() or (len(text) > 1 and text[0] == "0") or not 0 <= int(text) <= 65535:
        raise ProxyProtocolError(f"bad port {text!r}")
    return int(text)


def parse_v1(line: bytes) -> Optional[Tuple[str, int]]:
    """Parse one v1 line (with CRLF). None for 'PROXY UNKNOWN' (a health
    check from the proxy itself: keep the socket's own address)."""
    if not line.endswith(b"\r\n") or len(line) > V1_MAX_LENGTH:
        raise ProxyProtocolError("v1 header too long or not CRLF-terminated")
    try:
        parts = line[:-2].decode("ascii").split(" ")
    except UnicodeDecodeError as e:
        raise ProxyProtocolError("v1 header is not ASCII") from e
    if parts[0] != "PROXY" or len(parts) < 2:
        raise ProxyProtocolError("not a PROXY header")
    if parts[1] == "UNKNOWN":
        return None
    if parts[1] not in ("TCP4", "TCP6") or len(parts) != 6:
        raise ProxyProtocolError(f"unsupported v1 header: {parts[1]!r}")
    version = 4 if parts[1] == "TCP4" else 6
    source = _valid_ip(parts[2], version)
    _valid_ip(parts[3], version)
    port = _valid_port(parts[4])
    _valid_port(parts[5])
    return source, port


def parse_v2(header: bytes, body: bytes) -> Optional[Tuple[str, int]]:
    """Parse a v2 header (16 bytes) and its address block. None for LOCAL
    commands and non-IP families."""
    version_command, family = header[12], header[13]
    if version_command >> 4 != 2:
        raise ProxyProtocolError("unsupported v2 version")
    command = version_command & 0x0F
    if command == 0x0:          # LOCAL: the proxy's own health check
        return None
    if command != 0x1:          # only PROXY is defined
        raise ProxyProtocolError("unknown v2 command")
    address_family, transport = family >> 4, family & 0x0F
    if transport != 0x1:        # STREAM (TCP) only
        return None
    if address_family == 0x1:   # AF_INET: 4 + 4 + 2 + 2 bytes
        if len(body) < 12:
            raise ProxyProtocolError("short v2 IPv4 block")
        source = socket.inet_ntop(socket.AF_INET, body[0:4])
        (port,) = struct.unpack("!H", body[8:10])
        return source, port
    if address_family == 0x2:   # AF_INET6: 16 + 16 + 2 + 2 bytes
        if len(body) < 36:
            raise ProxyProtocolError("short v2 IPv6 block")
        source = socket.inet_ntop(socket.AF_INET6, body[0:16])
        (port,) = struct.unpack("!H", body[32:34])
        return source, port
    return None                 # AF_UNSPEC / AF_UNIX: no usable client IP


async def read_header(reader: asyncio.StreamReader, timeout: float = 5.0) -> Optional[Tuple[str, int]]:
    """Consume the PROXY header at the start of a connection and return the
    client's (ip, port), or None when the proxy sent no client address."""
    async def _read() -> Optional[Tuple[str, int]]:
        first = await reader.readexactly(5)
        if first == b"PROXY":
            # Byte by byte, so a hostile peer cannot make us buffer more
            # than the spec's 107 bytes looking for the line end.
            line = bytearray(first)
            while not line.endswith(b"\r\n"):
                if len(line) >= V1_MAX_LENGTH:
                    raise ProxyProtocolError("v1 header too long")
                line += await reader.readexactly(1)
            return parse_v1(bytes(line))
        if first == V2_SIGNATURE[:5]:
            header = first + await reader.readexactly(11)
            if header[:12] != V2_SIGNATURE:
                raise ProxyProtocolError("bad v2 signature")
            (length,) = struct.unpack("!H", header[14:16])
            body = await reader.readexactly(length) if length else b""
            return parse_v2(header, body)
        raise ProxyProtocolError("connection did not start with a PROXY header")

    try:
        return await asyncio.wait_for(_read(), timeout=timeout)
    except asyncio.TimeoutError as e:
        raise ProxyProtocolError("timed out waiting for the PROXY header") from e
    except asyncio.LimitOverrunError as e:
        raise ProxyProtocolError("v1 header too long") from e
    except asyncio.IncompleteReadError as e:
        raise ProxyProtocolError("connection closed inside the PROXY header") from e
