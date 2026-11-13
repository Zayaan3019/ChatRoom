"""
Authentication: signed tokens and password hashes, standard library only.

Two ways to prove who you are in AUTH:

* **Token** (``CHAT_AUTH_MODE=token``). A JWT signed with HMAC-SHA256 by a
  login service that shares the secret. The username is the token's
  ``sub`` claim, so a client can no longer choose it. Verification checks,
  in order: structure, the algorithm (only HS256 -- "none" and every other
  value are rejected, which closes the algorithm-confusion hole), the
  signature (constant-time), then ``exp``/``nbf``/``iat`` with a small clock
  leeway, ``iss`` and ``aud`` when configured, and that ``sub`` is a valid
  username.

* **Password** (``CHAT_AUTH_MODE=password``). A JSON user file of scrypt
  hashes (``python -m app.core.auth add-user``). scrypt is memory-hard, so
  guessing in parallel on GPUs is expensive; each hash has its own salt.
  The server verifies in a worker thread so ~50 ms of hashing never blocks
  the event loop, and an unknown user costs the same time as a wrong
  password, so timing does not reveal which names exist.

CLI:
    python -m app.core.auth secret                     # print a new random secret
    python -m app.core.auth issue-token --user alice --secret-file secrets/jwt.secret
    python -m app.core.auth add-user alice --users-file users.json
"""
import argparse
import base64
import getpass
import hashlib
import hmac
import json
import os
import secrets
import sys
import threading
import time
from typing import Any, Dict, Optional

from .validation import InputValidator


class AuthError(Exception):
    """Credentials were rejected. The message is for logs, never for clients."""


# --------------------------------------------------------------------- JWT

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    if not isinstance(text, str) or any(c not in _B64URL for c in text):
        raise AuthError("Malformed token segment")
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


_B64URL = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
MAX_TOKEN_LENGTH = 4096


def issue_token(
    subject: str,
    secret: str,
    *,
    ttl_seconds: int = 3600,
    issuer: Optional[str] = None,
    audience: Optional[str] = "chat",
    now: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    issued = int(now if now is not None else time.time())
    claims: Dict[str, Any] = {"sub": subject, "iat": issued, "exp": issued + int(ttl_seconds)}
    if issuer:
        claims["iss"] = issuer
    if audience:
        claims["aud"] = audience
    if extra:
        claims.update(extra)
    header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = _b64url_encode(json.dumps(claims, separators=(",", ":")).encode())
    signing_input = f"{header}.{body}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url_encode(signature)}"


def verify_token(
    token: Any,
    secret: str,
    *,
    issuer: Optional[str] = None,
    audience: Optional[str] = "chat",
    leeway: float = 30.0,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Return the claims of a valid token, or raise AuthError."""
    if not isinstance(token, str) or not token or len(token) > MAX_TOKEN_LENGTH:
        raise AuthError("Missing or oversized token")
    parts = token.split(".")
    if len(parts) != 3:
        raise AuthError("Token must have three segments")
    header_b64, body_b64, sig_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64))
    except (ValueError, UnicodeDecodeError) as e:
        raise AuthError(f"Bad token header: {e}") from e
    if not isinstance(header, dict) or header.get("alg") != "HS256":
        raise AuthError(f"Unsupported token algorithm: {header.get('alg') if isinstance(header, dict) else '?'}")

    expected = hmac.new(secret.encode("utf-8"), f"{header_b64}.{body_b64}".encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, _b64url_decode(sig_b64)):
        raise AuthError("Bad token signature")

    try:
        claims = json.loads(_b64url_decode(body_b64))
    except (ValueError, UnicodeDecodeError) as e:
        raise AuthError(f"Bad token claims: {e}") from e
    if not isinstance(claims, dict):
        raise AuthError("Token claims must be an object")

    current = now if now is not None else time.time()
    exp = claims.get("exp")
    if not isinstance(exp, (int, float)) or isinstance(exp, bool):
        raise AuthError("Token has no expiry")
    if current > exp + leeway:
        raise AuthError("Token expired")
    nbf = claims.get("nbf")
    if isinstance(nbf, (int, float)) and current + leeway < nbf:
        raise AuthError("Token not yet valid")
    iat = claims.get("iat")
    if isinstance(iat, (int, float)) and iat > current + leeway:
        raise AuthError("Token issued in the future")
    if issuer is not None and claims.get("iss") != issuer:
        raise AuthError("Wrong token issuer")
    if audience is not None:
        aud = claims.get("aud")
        if not (aud == audience or (isinstance(aud, list) and audience in aud)):
            raise AuthError("Wrong token audience")
    subject = claims.get("sub")
    ok, _ = InputValidator.validate_username(subject if isinstance(subject, str) else "")
    if not ok:
        raise AuthError("Token subject is not a valid username")
    return claims


# ---------------------------------------------------------------- passwords

SCRYPT_N, SCRYPT_R, SCRYPT_P = 2 ** 14, 8, 1   # ~16 MiB, ~50 ms per hash
_SCRYPT_MAXMEM = 64 * 1024 * 1024


def hash_password(password: str, *, n: int = SCRYPT_N, r: int = SCRYPT_R, p: int = SCRYPT_P) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p, maxmem=_SCRYPT_MAXMEM, dklen=32)
    return f"scrypt${n}${r}${p}${_b64url_encode(salt)}${_b64url_encode(digest)}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, n, r, p, salt_b64, digest_b64 = encoded.split("$")
        if scheme != "scrypt":
            return False
        salt, expected = _b64url_decode(salt_b64), _b64url_decode(digest_b64)
        digest = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=int(n), r=int(r), p=int(p),
                                maxmem=_SCRYPT_MAXMEM, dklen=len(expected))
    except (ValueError, AuthError):
        return False
    return hmac.compare_digest(digest, expected)


# A real hash of a random password: verifying against it costs exactly what a
# real check costs, so "no such user" and "wrong password" take equal time.
_dummy_hash: Optional[str] = None


def _get_dummy_hash() -> str:
    global _dummy_hash
    if _dummy_hash is None:
        _dummy_hash = hash_password(secrets.token_urlsafe(16))
    return _dummy_hash


class UserStore:
    """``{"users": {"alice": "scrypt$..."}}`` in a JSON file, re-read when the
    file changes, so accounts can be added without a restart."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._users: Dict[str, str] = {}
        self._mtime: Optional[float] = None
        self._lock = threading.Lock()

    def _load(self) -> Dict[str, str]:
        with self._lock:
            try:
                mtime = os.stat(self.path).st_mtime
            except OSError:
                self._users, self._mtime = {}, None
                return self._users
            if mtime != self._mtime:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                users = data.get("users", {}) if isinstance(data, dict) else {}
                self._users = {k.casefold(): v for k, v in users.items() if isinstance(v, str)}
                self._mtime = mtime
            return self._users

    def check(self, username: str, password: str) -> bool:
        """True only for a known user with the right password. Constant work
        whether or not the user exists."""
        if not isinstance(username, str) or not isinstance(password, str) or len(password) > 1024:
            return False
        stored = self._load().get(username.casefold())
        ok = verify_password(password, stored or _get_dummy_hash())
        return ok and stored is not None

    def set_password(self, username: str, password: str) -> None:
        ok, error = InputValidator.validate_username(username)
        if not ok:
            raise ValueError(error)
        data: Dict[str, Any] = {"users": {}}
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        data.setdefault("users", {})[username] = hash_password(password)
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)  # atomic: the server never reads half a file


# --------------------------------------------------------------------- CLI

def _secret_from_args(args: argparse.Namespace) -> str:
    if args.secret_file:
        with open(args.secret_file, "r", encoding="utf-8") as f:
            return f.read().strip()
    value = os.getenv("CHAT_JWT_SECRET")
    if not value:
        sys.exit("Pass --secret-file or set CHAT_JWT_SECRET")
    return value


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.core.auth", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("secret", help="print a new random 256-bit secret")

    issue = sub.add_parser("issue-token", help="sign a token for a user")
    issue.add_argument("--user", required=True)
    issue.add_argument("--ttl", type=int, default=3600, help="lifetime in seconds (default 3600)")
    issue.add_argument("--secret-file")
    issue.add_argument("--issuer", default=os.getenv("CHAT_JWT_ISSUER"))
    issue.add_argument("--audience", default=os.getenv("CHAT_JWT_AUDIENCE", "chat"))

    add = sub.add_parser("add-user", help="add or reset a password-mode user")
    add.add_argument("username")
    add.add_argument("--users-file", default=os.getenv("CHAT_USERS_FILE", "users.json"))
    add.add_argument("--password-stdin", action="store_true", help="read the password from stdin")

    args = parser.parse_args(argv)
    if args.command == "secret":
        print(secrets.token_urlsafe(32))
    elif args.command == "issue-token":
        ok, error = InputValidator.validate_username(args.user)
        if not ok:
            sys.exit(error)
        print(issue_token(args.user, _secret_from_args(args), ttl_seconds=args.ttl,
                          issuer=args.issuer, audience=args.audience))
    elif args.command == "add-user":
        if args.password_stdin:
            password = sys.stdin.readline().rstrip("\n")
        else:
            password = getpass.getpass(f"Password for {args.username}: ")
            if password != getpass.getpass("Repeat: "):
                sys.exit("Passwords do not match")
        if len(password) < 8:
            sys.exit("Use at least 8 characters")
        UserStore(args.users_file).set_password(args.username, password)
        print(f"Saved {args.username} to {args.users_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
