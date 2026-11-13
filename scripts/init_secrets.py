"""
Create everything a deployment needs to start securely, once:

    <dir>/ca.pem, ca.key         a private certificate authority (10 years)
    <dir>/server.pem, server.key the chat server's certificate, signed by the CA
    <dir>/jwt.secret             256-bit HMAC secret for login tokens
    <dir>/redis.secret           Redis password

Existing files are kept, so re-running is safe and never rotates a secret by
accident (use --force to regenerate). Clients verify the server with ca.pem.

--export DIR copies what the running services need (everything except the CA
private key) to a second directory, e.g. a Docker volume, so ca.key never
reaches a server.

    python scripts/init_secrets.py secrets --hosts localhost,127.0.0.1,chat.example.com

Needs the `openssl` command (present in the python:*-slim images, Git for
Windows and every Linux distribution). Keys are EC P-256.
"""
import argparse
import ipaddress
import os
import secrets
import shutil
import subprocess
import sys
import tempfile

DEFAULT_HOSTS = "localhost,127.0.0.1,chat-server-1,chat-server-2,nginx"


def _run(*args: str) -> None:
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def _write_secret(path: str, force: bool) -> bool:
    if os.path.exists(path) and not force:
        return False
    with open(path, "w", encoding="utf-8") as f:
        f.write(secrets.token_urlsafe(32) + "\n")
    os.chmod(path, 0o600)
    return True


def make_certs(directory: str, hosts: str, force: bool = False, openssl: str = "openssl") -> list:
    created = []
    ca_key, ca_pem = os.path.join(directory, "ca.key"), os.path.join(directory, "ca.pem")
    key, pem = os.path.join(directory, "server.key"), os.path.join(directory, "server.pem")

    if force or not (os.path.exists(ca_key) and os.path.exists(ca_pem)):
        _run(openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", ca_key)
        _run(openssl, "req", "-x509", "-new", "-key", ca_key, "-sha256", "-days", "3650",
             "-subj", "/CN=Chat Server Private CA", "-out", ca_pem,
             "-addext", "basicConstraints=critical,CA:TRUE",
             "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        created += [ca_key, ca_pem]
        force = True  # a new CA invalidates the old server certificate

    if force or not (os.path.exists(key) and os.path.exists(pem)):
        names = [h.strip() for h in hosts.split(",") if h.strip()]
        san = []
        for name in names:
            try:
                ipaddress.ip_address(name)
                san.append(f"IP:{name}")
            except ValueError:
                san.append(f"DNS:{name}")
        with tempfile.TemporaryDirectory() as tmp:
            csr, ext = os.path.join(tmp, "server.csr"), os.path.join(tmp, "ext.cnf")
            with open(ext, "w", encoding="ascii") as f:
                f.write("basicConstraints=critical,CA:FALSE\n"
                        "keyUsage=critical,digitalSignature\n"
                        "extendedKeyUsage=serverAuth\n"
                        f"subjectAltName={','.join(san)}\n")
            _run(openssl, "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", key)
            _run(openssl, "req", "-new", "-key", key, "-subj", f"/CN={names[0]}", "-out", csr)
            _run(openssl, "x509", "-req", "-in", csr, "-CA", ca_pem, "-CAkey", ca_key,
                 "-CAcreateserial", "-sha256", "-days", "825", "-extfile", ext, "-out", pem)
        srl = os.path.join(directory, "ca.srl")
        if os.path.exists(srl):
            os.remove(srl)
        created += [key, pem]

    return created


RUNTIME_FILES = ("ca.pem", "server.pem", "server.key", "jwt.secret", "redis.secret")


def _protect(directory: str, owner, strict: bool) -> None:
    """Private keys and secrets owner-only, certificates world-readable.
    strict=False tolerates file systems that ignore permissions (a Windows
    folder mounted into a container)."""
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        try:
            os.chmod(path, 0o644 if name.endswith(".pem") else 0o600)
            if owner and hasattr(os, "chown"):
                uid, _, gid = owner.partition(":")
                os.chown(path, int(uid), int(gid or uid))
        except PermissionError:
            if strict:
                raise
            print(f"note: cannot set permissions on {path}; protect this folder yourself")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("directory")
    parser.add_argument("--hosts", default=os.getenv("CHAT_TLS_HOSTS", DEFAULT_HOSTS),
                        help=f"names and IPs for the server certificate (default: {DEFAULT_HOSTS})")
    parser.add_argument("--force", action="store_true", help="regenerate everything")
    parser.add_argument("--openssl", default=os.getenv("OPENSSL", "openssl"))
    parser.add_argument("--owner", help="UID:GID to own the exported files (when run as root for a container user)")
    parser.add_argument("--export", help="also copy the runtime files (not ca.key) here")
    args = parser.parse_args(argv)

    if shutil.which(args.openssl) is None:
        sys.exit(f"'{args.openssl}' not found; install OpenSSL or pass --openssl PATH")
    os.makedirs(args.directory, exist_ok=True)
    created = make_certs(args.directory, args.hosts, args.force, args.openssl)
    for name in ("jwt.secret", "redis.secret"):
        if _write_secret(os.path.join(args.directory, name), args.force):
            created.append(os.path.join(args.directory, name))
    _protect(args.directory, None, strict=False)
    if args.export:
        os.makedirs(args.export, exist_ok=True)
        for name in RUNTIME_FILES:
            shutil.copyfile(os.path.join(args.directory, name), os.path.join(args.export, name))
        stale = os.path.join(args.export, "ca.key")
        if os.path.exists(stale):
            os.remove(stale)
        _protect(args.export, args.owner, strict=True)
    print("created: " + (", ".join(os.path.basename(c) for c in created) if created else "nothing (all present)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
