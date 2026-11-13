"""
Main Entry Point for Chat Server/Client
Provides CLI interface for running server or client.
"""
import asyncio
import logging
import sys
import argparse
from typing import NoReturn

from app.server.enhanced_server import run_server
from app.client.client import run_client
from app.core.config import get_client_config


def setup_logging(level: str = "INFO") -> None:
    """Configure async-safe logging."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from asyncio
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="High-Performance Async Chat Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run server on default port 8888
  python -m app.main server
  
  # Run server on custom port
  python -m app.main server --port 9000
  
  # Run client
  python -m app.main client
  
  # Run client connecting to remote server
  python -m app.main client --host 192.168.1.100 --port 9000

  # Secure client: verify the server with a private CA, log in with a token
  python -m app.main client --tls --ca secrets/ca.pem --token-file alice.jwt

  # Password login (prompted, never echoed)
  python -m app.main client --tls --ca secrets/ca.pem --user alice --password
  
  # Enable debug logging
  python -m app.main server --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "mode",
        choices=["server", "client"],
        help="Run as server or client"
    )
    
    parser.add_argument(
        "--host",
        default=None,
        help="Host address (server: bind address, client: connect address)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port number (default: CHAT_PORT or 8888)"
    )
    
    client = parser.add_argument_group("client security")
    client.add_argument("--tls", action="store_true", default=None,
                        help="connect with TLS (CHAT_CLIENT_TLS)")
    client.add_argument("--ca", help="CA certificate that signed the server's (CHAT_CLIENT_TLS_CA)")
    client.add_argument("--server-name", help="name to verify in the certificate (default: --host)")
    client.add_argument("--token", help="login token (prefer --token-file; CHAT_CLIENT_TOKEN)")
    client.add_argument("--token-file", help="file holding the login token (CHAT_CLIENT_TOKEN_FILE)")
    client.add_argument("--user", help="username (skips the prompt)")
    client.add_argument("--password", action="store_true",
                        help="prompt for a password (server in password mode)")
    client.add_argument("--reconnect", action="store_true",
                        help="use the client that reconnects and resumes rooms")

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    return parser.parse_args()


async def main_async() -> None:
    """Async main function."""
    args = parse_args()
    setup_logging(args.log_level)
    
    logger = logging.getLogger(__name__)
    
    try:
        if args.mode == "server":
            # Same production server as `python main.py`; --host/--port
            # override the CHAT_HOST/CHAT_PORT environment settings.
            logger.info("Starting server mode")
            await run_server(args.host, args.port)
            
        elif args.mode == "client":
            config = get_client_config()
            host = args.host or config.host or "127.0.0.1"
            port = args.port or config.port or 8888
            if args.tls or args.ca:
                config.tls = True
            if args.ca:
                config.tls_ca = args.ca
            if args.server_name:
                config.tls_server_name = args.server_name
            if args.token_file:
                with open(args.token_file, "r", encoding="utf-8") as f:
                    config.token = f.read().strip()
            elif args.token:
                config.token = args.token
            if config.token and not config.tls:
                logger.warning("Sending a login token without --tls: anyone on the network can read it")
            logger.info(f"Starting client mode, connecting to {host}:{port}{' (TLS)' if config.tls else ''}")
            if args.reconnect:
                from app.client.enhanced_client import run_client as run_resilient_client
                await run_resilient_client(host, port, config, args.user, args.password)
            else:
                await run_client(host, port, config, args.user, args.password)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


def main() -> NoReturn:
    """
    Entry point for the application.
    
    Usage:
        python -m app.main server
        python -m app.main client
    """
    # Windows-specific event loop policy for better compatibility
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run async main
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass
    
    sys.exit(0)


if __name__ == "__main__":
    main()
