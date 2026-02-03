"""
Main Entry Point for Chat Server/Client
Provides CLI interface for running server or client.
"""
import asyncio
import logging
import sys
import argparse
from typing import NoReturn

from app.server.server import run_server
from app.client.client import run_client


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
        default=8888,
        help="Port number (default: 8888)"
    )
    
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
            host = args.host or "0.0.0.0"
            logger.info(f"Starting server mode on {host}:{args.port}")
            await run_server(host, args.port)
            
        elif args.mode == "client":
            host = args.host or "127.0.0.1"
            logger.info(f"Starting client mode, connecting to {host}:{args.port}")
            await run_client(host, args.port)
            
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
