"""
Entry point for the production chat server.

    python main.py

Configuration comes from CHAT_* environment variables (see .env.example).
"""
import asyncio
import logging
import sys

from app.core.config import get_server_config
from app.server.enhanced_server import EnhancedChatServer


def setup_logging(level: str, log_file: str = None) -> None:
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    logging.getLogger("asyncio").setLevel(logging.WARNING)


async def main() -> None:
    config = get_server_config()
    setup_logging(config.log_level, config.log_file)
    logger = logging.getLogger(__name__)
    logger.info(
        f"Chat port {config.port}, metrics port {config.metrics_port}, "
        f"max connections {config.max_connections}, "
        f"rate limiting {'on' if config.enable_rate_limiting else 'off'} "
        f"({config.max_messages_per_minute}/min per user, "
        f"{config.max_messages_per_minute_per_ip}/min per IP)"
    )
    await EnhancedChatServer(config=config).start()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
