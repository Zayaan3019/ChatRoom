"""
Entry point for the production-grade async chat server.
"""
import asyncio
import logging
import sys

from app.server.enhanced_server import EnhancedChatServer
from app.core.config import get_server_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('chat_server.log')
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """
    Start the production chat server.
    
    Features:
        - Rate limiting and connection limits
        - Metrics and health endpoints (port 9090)
        - Input validation and sanitization
        - Configuration via environment variables
        - Graceful shutdown
    """
    config = get_server_config()
    
    logger.info("="*60)
    logger.info(" PRODUCTION CHAT SERVER ".center(60, "="))
    logger.info("="*60)
    logger.info(f"Chat Port: {config.port}")
    logger.info(f"Metrics Port: {config.metrics_port}")
    logger.info(f"Max Connections: {config.max_connections}")
    logger.info(f"Max Messages/Min: {config.max_messages_per_minute}")
    logger.info(f"Rate Limit Enabled: {config.enable_rate_limiting}")
    logger.info("="*60)
    
    # Create enhanced server
    server = EnhancedChatServer(
        host=config.host,
        port=config.port
    )
    
    try:
        await server.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        raise
    finally:
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server stopped")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
