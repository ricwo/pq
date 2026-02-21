"""Centralized loguru configuration for PQ."""

import sys

from loguru import logger


def configure_logging() -> None:
    """Configure loguru with a clean, aligned format.

    This is an opt-in helper for applications that want pq's default log
    format. Call it explicitly during application startup. pq itself never
    calls this function so it will not interfere with an existing loguru
    configuration.
    """
    # Remove default handler
    logger.remove()

    # Add handler with clean format
    # Fixed-width level, simple format without variable-length source info
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="DEBUG",
        colorize=True,
    )
