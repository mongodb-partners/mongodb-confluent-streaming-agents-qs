"""
Shared logging configuration utilities.

Provides a standardized logging setup function used across all scripts
to ensure consistent logging behavior and formatting.
"""

import logging


def setup_logging(verbose: bool = False, default_level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration with consistent formatting.

    Args:
        verbose: Enable verbose (DEBUG) logging if True
        default_level: Default logging level when verbose=False
                      ("INFO" for most scripts, "ERROR" for quieter scripts)

    Returns:
        Logger instance for the calling module
    """
    if verbose:
        level = logging.DEBUG
    else:
        level = getattr(logging, default_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return logging.getLogger(__name__)
