"""
Shared logging configuration utilities.

Provides a standardized logging setup function used across all scripts
to ensure consistent logging behavior and formatting.
"""

import inspect
import logging
from typing import Optional


def setup_logging(
    verbose: bool = False,
    default_level: str = "INFO",
    name: Optional[str] = None,
) -> logging.Logger:
    """
    Set up logging configuration with consistent formatting.

    Args:
        verbose: Enable verbose (DEBUG) logging if True
        default_level: Default logging level when verbose=False
                      ("INFO" for most scripts, "ERROR" for quieter scripts)
        name: Logger name to return. When omitted, the CALLER's module name is
              used — not this module's. Returning ``getLogger(__name__)`` here
              would hand every caller the same "logging_utils" logger, so
              per-module log levels / filters could not target the real source.

    Returns:
        Logger instance named for the caller's module (or ``name`` if given).
    """
    if verbose:
        level = logging.DEBUG
    else:
        level = getattr(logging, default_level.upper(), logging.INFO)

    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

    if name is None:
        # Derive the caller's module __name__ from the call stack so the
        # returned logger reflects where setup_logging was invoked.
        frame = inspect.stack()[1].frame
        name = frame.f_globals.get("__name__", "__main__")

    return logging.getLogger(name)
