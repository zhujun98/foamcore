"""
Distributed under the terms of the BSD 3-Clause License.

The full license is in the file LICENSE, distributed with this software.

Author: Jun Zhu
"""
import logging


def create_logger(name):
    """Create the logger object for the whole API."""
    _logger = logging.getLogger(name)

    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s - %(levelname)s - %(message)s'
    )

    ch.setFormatter(formatter)

    _logger.addHandler(ch)

    return _logger


logger = create_logger("foamcore")
logger.setLevel(logging.INFO)
