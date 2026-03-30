"""
Environment management for Flink CDC Python SDK.

This module handles:
- Flink CDC version management
- Automatic downloading and setup of Flink CDC distributions and connectors
- Flink environment is provided by the user (not managed here)
"""

from .env import Env
from .version import FlinkCdcVersion

__all__ = ['Env', 'FlinkCdcVersion']
