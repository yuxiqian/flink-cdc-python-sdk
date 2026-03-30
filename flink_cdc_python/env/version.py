"""
Version management for Flink CDC.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import total_ordering


@total_ordering
@dataclass(frozen=True)
class FlinkCdcVersion:
    """
    Represents a Flink CDC version.

    Attributes:
        major: Major version number
        minor: Minor version number
        patch: Patch version number
    """

    major: int
    minor: int
    patch: int = 0

    _VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)(?:\.(\d+))?')

    # Known stable versions
    KNOWN_VERSIONS = [
        '3.3.0',
        '3.2.0',
        '3.1.1',
        '3.1.0',
        '3.0.1',
        '3.0.0',
    ]

    @classmethod
    def parse(cls, version_str: str) -> FlinkCdcVersion:
        """
        Parse a version string into a FlinkCdcVersion object.

        Args:
            version_str: Version string like "3.3.0"

        Returns:
            FlinkCdcVersion object

        Raises:
            ValueError: If the version string is invalid
        """
        match = cls._VERSION_PATTERN.match(version_str.strip())
        if not match:
            raise ValueError(f'Invalid Flink CDC version: {version_str}')

        major = int(match.group(1))
        minor = int(match.group(2))
        patch = int(match.group(3)) if match.group(3) else 0

        return cls(major=major, minor=minor, patch=patch)

    @property
    def version_string(self) -> str:
        """Return the version string."""
        return f'{self.major}.{self.minor}.{self.patch}'

    @property
    def download_name(self) -> str:
        """Return the download file name pattern."""
        return f'flink-cdc-{self.version_string}-bin.tar.gz'

    @property
    def download_url(self) -> str:
        """Return the Apache archive download URL."""
        return f'https://archive.apache.org/dist/flink/flink-cdc-{self.version_string}/{self.download_name}'

    def maven_url(self, artifact: str, scala_version: str = '2.12') -> str:
        """
        Generate Maven Central URL for a specific artifact.

        Args:
            artifact: Artifact name (e.g., "flink-cdc-pipeline-connector-mysql")
            scala_version: Scala binary version

        Returns:
            Maven Central URL for the artifact
        """
        return (
            f'https://repo1.maven.org/maven2/org/apache/flink/{artifact}/'
            f'{self.version_string}/{artifact}-{self.version_string}-{scala_version}.jar'
        )

    def __str__(self) -> str:
        return self.version_string

    def __lt__(self, other: FlinkCdcVersion) -> bool:
        if not isinstance(other, FlinkCdcVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FlinkCdcVersion):
            return NotImplemented
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch))
