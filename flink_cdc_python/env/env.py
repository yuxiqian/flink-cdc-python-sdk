"""
Environment management for Flink CDC Python SDK.

Manages all environment-related configuration:
- Flink CDC version and automatic download
- User's Flink installation
- Additional JAR dependencies
"""

from __future__ import annotations

import logging
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .version import FlinkCdcVersion

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)

# Default directory for environment storage
DEFAULT_ENV_DIR = '.env'


@dataclass
class Env:
    """
    Manages the execution environment for Flink CDC pipelines.

    This class handles:
    - Flink CDC version and automatic download to env_dir
    - User's Flink installation path
    - Additional JAR dependencies

    Attributes:
        flink_cdc_version: Flink CDC version to use
        flink_home: Path to user's Flink installation
        env_dir: Directory to store Flink CDC files (default: .env)
        jars: Additional JAR paths to include
    """

    flink_cdc_version: FlinkCdcVersion
    flink_home: Path
    env_dir: Path = field(default_factory=lambda: Path(DEFAULT_ENV_DIR))
    jars: list[str] = field(default_factory=list)

    _flink_cdc_path: Path | None = field(default=None, init=False, repr=False)
    _connectors: dict[str, Path] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize paths after instance creation."""
        if isinstance(self.env_dir, str):
            self.env_dir = Path(self.env_dir)
        if isinstance(self.flink_home, str):
            self.flink_home = Path(self.flink_home)

    @property
    def flink_cdc_path(self) -> Path | None:
        """Return the path to Flink CDC installation."""
        if self._flink_cdc_path:
            return self._flink_cdc_path
        if self.flink_cdc_version:
            return self.env_dir / f'flink-cdc-{self.flink_cdc_version.version_string}'
        return None

    @property
    def flink_cdc_lib(self) -> Path | None:
        """Return the path to Flink CDC lib directory."""
        if self.flink_cdc_path:
            lib_path = self.flink_cdc_path / 'lib'
            lib_path.mkdir(parents=True, exist_ok=True)
            return lib_path
        return None

    def setup(self) -> Env:
        """
        Set up the Flink CDC environment.

        This will download Flink CDC distribution if not already present.
        Also validates that Flink home exists.

        Note: This is called automatically by add_connector() and execute().
        You only need to call this manually if you want to pre-setup the environment.

        Returns:
            Self for method chaining
        """
        # Validate Flink home
        if not self.flink_home.exists():
            raise FileNotFoundError(f'Flink home does not exist: {self.flink_home}')

        self.env_dir.mkdir(parents=True, exist_ok=True)

        if not self._is_flink_cdc_ready():
            self._setup_flink_cdc()

        return self

    def _is_flink_cdc_ready(self) -> bool:
        """Check if Flink CDC is already set up."""
        cdc_path = self.flink_cdc_path
        if not cdc_path:
            return False
        return cdc_path.exists() and (cdc_path / 'lib').exists()

    def _setup_flink_cdc(self) -> None:
        """Download and set up Flink CDC."""
        logger.info('Setting up Flink CDC %s...', self.flink_cdc_version)

        archive_path = self.env_dir / self.flink_cdc_version.download_name
        extract_path = self.env_dir

        # Download if not exists
        if not archive_path.exists():
            self._download_file(
                self.flink_cdc_version.download_url,
                archive_path,
                f'Flink CDC {self.flink_cdc_version}',
            )

        # Extract
        self._extract_archive(archive_path, extract_path)

        # Set executable permissions
        cdc_path = self.flink_cdc_path
        if cdc_path:
            bin_path = cdc_path / 'bin'
            if bin_path.exists():
                self._set_executable_permissions(bin_path)

        logger.info('Flink CDC %s setup complete', self.flink_cdc_version)

    def _ensure_setup(self) -> None:
        """Ensure Flink CDC is set up (auto-setup if needed)."""
        if not self._is_flink_cdc_ready():
            self._setup_flink_cdc()

    def add_connector(self, connector: str, scala_version: str = '2.12') -> Path:
        """
        Add a connector JAR to the Flink CDC lib directory.

        Args:
            connector: Connector artifact name (e.g., "flink-cdc-pipeline-connector-mysql")
            scala_version: Scala binary version

        Returns:
            Path to the downloaded connector JAR
        """
        # Auto-setup if needed
        self._ensure_setup()

        lib_path = self.flink_cdc_lib
        if not lib_path:
            raise ValueError('Flink CDC lib directory not available')

        # Generate Maven URL
        url = self.flink_cdc_version.maven_url(connector, scala_version)
        jar_name = f'{connector}-{self.flink_cdc_version.version_string}-{scala_version}.jar'
        jar_path = lib_path / jar_name

        if not jar_path.exists():
            logger.info('Downloading connector %s...', connector)
            self._download_file(url, jar_path, connector)

        self._connectors[connector] = jar_path
        return jar_path

    def add_connectors(self, connectors: list[str], scala_version: str = '2.12') -> list[Path]:
        """
        Add multiple connector JARs.

        Args:
            connectors: List of connector artifact names
            scala_version: Scala binary version

        Returns:
            List of paths to downloaded connector JARs
        """
        return [self.add_connector(c, scala_version) for c in connectors]

    def get_connector(self, connector: str) -> Path | None:
        """Get the path to a previously added connector."""
        return self._connectors.get(connector)

    def list_jars(self) -> list[str]:
        """
        List all JAR files to include.

        Includes:
        - JARs from env.jars
        - Connector JARs in lib directory

        Returns:
            List of JAR paths as strings
        """
        all_jars: list[str] = []

        # Add explicitly configured JARs
        all_jars.extend(self.jars)

        # Add connector JARs from lib directory
        lib_path = self.flink_cdc_lib
        if lib_path and lib_path.exists():
            for jar_path in lib_path.glob('*.jar'):
                all_jars.append(str(jar_path))

        return all_jars

    @staticmethod
    def _download_file(url: str, dest: Path, description: str = 'file') -> None:
        """Download a file from URL."""
        logger.info('Downloading %s from %s', description, url)

        try:
            # Create a temporary file and then rename for atomicity
            temp_path = dest.with_suffix('.tmp')

            def progress_hook(block_num: int, block_size: int, total_size: int) -> None:
                if total_size > 0:
                    downloaded = block_num * block_size
                    percent = min(100, downloaded * 100 // total_size)
                    if block_num % 100 == 0:  # Log every 100 blocks
                        logger.debug('Downloaded %d%%', percent)

            urllib.request.urlretrieve(url, temp_path, progress_hook)
            temp_path.rename(dest)
            logger.info('Download complete: %s', dest)

        except Exception as e:
            logger.error('Failed to download %s: %s', description, e)
            raise RuntimeError(f'Failed to download {description} from {url}: {e}') from e

    @staticmethod
    def _extract_archive(archive_path: Path, dest: Path) -> None:
        """Extract a tar.gz or zip archive."""
        logger.info('Extracting %s to %s', archive_path, dest)

        if archive_path.suffix == '.zip':
            import zipfile

            with zipfile.ZipFile(archive_path, 'r') as zf:
                zf.extractall(dest)
        elif archive_path.name.endswith('.tar.gz') or archive_path.name.endswith('.tgz'):
            with tarfile.open(archive_path, 'r:gz') as tf:
                tf.extractall(dest)
        else:
            raise ValueError(f'Unsupported archive format: {archive_path}')

        logger.info('Extraction complete')

    @staticmethod
    def _set_executable_permissions(bin_dir: Path) -> None:
        """Set executable permissions on scripts in bin directory."""
        system = platform.system()
        if system == 'Windows':
            return  # Windows doesn't need executable permissions

        for script in bin_dir.iterdir():
            if script.is_file():
                current_mode = script.stat().st_mode
                script.chmod(current_mode | 0o755)

    @classmethod
    def create(
        cls,
        flink_cdc_version: str | FlinkCdcVersion,
        flink_home: str | Path,
        env_dir: Path | str | None = None,
        jars: list[str] | None = None,
    ) -> Env:
        """
        Create an Env with specified configuration.

        Args:
            flink_cdc_version: Flink CDC version (string or FlinkCdcVersion)
            flink_home: Path to user's Flink installation
            env_dir: Directory for Flink CDC artifacts storage
            jars: Additional JAR paths to include

        Returns:
            Configured Env instance

        Example:
            ```python
            # Create env
            env = Env.create(
                flink_cdc_version="3.3.0",
                flink_home="/path/to/flink",
                jars=["/path/to/mysql-connector-java.jar"],
            )

            # Setup and add connectors
            env.setup()
            env.add_connector("flink-cdc-pipeline-connector-mysql")
            ```
        """
        fcv: FlinkCdcVersion
        if isinstance(flink_cdc_version, str):
            fcv = FlinkCdcVersion.parse(flink_cdc_version)
        else:
            fcv = flink_cdc_version

        return cls(
            flink_cdc_version=fcv,
            flink_home=Path(flink_home),
            env_dir=Path(env_dir) if env_dir else Path(DEFAULT_ENV_DIR),
            jars=jars or [],
        )

    def execute(
        self,
        pipeline: Pipeline,
        *,
        yaml_file: str | Path | None = None,
        detach: bool = True,
    ) -> subprocess.CompletedProcess[bytes] | None:
        """
        Execute a pipeline using this environment.

        Args:
            pipeline: Pipeline to execute
            yaml_file: Path to save the YAML file (optional, temporary file used if not provided)
            detach: Whether to run in background

        Returns:
            subprocess result or None if detached

        Raises:
            FileNotFoundError: If Flink home or Flink CDC CLI not found
        """
        # Validate Flink home
        if not self.flink_home.exists():
            raise FileNotFoundError(f'Flink home does not exist: {self.flink_home}')

        # Auto-setup Flink CDC if needed
        self._ensure_setup()

        cdc_home = self.flink_cdc_path
        if not cdc_home:
            raise ValueError('Flink CDC path not available after setup.')

        # Save YAML file
        yaml_path: Path
        if yaml_file:
            yaml_path = pipeline.save_yaml(yaml_file)
        else:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tf:
                tf.write(pipeline.to_yaml())
                yaml_path = Path(tf.name)

        # Build command
        flink_cdc_sh = cdc_home / 'bin' / 'flink-cdc.sh'

        if not flink_cdc_sh.exists():
            raise FileNotFoundError(f'Flink CDC CLI not found at {flink_cdc_sh}')

        cmd = [str(flink_cdc_sh), str(yaml_path)]

        # Add all JARs from Env
        for jar in self.list_jars():
            cmd.extend(['--jar', jar])

        # Set FLINK_HOME environment variable
        env_vars = {'FLINK_HOME': str(self.flink_home)}

        logger.info('Executing pipeline: %s', ' '.join(cmd))
        logger.info('Pipeline YAML:\n%s', pipeline.to_yaml())

        try:
            if detach:
                # Run in background
                subprocess.Popen(
                    cmd,
                    env={**os.environ, **env_vars},
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return None
            else:
                # Run in foreground
                return subprocess.run(
                    cmd,
                    env={**os.environ, **env_vars},
                    check=True,
                )
        finally:
            # Clean up temporary YAML file
            if yaml_file is None and yaml_path.exists():
                yaml_path.unlink()

    def clean(self) -> None:
        """Clean up downloaded Flink CDC files."""
        if self.flink_cdc_path and self.flink_cdc_path.exists():
            shutil.rmtree(self.flink_cdc_path)

        # Clean archives
        for archive in self.env_dir.glob('*.tgz'):
            archive.unlink()
        for archive in self.env_dir.glob('*.tar.gz'):
            archive.unlink()
        for archive in self.env_dir.glob('*.zip'):
            archive.unlink()
