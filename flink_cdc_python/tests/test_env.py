"""Tests for Env class."""

from pathlib import Path

import pytest

from flink_cdc_python import Env, FlinkCdcVersion


class TestEnvCreate:
    """Tests for Env.create()."""

    def test_create_with_version_string(self, tmp_path: Path) -> None:
        """Test creating env with version string."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
        )

        assert env.flink_cdc_version == FlinkCdcVersion.parse('3.3.0')
        assert env.flink_home == tmp_path / 'flink'
        assert env.env_dir == tmp_path / '.env'

    def test_create_with_version_object(self, tmp_path: Path) -> None:
        """Test creating env with FlinkCdcVersion object."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        env = Env.create(
            flink_cdc_version=version,
            flink_home=tmp_path / 'flink',
        )

        assert env.flink_cdc_version == version

    def test_create_with_jars(self, tmp_path: Path) -> None:
        """Test creating env with JARs."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            jars=['/path/to/connector.jar', '/path/to/udf.jar'],
        )

        assert len(env.jars) == 2
        assert '/path/to/connector.jar' in env.jars

    def test_create_default_env_dir(self, tmp_path: Path) -> None:
        """Test that default env_dir is .env."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
        )

        assert env.env_dir == Path('.env')


class TestEnvProperties:
    """Tests for Env properties."""

    def test_flink_cdc_path(self, tmp_path: Path) -> None:
        """Test flink_cdc_path property."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
        )

        expected = tmp_path / '.env' / 'flink-cdc-3.3.0'
        assert env.flink_cdc_path == expected

    def test_flink_cdc_lib(self, tmp_path: Path) -> None:
        """Test flink_cdc_lib property creates directory."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
        )

        lib_path = env.flink_cdc_lib
        cdc_path = env.flink_cdc_path
        assert lib_path is not None
        assert cdc_path is not None
        assert lib_path == cdc_path / 'lib'


class TestEnvListJars:
    """Tests for list_jars method."""

    def test_list_jars_empty(self, tmp_path: Path) -> None:
        """Test listing JARs when empty."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
        )

        jars = env.list_jars()
        assert jars == []

    def test_list_jars_with_configured_jars(self, tmp_path: Path) -> None:
        """Test listing JARs from env.jars."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
            jars=['/path/to/connector.jar'],
        )

        jars = env.list_jars()
        assert '/path/to/connector.jar' in jars

    def test_list_jars_with_lib_jars(self, tmp_path: Path) -> None:
        """Test listing JARs from lib directory."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
        )

        # Create lib directory and add a JAR
        lib_path = env.flink_cdc_lib
        assert lib_path is not None
        jar_file = lib_path / 'test-connector.jar'
        jar_file.touch()

        jars = env.list_jars()
        assert str(jar_file) in jars

    def test_list_jars_combined(self, tmp_path: Path) -> None:
        """Test listing JARs from both sources."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
            jars=['/configured/jar.jar'],
        )

        # Create lib directory and add a JAR
        lib_path = env.flink_cdc_lib
        assert lib_path is not None
        jar_file = lib_path / 'lib-connector.jar'
        jar_file.touch()

        jars = env.list_jars()
        assert '/configured/jar.jar' in jars
        assert str(jar_file) in jars


class TestEnvSetup:
    """Tests for setup method."""

    def test_setup_missing_flink_home(self, tmp_path: Path) -> None:
        """Test that setup raises error if Flink home doesn't exist."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'nonexistent' / 'flink',
            env_dir=tmp_path / '.env',
        )

        with pytest.raises(FileNotFoundError, match='Flink home does not exist'):
            env.setup()

    def test_setup_creates_env_dir(self, tmp_path: Path) -> None:
        """Test that setup creates env_dir if needed."""
        flink_home = tmp_path / 'flink'
        flink_home.mkdir()

        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=flink_home,
            env_dir=tmp_path / '.env',
        )

        env.setup()
        assert env.env_dir.exists()


class TestEnvClean:
    """Tests for clean method."""

    def test_clean_removes_flink_cdc(self, tmp_path: Path) -> None:
        """Test that clean removes Flink CDC directory."""
        flink_home = tmp_path / 'flink'
        flink_home.mkdir()

        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=flink_home,
            env_dir=tmp_path / '.env',
        )

        # Create the Flink CDC directory
        cdc_path = env.flink_cdc_path
        assert cdc_path is not None
        cdc_path.mkdir(parents=True)

        env.clean()
        assert not cdc_path.exists()


class TestEnvDirectConstruction:
    """Tests for direct construction."""

    def test_direct_construction(self, tmp_path: Path) -> None:
        """Test constructing Env directly."""
        version = FlinkCdcVersion.parse('3.3.0')
        env = Env(
            flink_cdc_version=version,
            flink_home=tmp_path / 'flink',
            env_dir=tmp_path / '.env',
            jars=['/test.jar'],
        )

        assert env.flink_cdc_version == version
        assert env.flink_home == tmp_path / 'flink'
        assert env.jars == ['/test.jar']
