"""Tests for FlinkCdcVersion class."""

import pytest

from flink_cdc_python import FlinkCdcVersion


class TestFlinkCdcVersionParsing:
    """Tests for version parsing."""

    def test_parse_full_version(self) -> None:
        """Test parsing a full version string."""
        version = FlinkCdcVersion.parse('3.3.0')
        assert version.major == 3
        assert version.minor == 3
        assert version.patch == 0

    def test_parse_minor_version(self) -> None:
        """Test parsing a version without patch number."""
        version = FlinkCdcVersion.parse('3.3')
        assert version.major == 3
        assert version.minor == 3
        assert version.patch == 0

    def test_parse_with_whitespace(self) -> None:
        """Test parsing a version with whitespace."""
        version = FlinkCdcVersion.parse('  3.3.0  ')
        assert version.major == 3
        assert version.minor == 3
        assert version.patch == 0

    def test_parse_invalid_version(self) -> None:
        """Test parsing an invalid version string."""
        with pytest.raises(ValueError, match='Invalid Flink CDC version'):
            FlinkCdcVersion.parse('invalid')

    def test_parse_invalid_version_format(self) -> None:
        """Test parsing an invalid version format."""
        with pytest.raises(ValueError, match='Invalid Flink CDC version'):
            FlinkCdcVersion.parse('3')


class TestFlinkCdcVersionProperties:
    """Tests for version properties."""

    def test_version_string(self) -> None:
        """Test version_string property."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert version.version_string == '3.3.0'

    def test_download_name(self) -> None:
        """Test download_name property."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert version.download_name == 'flink-cdc-3.3.0-bin.tar.gz'

    def test_download_url(self) -> None:
        """Test download_url property."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        expected = 'https://archive.apache.org/dist/flink/flink-cdc-3.3.0/flink-cdc-3.3.0-bin.tar.gz'
        assert version.download_url == expected

    def test_maven_url(self) -> None:
        """Test maven_url method."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        url = version.maven_url('flink-cdc-pipeline-connector-mysql')
        expected = (
            'https://repo1.maven.org/maven2/org/apache/flink/'
            'flink-cdc-pipeline-connector-mysql/3.3.0/'
            'flink-cdc-pipeline-connector-mysql-3.3.0-2.12.jar'
        )
        assert url == expected

    def test_maven_url_custom_scala(self) -> None:
        """Test maven_url with custom Scala version."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        url = version.maven_url('flink-cdc-pipeline-connector-mysql', scala_version='2.13')
        assert '2.13.jar' in url


class TestFlinkCdcVersionComparison:
    """Tests for version comparison."""

    def test_equal_versions(self) -> None:
        """Test equality comparison."""
        v1 = FlinkCdcVersion(major=3, minor=3, patch=0)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert v1 == v2

    def test_not_equal_versions(self) -> None:
        """Test inequality comparison."""
        v1 = FlinkCdcVersion(major=3, minor=3, patch=0)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=1)
        assert v1 != v2

    def test_less_than_major(self) -> None:
        """Test less than comparison (major version)."""
        v1 = FlinkCdcVersion(major=2, minor=9, patch=9)
        v2 = FlinkCdcVersion(major=3, minor=0, patch=0)
        assert v1 < v2

    def test_less_than_minor(self) -> None:
        """Test less than comparison (minor version)."""
        v1 = FlinkCdcVersion(major=3, minor=2, patch=9)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert v1 < v2

    def test_less_than_patch(self) -> None:
        """Test less than comparison (patch version)."""
        v1 = FlinkCdcVersion(major=3, minor=3, patch=0)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=1)
        assert v1 < v2

    def test_greater_than(self) -> None:
        """Test greater than comparison."""
        v1 = FlinkCdcVersion(major=3, minor=3, patch=1)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert v1 > v2

    def test_sorting(self) -> None:
        """Test sorting a list of versions."""
        versions = [
            FlinkCdcVersion(major=3, minor=3, patch=0),
            FlinkCdcVersion(major=3, minor=0, patch=0),
            FlinkCdcVersion(major=3, minor=1, patch=0),
        ]
        sorted_versions = sorted(versions)
        assert sorted_versions[0].version_string == '3.0.0'
        assert sorted_versions[1].version_string == '3.1.0'
        assert sorted_versions[2].version_string == '3.3.0'


class TestFlinkCdcVersionString:
    """Tests for string representation."""

    def test_str(self) -> None:
        """Test string representation."""
        version = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert str(version) == '3.3.0'

    def test_hash(self) -> None:
        """Test that versions are hashable."""
        v1 = FlinkCdcVersion(major=3, minor=3, patch=0)
        v2 = FlinkCdcVersion(major=3, minor=3, patch=0)
        assert hash(v1) == hash(v2)

        # Can be used in set
        version_set = {v1, v2}
        assert len(version_set) == 1
