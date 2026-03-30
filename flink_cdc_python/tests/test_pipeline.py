"""Tests for Pipeline API."""

from pathlib import Path

import pytest

from flink_cdc_python import (
    Env,
    Pipeline,
)


class TestPipelineCreate:
    """Tests for Pipeline.create()."""

    def test_create_basic(self) -> None:
        """Test creating a basic pipeline."""
        pipeline = Pipeline.create('test-pipeline', parallelism=2)
        assert pipeline._pipeline_config['name'] == 'test-pipeline'
        assert pipeline._pipeline_config['parallelism'] == 2

    def test_create_with_multiple_options(self) -> None:
        """Test creating a pipeline with multiple options."""
        pipeline = Pipeline.create(
            'test',
            parallelism=4,
            local_time_zone='America/Los_Angeles',
            execution_runtime_mode='STREAMING',
        )
        assert pipeline._pipeline_config['parallelism'] == 4
        assert pipeline._pipeline_config['local_time_zone'] == 'America/Los_Angeles'


class TestPipelineSource:
    """Tests for source definition."""

    def test_from_source_basic(self) -> None:
        """Test defining a basic source."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost', port=3306)
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert pipeline._source is not None
        assert pipeline._source.type == 'mysql'
        assert pipeline._source.config['hostname'] == 'localhost'
        assert pipeline._source.config['port'] == 3306

    def test_from_source_with_tables(self) -> None:
        """Test defining a source with tables pattern."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost', tables=r'app_db\..*')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert pipeline._source is not None
        assert pipeline._source.config['tables'] == r'app_db\..*'


class TestPipelineSink:
    """Tests for sink definition."""

    def test_to_sink_basic(self) -> None:
        """Test defining a basic sink."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .to_sink('doris', fenodes='127.0.0.1:8030', username='root')
        )

        assert pipeline._sink is not None
        assert pipeline._sink.type == 'doris'
        assert pipeline._sink.config['fenodes'] == '127.0.0.1:8030'
        assert pipeline._sink.config['username'] == 'root'


class TestPipelineTransform:
    """Tests for transform definition."""

    def test_transform_basic(self) -> None:
        """Test adding a basic transform."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .transform('app_db.orders', projection='id, name')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._transforms) == 1
        assert pipeline._transforms[0].source_table == 'app_db.orders'
        assert pipeline._transforms[0].projection == 'id, name'

    def test_transform_with_filter(self) -> None:
        """Test adding a transform with filter."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .transform('app_db.orders', projection='id, name', filtering='id > 10')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert pipeline._transforms[0].filtering == 'id > 10'

    def test_multiple_transforms(self) -> None:
        """Test adding multiple transforms."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .transform('app_db.orders', projection='id')
            .transform('app_db.users', projection='name')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._transforms) == 2


class TestPipelineRoute:
    """Tests for route definition."""

    def test_route_basic(self) -> None:
        """Test adding a basic route."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .route('app_db.orders', 'ods_db.ods_orders')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._routes) == 1
        assert pipeline._routes[0].source_table == 'app_db.orders'
        assert pipeline._routes[0].sink_table == 'ods_db.ods_orders'

    def test_multiple_routes(self) -> None:
        """Test adding multiple routes."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .route('app_db.orders', 'ods_db.ods_orders')
            .route('app_db.users', 'ods_db.ods_users')
            .route(r'app_db\..*', 'ods_db.others')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._routes) == 3


class TestPipelineUdf:
    """Tests for UDF definition."""

    def test_java_udf(self) -> None:
        """Test adding a Java UDF."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .with_java_udf('addone', 'com.example.AddOne')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._udfs) == 1
        assert pipeline._udfs[0].name == 'addone'
        assert pipeline._udfs[0].classpath == 'com.example.AddOne'


class TestPipelineModel:
    """Tests for AI model definition."""

    def test_ai_model(self) -> None:
        """Test adding an AI model."""
        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .with_ai_model('CHAT', 'OpenAIChatModel', openai_model='gpt-4')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        assert len(pipeline._models) == 1
        assert pipeline._models[0].model_name == 'CHAT'
        assert pipeline._models[0].class_name == 'OpenAIChatModel'


class TestPipelineYaml:
    """Tests for YAML generation."""

    def test_to_yaml(self) -> None:
        """Test generating YAML."""
        pipeline = (
            Pipeline.create('test', parallelism=2)
            .from_source('mysql', hostname='localhost', tables=r'app_db\..*')
            .route('app_db.orders', 'ods_db.ods_orders')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        yaml_str = pipeline.to_yaml()

        assert 'source:' in yaml_str
        assert 'type: mysql' in yaml_str
        assert 'sink:' in yaml_str
        assert 'type: doris' in yaml_str
        assert 'route:' in yaml_str
        assert 'pipeline:' in yaml_str
        assert 'name: test' in yaml_str
        assert 'parallelism: 2' in yaml_str

    def test_to_yaml_missing_source(self) -> None:
        """Test that missing source raises error."""
        pipeline = Pipeline.create('test')

        with pytest.raises(ValueError, match='must have a source'):
            pipeline.to_yaml()

    def test_to_yaml_missing_sink(self) -> None:
        """Test that missing sink raises error."""
        pipeline = Pipeline.create('test').from_source('mysql', hostname='localhost')

        with pytest.raises(ValueError, match='must have a sink'):
            pipeline.to_yaml()


class TestPipelineToDict:
    """Tests for to_dict method."""

    def test_to_dict(self) -> None:
        """Test getting pipeline as dictionary."""
        pipeline = (
            Pipeline.create('test', parallelism=2)
            .from_source('mysql', hostname='localhost')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        d = pipeline.to_dict()

        assert 'source' in d
        assert 'sink' in d
        assert 'pipeline' in d
        assert d['pipeline']['name'] == 'test'


class TestEnvExecute:
    """Tests for Env.execute(pipeline)."""

    def test_execute_with_missing_flink(self, tmp_path: Path) -> None:
        """Test that execute raises error if Flink home doesn't exist."""
        env = Env.create(
            flink_cdc_version='3.3.0',
            flink_home=tmp_path / 'nonexistent' / 'flink',
            env_dir=tmp_path / '.env',
        )

        pipeline = (
            Pipeline.create('test')
            .from_source('mysql', hostname='localhost')
            .to_sink('doris', fenodes='127.0.0.1:8030')
        )

        with pytest.raises(FileNotFoundError, match='Flink home does not exist'):
            env.execute(pipeline)
