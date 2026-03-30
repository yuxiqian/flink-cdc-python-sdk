"""Tests for pipeline definition data structures."""

from flink_cdc_python.compose import (
    ModelDef,
    PipelineDef,
    RouteDef,
    SinkDef,
    SourceDef,
    TransformDef,
    UdfDef,
)


class TestSourceDef:
    """Tests for SourceDef."""

    def test_basic_source(self) -> None:
        """Test creating a basic source."""
        source = SourceDef(type='mysql', config={'hostname': 'localhost', 'port': 3306})
        result = source.to_dict()
        assert result['type'] == 'mysql'
        assert result['hostname'] == 'localhost'
        assert result['port'] == 3306

    def test_source_with_name(self) -> None:
        """Test creating a source with name."""
        source = SourceDef(type='mysql', config={'hostname': 'localhost'}, name='source1')
        result = source.to_dict()
        assert result['type'] == 'mysql'
        assert result['name'] == 'source1'
        assert result['hostname'] == 'localhost'


class TestSinkDef:
    """Tests for SinkDef."""

    def test_basic_sink(self) -> None:
        """Test creating a basic sink."""
        sink = SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'})
        result = sink.to_dict()
        assert result['type'] == 'doris'
        assert result['fenodes'] == '127.0.0.1:8030'

    def test_sink_with_name(self) -> None:
        """Test creating a sink with name."""
        sink = SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'}, name='sink1')
        result = sink.to_dict()
        assert result['type'] == 'doris'
        assert result['name'] == 'sink1'


class TestTransformDef:
    """Tests for TransformDef."""

    def test_basic_transform(self) -> None:
        """Test creating a basic transform."""
        transform = TransformDef(source_table='app_db.orders', projection='id, name')
        result = transform.to_dict()
        assert result['source-table'] == 'app_db.orders'
        assert result['projection'] == 'id, name'

    def test_transform_with_all_options(self) -> None:
        """Test creating a transform with all options."""
        transform = TransformDef(
            source_table='app_db.orders',
            projection='id, name',
            filtering='id > 10',
            primary_keys=['id'],
            partition_keys=['date'],
            table_options={'bucket': '4'},
            description='Transform orders',
        )
        result = transform.to_dict()
        assert result['source-table'] == 'app_db.orders'
        assert result['projection'] == 'id, name'
        assert result['filter'] == 'id > 10'
        assert result['primary-keys'] == 'id'
        assert result['partition-keys'] == 'date'
        # table_options is converted to string format
        assert result['table-options'] == 'bucket=4'
        assert result['description'] == 'Transform orders'


class TestRouteDef:
    """Tests for RouteDef."""

    def test_basic_route(self) -> None:
        """Test creating a basic route."""
        route = RouteDef(source_table='app_db.orders', sink_table='ods_db.ods_orders')
        result = route.to_dict()
        assert result['source-table'] == 'app_db.orders'
        assert result['sink-table'] == 'ods_db.ods_orders'

    def test_route_with_description(self) -> None:
        """Test creating a route with description."""
        route = RouteDef(
            source_table='app_db.orders',
            sink_table='ods_db.ods_orders',
            description='Route orders',
        )
        result = route.to_dict()
        assert result['source-table'] == 'app_db.orders'
        assert result['sink-table'] == 'ods_db.ods_orders'
        assert result['description'] == 'Route orders'


class TestUdfDef:
    """Tests for UdfDef."""

    def test_basic_udf(self) -> None:
        """Test creating a basic UDF."""
        udf = UdfDef(name='addone', classpath='com.example.AddOne')
        result = udf.to_dict()
        assert result['name'] == 'addone'
        assert result['classpath'] == 'com.example.AddOne'

    def test_udf_with_options(self) -> None:
        """Test creating a UDF with options."""
        udf = UdfDef(name='addone', classpath='com.example.AddOne', options={'param': 'value'})
        result = udf.to_dict()
        assert result['name'] == 'addone'
        assert result['classpath'] == 'com.example.AddOne'
        # options are stored under 'options' key
        assert result['options'] == {'param': 'value'}


class TestModelDef:
    """Tests for ModelDef."""

    def test_basic_model(self) -> None:
        """Test creating a basic model."""
        model = ModelDef(model_name='CHAT', class_name='OpenAIChatModel')
        result = model.to_dict()
        assert result['name'] == 'CHAT'
        assert result['model'] == 'OpenAIChatModel'

    def test_model_with_parameters(self) -> None:
        """Test creating a model with parameters."""
        model = ModelDef(
            model_name='CHAT',
            class_name='OpenAIChatModel',
            parameters={'openai_model': 'gpt-4', 'openai_apikey': 'key'},
        )
        result = model.to_dict()
        assert result['name'] == 'CHAT'
        assert result['model'] == 'OpenAIChatModel'
        assert result['parameters']['openai_model'] == 'gpt-4'


class TestPipelineDef:
    """Tests for PipelineDef."""

    def test_minimal_pipeline(self) -> None:
        """Test creating a minimal pipeline."""
        source = SourceDef(type='mysql', config={'hostname': 'localhost'})
        sink = SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'})

        pipeline = PipelineDef(source=source, sink=sink)
        result = pipeline.to_dict()

        assert result['source']['type'] == 'mysql'
        assert result['sink']['type'] == 'doris'
        assert result['pipeline'] == {}

    def test_pipeline_with_config(self) -> None:
        """Test creating a pipeline with configuration."""
        source = SourceDef(type='mysql', config={'hostname': 'localhost'})
        sink = SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'})

        pipeline = PipelineDef(
            source=source,
            sink=sink,
            config={'name': 'test-pipeline', 'parallelism': 2},
        )
        result = pipeline.to_dict()

        assert result['pipeline']['name'] == 'test-pipeline'
        assert result['pipeline']['parallelism'] == 2

    def test_full_pipeline(self) -> None:
        """Test creating a full pipeline with all components."""
        source = SourceDef(type='mysql', config={'hostname': 'localhost', 'tables': 'db\\..*'})
        sink = SinkDef(type='doris', config={'fenodes': '127.0.0.1:8030'})

        pipeline = PipelineDef(
            source=source,
            sink=sink,
            config={'name': 'full-pipeline', 'parallelism': 4},
            routes=[
                RouteDef(source_table='db.orders', sink_table='ods.orders'),
            ],
            transforms=[
                TransformDef(source_table='db.orders', projection='id, name'),
            ],
            udfs=[
                UdfDef(name='addone', classpath='com.example.AddOne'),
            ],
            models=[
                ModelDef(model_name='CHAT', class_name='OpenAIChatModel'),
            ],
        )
        result = pipeline.to_dict()

        # Verify source and sink
        assert result['source']['type'] == 'mysql'
        assert result['sink']['type'] == 'doris'

        # Verify transforms
        assert len(result['transform']) == 1
        assert result['transform'][0]['source-table'] == 'db.orders'

        # Verify routes
        assert len(result['route']) == 1
        assert result['route'][0]['source-table'] == 'db.orders'

        # Verify pipeline config with UDFs and models
        assert result['pipeline']['name'] == 'full-pipeline'
        assert result['pipeline']['parallelism'] == 4
        assert len(result['pipeline']['user-defined-function']) == 1
        assert len(result['pipeline']['model']) == 1
