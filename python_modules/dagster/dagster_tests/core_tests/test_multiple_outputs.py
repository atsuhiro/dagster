import pytest

from dagster import (
    DagsterInvariantViolationError,
    DagsterStepOutputNotFoundError,
    DependencyDefinition,
    ExpectationDefinition,
    ExpectationResult,
    InputDefinition,
    OutputDefinition,
    PipelineDefinition,
    Output,
    SolidDefinition,
    execute_pipeline,
)


def test_multiple_outputs():
    def _t_fn(*_args):
        yield Output(output_name='output_one', value='foo')
        yield Output(output_name='output_two', value='bar')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[OutputDefinition(name='output_one'), OutputDefinition(name='output_two')],
        compute_fn=_t_fn,
    )

    pipeline = PipelineDefinition(solid_defs=[solid])

    result = execute_pipeline(pipeline)
    solid_result = result.solid_result_list[0]

    assert solid_result.solid.name == 'multiple_outputs'
    assert solid_result.result_value('output_one') == 'foo'
    assert solid_result.result_value('output_two') == 'bar'

    with pytest.raises(
        DagsterInvariantViolationError, match='not_defined not defined in solid multiple_outputs'
    ):
        solid_result.result_value('not_defined')


def test_multiple_outputs_expectations():
    called = {}

    def _expect_fn_one(*_args, **_kwargs):
        called['expectation_one'] = True
        return ExpectationResult(success=True)

    def _expect_fn_two(*_args, **_kwargs):
        called['expectation_two'] = True
        return ExpectationResult(success=True)

    def _compute_fn(*_args, **_kwargs):
        yield Output('foo', 'output_one')
        yield Output('bar', 'output_two')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[
            OutputDefinition(
                name='output_one',
                expectations=[
                    ExpectationDefinition(name='some_expectation', expectation_fn=_expect_fn_one)
                ],
            ),
            OutputDefinition(
                name='output_two',
                expectations=[
                    ExpectationDefinition(
                        name='some_other_expectation', expectation_fn=_expect_fn_two
                    )
                ],
            ),
        ],
        compute_fn=_compute_fn,
    )

    pipeline = PipelineDefinition(solid_defs=[solid])

    result = execute_pipeline(pipeline)

    assert result.success
    assert called['expectation_one']
    assert called['expectation_two']


def test_wrong_multiple_output():
    def _t_fn(*_args):
        yield Output(output_name='mismatch', value='foo')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[OutputDefinition(name='output_one')],
        compute_fn=_t_fn,
    )

    pipeline = PipelineDefinition(solid_defs=[solid])

    with pytest.raises(DagsterInvariantViolationError):
        execute_pipeline(pipeline)


def test_multiple_outputs_of_same_name_disallowed():
    # make this illegal until it is supported

    def _t_fn(*_args):
        yield Output(output_name='output_one', value='foo')
        yield Output(output_name='output_one', value='foo')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[OutputDefinition(name='output_one')],
        compute_fn=_t_fn,
    )

    pipeline = PipelineDefinition(solid_defs=[solid])

    with pytest.raises(DagsterInvariantViolationError):
        execute_pipeline(pipeline)


def test_multiple_outputs_only_emit_one():
    def _t_fn(*_args):
        yield Output(output_name='output_one', value='foo')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[
            OutputDefinition(name='output_one'),
            OutputDefinition(name='output_two', is_optional=True),
        ],
        compute_fn=_t_fn,
    )

    called = {}

    def _compute_fn_one(*_args, **_kwargs):
        called['one'] = True

    downstream_one = SolidDefinition(
        name='downstream_one',
        inputs=[InputDefinition('some_input')],
        outputs=[],
        compute_fn=_compute_fn_one,
    )

    def _compute_fn_two(*_args, **_kwargs):
        raise Exception('do not call me')

    downstream_two = SolidDefinition(
        name='downstream_two',
        inputs=[InputDefinition('some_input')],
        outputs=[],
        compute_fn=_compute_fn_two,
    )

    pipeline = PipelineDefinition(
        solid_defs=[solid, downstream_one, downstream_two],
        dependencies={
            'downstream_one': {'some_input': DependencyDefinition(solid.name, output='output_one')},
            'downstream_two': {'some_input': DependencyDefinition(solid.name, output='output_two')},
        },
    )

    result = execute_pipeline(pipeline)
    assert result.success

    assert called['one']
    solid_result = result.result_for_solid('multiple_outputs')
    assert set(solid_result.result_values.keys()) == set(['output_one'])

    with pytest.raises(
        DagsterInvariantViolationError, match='not_defined not defined in solid multiple_outputs'
    ):
        solid_result.result_value('not_defined')

    with pytest.raises(DagsterInvariantViolationError, match='Did not find result output_two'):
        solid_result.result_value('output_two')

    with pytest.raises(
        DagsterInvariantViolationError,
        match='Try to get result for solid not_present in <<unnamed>>. No such solid.',
    ):
        result.result_for_solid('not_present')

    assert result.result_for_solid('downstream_two').skipped


def test_missing_non_optional_output_fails():
    def _t_fn(*_args):
        yield Output(output_name='output_one', value='foo')

    solid = SolidDefinition(
        name='multiple_outputs',
        inputs=[],
        outputs=[OutputDefinition(name='output_one'), OutputDefinition(name='output_two')],
        compute_fn=_t_fn,
    )

    pipeline = PipelineDefinition(solid_defs=[solid])

    with pytest.raises(DagsterStepOutputNotFoundError):
        execute_pipeline(pipeline)
