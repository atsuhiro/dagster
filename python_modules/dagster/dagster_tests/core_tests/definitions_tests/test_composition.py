# pylint: disable=no-value-for-parameter

import pytest

from dagster import (
    DependencyDefinition,
    Field,
    InputDefinition,
    Int,
    Nothing,
    OutputDefinition,
    PipelineDefinition,
    RepositoryDefinition,
    Output,
    composite_solid,
    execute_pipeline,
    lambda_solid,
    pipeline,
    solid,
)

from dagster.core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError


def builder(graph):
    return graph.add_one(graph.return_one())


@lambda_solid
def echo(blah):
    return blah


@lambda_solid
def return_one():
    return 1


@lambda_solid
def return_two():
    return 2


@lambda_solid(inputs=[InputDefinition('num')])
def add_one(num):
    return num + 1


@lambda_solid(inputs=[InputDefinition('num')])
def pipe(num):
    return num


@solid(inputs=[InputDefinition('int_1', Int), InputDefinition('int_2', Int)])
def adder(_context, int_1, int_2):
    return int_1 + int_2


@solid(outputs=[OutputDefinition(Int, 'one'), OutputDefinition(Int, 'two')])
def return_mult(_context):
    yield Output(1, 'one')
    yield Output(2, 'two')


def test_basic():
    @composite_solid
    def test():
        one = return_one()
        add_one(num=one)

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test]))
        .result_for_handle('test.add_one')
        .result_value()
        == 2
    )


def test_args():
    @composite_solid
    def _test_1():
        one = return_one()
        add_one(one)

    @composite_solid
    def _test_2():
        adder(return_one(), return_two())

    @composite_solid
    def _test_3():
        adder(int_1=return_one(), int_2=return_two())

    @composite_solid
    def _test_4():
        adder(return_one(), return_two())

    @composite_solid
    def _test_5():
        adder(return_one(), int_2=return_two())

    @composite_solid
    def _test_6():
        adder(return_one())

    @composite_solid
    def _test_7():
        adder(int_2=return_two())


def test_arg_fails():

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _fail_2():
            adder(return_one(), 1)

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _fail_3():
            # pylint: disable=too-many-function-args
            adder(return_one(), return_two(), return_one.alias('three')())


def test_mult_out_fail():

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _test():
            ret = return_mult()
            add_one(ret)


def test_dupes_fail():
    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid
        def _test():
            one, two = return_mult()
            add_one(num=one)
            add_one(num=two)


def test_multiple():
    @composite_solid
    def test():
        one, two = return_mult()
        add_one(num=one)
        add_one.alias('add_one_2')(num=two)

    results = execute_pipeline(PipelineDefinition(solid_defs=[test]))
    assert results.result_for_handle('test.add_one').result_value() == 2
    assert results.result_for_handle('test.add_one_2').result_value() == 3


def test_two_inputs_with_dsl():
    @lambda_solid(inputs=[InputDefinition('num_one'), InputDefinition('num_two')])
    def add(num_one, num_two):
        return num_one + num_two

    @lambda_solid
    def return_three():
        return 3

    @composite_solid
    def test():
        add(num_one=return_two(), num_two=return_three())

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test]))
        .result_for_handle('test.add')
        .result_value()
        == 5
    )


def test_basic_aliasing_with_dsl():
    @composite_solid
    def test():
        add_one.alias('renamed')(num=return_one())

    assert (
        execute_pipeline(PipelineDefinition(solid_defs=[test]))
        .result_for_handle('test.renamed')
        .result_value()
        == 2
    )


def test_diamond_graph():
    @solid(outputs=[OutputDefinition(name='value_one'), OutputDefinition(name='value_two')])
    def emit_values(_context):
        yield Output(1, 'value_one')
        yield Output(2, 'value_two')

    @lambda_solid(inputs=[InputDefinition('num_one'), InputDefinition('num_two')])
    def add(num_one, num_two):
        return num_one + num_two

    @composite_solid
    def diamond():
        value_one, value_two = emit_values()
        add(num_one=add_one(num=value_one), num_two=add_one.alias('renamed')(num=value_two))

    result = execute_pipeline(PipelineDefinition(solid_defs=[diamond]))

    assert result.result_for_handle('diamond.add').result_value() == 5


def test_mapping():
    @lambda_solid(inputs=[InputDefinition('num_in', Int)], output=OutputDefinition(Int, 'num_out'))
    def double(num_in):
        return num_in * 2

    @composite_solid(
        inputs=[InputDefinition('num_in', Int)], outputs=[OutputDefinition(Int, 'num_out')]
    )
    def composed_inout(num_in):
        return double(num_in=num_in)

    # have to use "pipe" solid since "result_for_solid" doesnt work with composite mappings
    assert (
        execute_pipeline(
            PipelineDefinition(
                solid_defs=[return_one, composed_inout, pipe],
                dependencies={
                    'composed_inout': {'num_in': DependencyDefinition('return_one')},
                    'pipe': {'num': DependencyDefinition('composed_inout', 'num_out')},
                },
            )
        )
        .result_for_solid('pipe')
        .result_value()
        == 2
    )


def test_mapping_args_kwargs():
    @lambda_solid
    def take(a, b, c):
        return (a, b, c)

    @composite_solid
    def maps(m_c, m_b, m_a):
        take(m_a, b=m_b, c=m_c)

    assert maps.input_mappings[2].definition.name == 'm_a'
    assert maps.input_mappings[2].input_name == 'a'

    assert maps.input_mappings[1].definition.name == 'm_b'
    assert maps.input_mappings[1].input_name == 'b'

    assert maps.input_mappings[0].definition.name == 'm_c'
    assert maps.input_mappings[0].input_name == 'c'


def test_output_map_mult():
    @composite_solid(outputs=[OutputDefinition(Int, 'one'), OutputDefinition(Int, 'two')])
    def wrap_mult():
        return return_mult()

    @pipeline
    def mult_pipe():
        one, two = wrap_mult()
        echo.alias('echo_one')(one)
        echo.alias('echo_two')(two)

    result = execute_pipeline(mult_pipe)
    assert result.result_for_solid('echo_one').result_value() == 1
    assert result.result_for_solid('echo_two').result_value() == 2


def test_output_map_mult_swizzle():
    @composite_solid(outputs=[OutputDefinition(Int, 'x'), OutputDefinition(Int, 'y')])
    def wrap_mult():
        one, two = return_mult()
        return {'x': one, 'y': two}

    @pipeline
    def mult_pipe():
        x, y = wrap_mult()
        echo.alias('echo_x')(x)
        echo.alias('echo_y')(y)

    result = execute_pipeline(mult_pipe)
    assert result.result_for_solid('echo_x').result_value() == 1
    assert result.result_for_solid('echo_y').result_value() == 2


def test_output_map_fail():
    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(outputs=[OutputDefinition(Int, 'one'), OutputDefinition(Int, 'two')])
        def _bad(_context):
            return return_one()

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(outputs=[OutputDefinition(Int, 'one'), OutputDefinition(Int, 'two')])
        def _bad(_context):
            return {'one': 1}

    with pytest.raises(DagsterInvalidDefinitionError):

        @composite_solid(outputs=[OutputDefinition(Int, 'three'), OutputDefinition(Int, 'four')])
        def _bad():
            return return_mult()


def test_deep_graph():
    @solid(config_field=Field(Int))
    def download_num(context):
        return context.solid_config

    @lambda_solid(inputs=[InputDefinition('num')])
    def unzip_num(num):
        return num

    @lambda_solid(inputs=[InputDefinition('num')])
    def ingest_num(num):
        return num

    @lambda_solid(inputs=[InputDefinition('num')])
    def subsample_num(num):
        return num

    @lambda_solid(inputs=[InputDefinition('num')])
    def canonicalize_num(num):
        return num

    @lambda_solid(inputs=[InputDefinition('num')])
    def load_num(num):
        return num + 3

    @composite_solid(outputs=[OutputDefinition(Int)])
    def test():
        return load_num(
            num=canonicalize_num(
                num=subsample_num(num=ingest_num(num=unzip_num(num=download_num())))
            )
        )

    result = execute_pipeline(
        PipelineDefinition(solid_defs=[test]),
        {'solids': {'test': {'solids': {'download_num': {'config': 123}}}}},
    )
    assert result.result_for_handle('test.canonicalize_num').result_value() == 123
    assert result.result_for_handle('test.load_num').result_value() == 126


def test_recursion():
    @composite_solid
    def outer():
        @composite_solid(outputs=[OutputDefinition()])
        def inner():
            return add_one(return_one())

        add_one(inner())

    assert execute_pipeline(PipelineDefinition(solid_defs=[outer])).success


class Garbage(Exception):
    pass


def test_recursion_with_exceptions():
    called = {}

    @pipeline
    def recurse():
        @composite_solid
        def outer():
            try:

                @composite_solid
                def throws():
                    called['throws'] = True
                    raise Garbage()

                throws()
            except Garbage:
                add_one(return_one())

        outer()

    assert execute_pipeline(recurse).success
    assert called['throws'] is True


def test_pipeline_has_solid_def():
    @composite_solid(outputs=[OutputDefinition()])
    def inner():
        return add_one(return_one())

    @composite_solid
    def outer():
        add_one(inner())

    @pipeline
    def a_pipeline():
        outer()

    assert a_pipeline.has_solid_def('add_one')
    assert a_pipeline.has_solid_def('outer')
    assert a_pipeline.has_solid_def('inner')


def test_repositry_has_solid_def():
    @composite_solid(outputs=[OutputDefinition()])
    def inner():
        return add_one(return_one())

    @composite_solid
    def outer():
        add_one(inner())

    @pipeline
    def a_pipeline():
        outer()

    repo_def = RepositoryDefinition.eager_construction(
        name='has_solid_def_test', pipelines=[a_pipeline]
    )

    assert repo_def.solid_def_named('inner')


def test_mapping_args_ordering():
    @lambda_solid
    def take(a, b, c):
        assert a == 'a'
        assert b == 'b'
        assert c == 'c'

    @composite_solid
    def swizzle(b, a, c):
        take(a, b, c)

    @composite_solid
    def swizzle_2(c, b, a):
        swizzle(b, a=a, c=c)

    @pipeline
    def ordered():
        swizzle_2()

    for mapping in swizzle.input_mappings:
        assert mapping.definition.name == mapping.input_name

    for mapping in swizzle_2.input_mappings:
        assert mapping.definition.name == mapping.input_name

    execute_pipeline(
        ordered,
        {
            'solids': {
                'swizzle_2': {
                    'inputs': {'a': {'value': 'a'}, 'b': {'value': 'b'}, 'c': {'value': 'c'}}
                }
            }
        },
    )


def test_unused_mapping():
    with pytest.raises(DagsterInvalidDefinitionError, match='unmapped input'):

        @composite_solid
        def unused_mapping(_):
            return_one()


def test_calling_soild_outside_fn():
    with pytest.raises(DagsterInvariantViolationError, match='outside of a composition function'):

        return_one()


def test_compose_nothing():
    @lambda_solid(inputs=[InputDefinition('start', Nothing)])
    def go():
        pass

    @composite_solid(inputs=[InputDefinition('start', Nothing)])
    def _compose(start):
        go(start)  # pylint: disable=too-many-function-args
