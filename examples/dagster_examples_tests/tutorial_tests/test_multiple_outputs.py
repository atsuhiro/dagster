# pylint: disable=W0622,W0614,W0401
import pytest


from dagster import DagsterInvariantViolationError, execute_pipeline
from dagster_examples.intro_tutorial.multiple_outputs import multiple_outputs_pipeline
from dagster_examples.intro_tutorial.multiple_outputs_yield import multiple_outputs_yield_pipeline
from dagster_examples.intro_tutorial.multiple_outputs_conditional import (
    multiple_outputs_conditional_pipeline,
)


def test_intro_tutorial_multiple_outputs():
    result = execute_pipeline(multiple_outputs_pipeline)

    assert result.success
    assert result.result_for_solid('return_dict_results').result_value('out_one') == 23
    assert result.result_for_solid('return_dict_results').result_value('out_two') == 45
    assert result.result_for_solid('log_num').result_value() == 23
    assert result.result_for_solid('log_num_squared').result_value() == 45 * 45


def test_intro_tutorial_multiple_outputs_yield():
    result = execute_pipeline(multiple_outputs_yield_pipeline)

    assert result.success
    assert result.result_for_solid('yield_outputs').result_value('out_one') == 23
    assert result.result_for_solid('yield_outputs').result_value('out_two') == 45
    assert result.result_for_solid('log_num').result_value() == 23
    assert result.result_for_solid('log_num_squared').result_value() == 45 * 45


def test_intro_tutorial_multiple_outputs_conditional():
    result = execute_pipeline(
        multiple_outputs_conditional_pipeline, {'solids': {'conditional': {'config': 'out_two'}}}
    )

    # successful things
    assert result.success
    assert result.result_for_solid('conditional').result_value('out_two') == 45
    assert result.result_for_solid('log_num_squared').result_value() == 45 * 45

    # unsuccessful things
    with pytest.raises(DagsterInvariantViolationError):
        assert result.result_for_solid('conditional').result_value('out_one') == 45
