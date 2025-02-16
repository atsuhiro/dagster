# pylint: disable=no-value-for-parameter

from dagster import (
    execute_pipeline,
    pipeline,
    resource,
    solid,
    ExecutionTargetHandle,
    Field,
    Int,
    ModeDefinition,
    MultiprocessExecutorConfig,
    RunConfig,
)


def define_resource(num):
    @resource(config_field=Field(Int, is_optional=True))
    def a_resource(context):
        return num if context.resource_config is None else context.resource_config

    return a_resource


lots_of_resources = {'R' + str(r): define_resource(r) for r in range(20)}


@solid(required_resources=set(lots_of_resources.keys()))
def all_resources(_):
    return 1


@solid(required_resources={'R1'})
def one(context):
    return 1 + context.resources.R1


@solid(required_resources={'R2'})
def two(_):
    return 1


@solid(required_resources={'R1', 'R2', 'R3'})
def one_and_two_and_three(_):
    return 1


@pipeline(mode_definitions=[ModeDefinition(resources=lots_of_resources)])
def resource_pipeline():
    all_resources()
    one()
    two()
    one_and_two_and_three()


if __name__ == '__main__':
    result = execute_pipeline(
        resource_pipeline,
        environment_dict={'storage': {'filesystem': {}}},
        run_config=RunConfig(
            executor_config=MultiprocessExecutorConfig(
                ExecutionTargetHandle.for_pipeline_fn(resource_pipeline)
            )
        ),
    )
