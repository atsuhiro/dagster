'''Functions for interactively registering pipelines and solids on the notebook manager.'''

import warnings

from dagster import check, RepositoryDefinition, PipelineDefinition
from dagster.cli.load_handle import handle_for_pipeline_cli_args
from dagster.utils import DEFAULT_REPOSITORY_YAML_FILENAME

from .manager import MANAGER_FOR_NOTEBOOK_INSTANCE


def register_pipeline(
    repository_yaml=DEFAULT_REPOSITORY_YAML_FILENAME,
    python_file=None,
    module_name=None,
    function_name=None,
    pipeline_name=None,
    repository_def=None,
    pipeline_def=None,
):
    '''Register a pipeline definition with a dagstermill notebook outside of pipeline
    execution.

    Registration enables advanced features of dagstermill, such as accepting inputs and yielding
    outputs that require serialization strategies other than pickling, and allows dagstermill to
    make a rich in-notebook context object available during interactive execution.

    Arguments:
        repository_yaml (Optional[str]): Path to a repository.yaml file. Default:
            '{default_yaml_filename}'.
        python_file (Optional[str]): Path to a Python file containing a function which, when
            executed, returns a RepositoryDefinition or PipelineDefinition.
        module_name (Optional[str]): Name of a Python module containing a function which, when
            executed, returns a RepositoryDefinition or PipelineDefinition.
        function_name (Optional[str]): Name of a function which, when executed, returns a
            RepositoryDefinition or PipelineDefinition.
        pipeline_name (Optional[str]): Name of a pipeline.
        repository_def (Optional[RepositoryDefinition]): A RepositoryDefinition.
        pipeline_def (Optional[PipelineDefinition]): A PipelineDefinition.

    If you register a pipeline, you will have access to pipeline-level context features for
    interactive execution, including logging and resources.

    To register a pipeline, you may:

        - pass a PipelineDefinition object directly as the ``pipeline_def`` argument
        - pass the ``pipeline_name`` and a RepositoryDefinition object as the ``repository_def``
          argument
        - use any valid combination of the other arguments to select a pipeline, as for all of the
          Dagster CLI tools that target pipelines:

            * ``pipeline_name`` (provided that './{default_yaml_filename}' exists)
            * ``pipeline_name`` and ``repository_yaml``
            * ``pipeline_name``, ``module_name``, and ``function_name``
            * ``pipeline_name``, ``python_file``, and ``function_name
            * ``module_name`` and ``function_name``
            * ``python_file`` and ``function_name``

    '''.format(
        default_yaml_filename=DEFAULT_REPOSITORY_YAML_FILENAME
    )

    check.opt_str_param(repository_yaml, 'repository_yaml')
    check.opt_str_param(python_file, 'python_file')
    check.opt_str_param(module_name, 'module_name')
    check.opt_str_param(function_name, 'function_name')
    check.opt_str_param(pipeline_name, 'pipeline_name')
    check.opt_inst_param(repository_def, 'repository_def', RepositoryDefinition)
    check.opt_inst_param(pipeline_def, 'pipeline_def', PipelineDefinition)

    if pipeline_def:
        if (
            repository_yaml != DEFAULT_REPOSITORY_YAML_FILENAME
            or python_file
            or module_name
            or function_name
            or pipeline_name
            or repository_def
        ):
            warnings.warn(
                'You passed a pipeline_def directly for registration, but one or more of'
                'repository_yaml, python_file, module_name, function_name, pipeline_name, and '
                'repository_def were also set: ignoring in favor of the pipeline definition.'
            )

    elif repository_def:
        check.invariant(
            pipeline_name,
            'You must specify the pipeline_name when passing a repository_def directly for '
            'registration.',
        )
        if (
            repository_yaml != DEFAULT_REPOSITORY_YAML_FILENAME
            or python_file
            or module_name
            or function_name
        ):
            warnings.warn(
                'You passed a repository_def directly for registration, but one or more of'
                'repository_yaml, python_file, module_name, and function_name were also set: '
                'ignoring in favor of the repository definition and pipeline_name.'
            )
        pipeline_def = repository_def.get_pipeline(pipeline_name)

    else:
        handle = handle_for_pipeline_cli_args(
            {
                'repository_yaml': repository_yaml or DEFAULT_REPOSITORY_YAML_FILENAME,
                'module_name': module_name,
                'pipeline_name': pipeline_name,
                'python_file': python_file,
                'function_name': function_name,
            }
        )
        pipeline_def = handle.build_pipeline_definition()

    return MANAGER_FOR_NOTEBOOK_INSTANCE.register(pipeline_def=pipeline_def)


def register_solid(solid_name=None, solid_def=None):
    '''Register a solid with a dagstermill notebook outside of pipeline execution.

    Arguments:
        solid_name (Optional[str]): Name of the Dagstermill solid.
        solid_def (Optional[SolidDefinition]): SolidDefinition for the Dagstermill solid.

    If you register a solid, you will have access to solid-level context features on the
    in-notebook/interactive context object, particularly the ``solid_config`` attribute.
    Registering *only* a solid can simplify notebook reuse across multiple pipelines.

    To register a solid, you may either:

        - pass the SolidDefinition object directly as the ``solid_def`` argument
        - pass the ``solid_name`` argument, provided that a pipeline has already been registered --
          the solid will be looked up on this pipeline

    '''
    check.invariant(
        (solid_def or solid_name) and not (solid_def and solid_name),
        'You must specify only one of solid_def (a SolidDefinition object) or solid_name '
        '(the str name of a solid).',
    )

    if solid_name:
        check.invariant(
            MANAGER_FOR_NOTEBOOK_INSTANCE.pipeline_def,
            'A pipeline must already be registered if solid_name is used.',
        )

        solid_def = MANAGER_FOR_NOTEBOOK_INSTANCE.pipeline_def.solid_named(solid_name).definition

    return MANAGER_FOR_NOTEBOOK_INSTANCE.register(solid_def=solid_def)
