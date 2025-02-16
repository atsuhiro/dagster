from dagster import pipeline, solid, execute_pipeline, LocalFileHandle
from dagster.core.storage.file_manager import local_file_manager
from dagster.utils.test import get_temp_dir, get_temp_file_handle_with_data


def test_basic_file_manager_copy_handle_to_local_temp():
    foo_data = 'foo'.encode()
    with get_temp_dir() as temp_dir:
        with get_temp_file_handle_with_data(foo_data) as foo_handle:
            with local_file_manager(temp_dir) as manager:
                local_temp = manager.copy_handle_to_local_temp(foo_handle)
                assert local_temp != foo_handle.path
                with open(local_temp, 'rb') as ff:
                    assert ff.read() == foo_data


def test_basic_file_manager_execute_in_pipeline():
    called = {}

    @solid
    def file_handle(context):
        foo_bytes = 'foo'.encode()
        file_handle = context.file_manager.write_data(foo_bytes)
        assert isinstance(file_handle, LocalFileHandle)
        with open(file_handle.path, 'rb') as handle_obj:
            assert foo_bytes == handle_obj.read()

        with context.file_manager.read(file_handle) as handle_obj:
            assert foo_bytes == handle_obj.read()

        called['yup'] = True

    @pipeline
    def basic_file_manager_test():
        file_handle()  # pylint: disable=no-value-for-parameter

    result = execute_pipeline(basic_file_manager_test)
    assert result.success
    assert called['yup']
