import logging
import time

import boto3

from dagster import check, Dict, Field, logger, seven, String
from dagster.core.log_manager import coerce_valid_log_level

# The maximum batch size is 1,048,576 bytes, and this size is calculated as the sum of all event
# messages in UTF-8, plus 26 bytes for each log event.
MAXIMUM_BATCH_SIZE = 1048576
OVERHEAD = 26

class CloudwatchLogsHandler(logging.Handler):
    def __init__(self, log_group_name, log_stream_name):
        self.client = boto3.client('logs')
        self.log_group_name = check.str_param(log_group_name, 'log_group_name')
        self.log_stream_name = check.str_param(log_stream_name, 'log_stream_name')
        self.overhead = OVERHEAD
        self.maximum_batch_size = MAXIMUM_BATCH_SIZE
        self.sequence_token = None

        super(CloudwatchLogsHandler, self).__init__()

    def emit(self, record):
        import pdb; pdb.set_trace()
        message = seven.json.dumps(record.__dict__)
        timestamp = int(time.time())
        params = {
            'logGroupName': self.log_group_name,
            'logStreamName': self.log_stream_name,
            'logEvents': [
                {
                    'timestamp': timestamp,
                    'message': message
                }
            ]
        }
        if self.sequence_token is not None:
            params['sequenceToken'] = self.sequence_token

        res = self.client.put_log_events(**params)
        # handle sequence token
        # handle errors

@logger(
    config_field=Field(
        Dict(
            {
                'log_level': Field(String, is_optional=True, default_value='INFO'),
                'name': Field(String, is_optional=True, default_value='dagster'),
                'log_group_name': Field(String),
                'log_stream_name': Field(String),
            }
        )
    ),
    description='The default colored console logger.',
)
def colored_console_logger(init_context):

    level = coerce_valid_log_level(init_context.logger_config['log_level'])
    name = init_context.logger_config['name']

    klass = logging.getLoggerClass()
    logger_ = klass(name, level=level)

    logger_.addHandler(
        CloudwatchLogsHandler(
            init_context.logger_config['log_group_name'],
            init_context.logger_config['log_stream_name']
        )
    )
    return logger_
