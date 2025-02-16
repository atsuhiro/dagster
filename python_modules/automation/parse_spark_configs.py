'''Spark config codegen.

This script parses the Spark configuration parameters downloaded from the Spark Github repository,
and codegens a file that contains dagster configurations for these parameters.
'''
from __future__ import print_function

import re
import sys

from enum import Enum

import click
import requests
import pytablereader as ptr

from printer import IndentingBufferPrinter

SPARK_VERSION = "v2.4.0"
TABLE_REGEX = r"### (.{,30}?)\n\n(<table.*?>.*?<\/table>)"


class ConfigType(Enum):
    STRING = 'String'
    INT = 'Int'
    FLOAT = 'Float'
    BOOL = 'Bool'
    MEMORY = 'String'  # TODO: We should handle memory field types
    TIME = 'String'  # TODO: We should handle time field types


CONFIG_TYPES = {
    #
    # APPLICATION PROPERTIES
    'spark.app.name': ConfigType.STRING,
    'spark.driver.cores': ConfigType.INT,
    'spark.driver.maxResultSize': ConfigType.MEMORY,
    'spark.driver.memory': ConfigType.MEMORY,
    'spark.driver.memoryOverhead': ConfigType.MEMORY,
    'spark.executor.memory': ConfigType.MEMORY,
    'spark.executor.pyspark.memory': ConfigType.MEMORY,
    'spark.executor.memoryOverhead	': ConfigType.MEMORY,
    'spark.extraListeners': ConfigType.STRING,
    'spark.local.dir': ConfigType.STRING,
    'spark.logConf': ConfigType.BOOL,
    # TODO: Validate against https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
    'spark.master': ConfigType.STRING,
    # TODO: Validate against client/cluster *only*.
    'spark.submit.deployMode': ConfigType.STRING,
    'spark.log.callerContext': ConfigType.STRING,
    'spark.driver.supervise': ConfigType.BOOL,
    #
    # RUNTIME ENVIRONMENT
    'spark.driver.extraClassPath': ConfigType.STRING,
    'spark.driver.extraJavaOptions': ConfigType.STRING,
    'spark.driver.extraLibraryPath': ConfigType.STRING,
    'spark.driver.userClassPathFirst': ConfigType.BOOL,
    'spark.executor.extraClassPath': ConfigType.STRING,
    'spark.executor.extraJavaOptions': ConfigType.STRING,
    'spark.executor.extraLibraryPath': ConfigType.STRING,
    'spark.executor.logs.rolling.maxRetainedFiles': ConfigType.INT,
    'spark.executor.logs.rolling.enableCompression': ConfigType.BOOL,
    'spark.executor.logs.rolling.maxSize': ConfigType.INT,
    # TODO: Can only be 'time' or 'size'
    'spark.executor.logs.rolling.strategy': ConfigType.STRING,
    'spark.executor.logs.rolling.time.interval': ConfigType.STRING,
    'spark.executor.userClassPathFirst': ConfigType.BOOL,
    'spark.redaction.regex': ConfigType.STRING,
    'spark.python.profile': ConfigType.BOOL,
    # TODO: Should be a path?
    'spark.python.profile.dump': ConfigType.STRING,
    'spark.python.worker.memory': ConfigType.MEMORY,
    'spark.python.worker.reuse': ConfigType.BOOL,
    'spark.files': ConfigType.STRING,
    'spark.submit.pyFiles': ConfigType.STRING,
    'spark.jars': ConfigType.STRING,
    'spark.jars.packages': ConfigType.STRING,
    'spark.jars.excludes': ConfigType.STRING,
    'spark.jars.ivy': ConfigType.STRING,
    'spark.jars.ivySettings': ConfigType.STRING,
    'spark.jars.repositories': ConfigType.STRING,
    'spark.pyspark.driver.python': ConfigType.STRING,
    'spark.pyspark.python': ConfigType.STRING,
    #
    # SHUFFLE BEHAVIOR
    'spark.reducer.maxSizeInFlight': ConfigType.MEMORY,
    'spark.reducer.maxReqsInFlight': ConfigType.INT,
    'spark.reducer.maxBlocksInFlightPerAddress': ConfigType.INT,
    'spark.maxRemoteBlockSizeFetchToMem': ConfigType.INT,
    'spark.shuffle.compress': ConfigType.BOOL,
    'spark.shuffle.file.buffer': ConfigType.MEMORY,
    'spark.shuffle.io.maxRetries': ConfigType.INT,
    'spark.shuffle.io.numConnectionsPerPeer': ConfigType.INT,
    'spark.shuffle.io.preferDirectBufs': ConfigType.BOOL,
    'spark.shuffle.io.retryWait': ConfigType.TIME,
    'spark.shuffle.service.enabled': ConfigType.BOOL,
    'spark.shuffle.service.port': ConfigType.INT,
    'spark.shuffle.service.index.cache.size': ConfigType.MEMORY,
    'spark.shuffle.maxChunksBeingTransferred': ConfigType.INT,
    'spark.shuffle.sort.bypassMergeThreshold': ConfigType.INT,
    'spark.shuffle.spill.compress': ConfigType.BOOL,
    'spark.shuffle.accurateBlockThreshold': ConfigType.INT,
    'spark.shuffle.registration.timeout': ConfigType.INT,
    'spark.shuffle.registration.maxAttempts': ConfigType.INT,
    #
    # SPARK UI
    ### TODO
    #
    # COMPRESSION AND SERIALIZATION
    ### TODO
    #
    # MEMORY MANAGEMENT
    'spark.memory.fraction': ConfigType.FLOAT,
    'spark.memory.storageFraction': ConfigType.FLOAT,
    'spark.memory.offHeap.enabled': ConfigType.BOOL,
    'spark.memory.offHeap.size': ConfigType.INT,
    'spark.memory.useLegacyMode': ConfigType.BOOL,
    'spark.shuffle.memoryFraction': ConfigType.FLOAT,
    'spark.storage.memoryFraction': ConfigType.FLOAT,
    'spark.storage.unrollFraction': ConfigType.FLOAT,
    'spark.storage.replication.proactive': ConfigType.BOOL,
    'spark.cleaner.periodicGC.interval': ConfigType.TIME,
    'spark.cleaner.referenceTracking': ConfigType.BOOL,
    'spark.cleaner.referenceTracking.blocking': ConfigType.BOOL,
    'spark.cleaner.referenceTracking.blocking.shuffle': ConfigType.BOOL,
    'spark.cleaner.referenceTracking.cleanCheckpoints': ConfigType.BOOL,
    #
    # EXECUTION BEHAVIOR
    'spark.broadcast.blockSize': ConfigType.MEMORY,
    'spark.executor.cores': ConfigType.INT,
    'spark.default.parallelism': ConfigType.INT,
    'spark.executor.heartbeatInterval': ConfigType.TIME,
    'spark.files.fetchTimeout': ConfigType.TIME,
    'spark.files.useFetchCache': ConfigType.BOOL,
    'spark.files.overwrite': ConfigType.BOOL,
    'spark.files.maxPartitionBytes': ConfigType.INT,
    'spark.files.openCostInBytes': ConfigType.INT,
    'spark.hadoop.cloneConf': ConfigType.BOOL,
    'spark.hadoop.validateOutputSpecs': ConfigType.BOOL,
    'spark.storage.memoryMapThreshold': ConfigType.MEMORY,
    # TODO: Can only be 1 or 2.
    'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': ConfigType.INT,
    #
    # NETWORKING
    ### TODO
    #
    # SCHEDULING
    ### TODO
    #
    # DYNAMIC ALLOCATION
    ### TODO
}


class SparkConfig:
    def __init__(self, path, default, meaning):
        self.path = path

        # The original documentation strings include extraneous newlines, spaces
        WHITESPACE_REGEX = r'\s+'
        self.default = re.sub(WHITESPACE_REGEX, ' ', str(default)).strip()
        self.meaning = re.sub(WHITESPACE_REGEX, ' ', meaning).strip()

    @property
    def split_path(self):
        return self.path.split('.')

    def write(self, printer):
        config_type = CONFIG_TYPES.get(self.path, ConfigType.STRING).value

        printer.append('Field(')
        with printer.with_indent():
            printer.line('')
            printer.line('{config_type},'.format(config_type=config_type))
            printer.append("description='''")
            printer.append(self.meaning)
            printer.line("''',")
            # printer.line("default_value='{}',".format(self.default))
            printer.line('is_optional=True,')

        printer.append(')')


class SparkConfigNode:
    def __init__(self, value=None):
        self.value = value
        self.children = {}

    def write(self, printer):
        if not self.children:
            self.value.write(printer)
        else:
            if self.value:
                retdict = {'root': self.value}
                retdict.update(self.children)
            else:
                retdict = self.children

            printer.append('Field(')
            printer.line('')
            with printer.with_indent():
                printer.line('PermissiveDict(')
                with printer.with_indent():
                    printer.line('fields={')
                    with printer.with_indent():
                        for (k, v) in retdict.items():
                            with printer.with_indent():
                                printer.append("'{}': ".format(k))
                            v.write(printer)

                            printer.line(',')
                    printer.line('}')
                printer.line(')')
            printer.line(')')
        return printer.read()


@click.command()
@click.option('--output-file', help='Base path to write config file to', required=True)
def run(output_file):
    r = requests.get(
        'https://raw.githubusercontent.com/apache/spark/{}/docs/configuration.md'.format(
            SPARK_VERSION
        )
    )

    tables = re.findall(TABLE_REGEX, r.text, re.DOTALL | re.MULTILINE)

    spark_configs = []
    for name, table in tables:
        parsed_table = list(ptr.HtmlTableTextLoader(table).load())[0]
        df = parsed_table.as_dataframe()
        for _, row in df.iterrows():
            s = SparkConfig(row['Property Name'], row['Default'], name + ": " + row['Meaning'])
            spark_configs.append(s)

    result = SparkConfigNode()
    for s in spark_configs:
        # TODO: we should handle this thing
        if s.path == 'spark.executorEnv.[EnvironmentVariableName]':
            continue

        print(s.path, file=sys.stderr)
        key_path = s.split_path
        d = result
        while key_path:
            key = key_path.pop(0)
            if key not in d.children:
                d.children[key] = SparkConfigNode()
            d = d.children[key]
        d.value = s

    with IndentingBufferPrinter() as printer:
        printer.write_header()
        printer.line('from dagster import Bool, Field, Float, Int, PermissiveDict, String')
        printer.blank_line()
        printer.blank_line()
        printer.line('# pylint: disable=line-too-long')
        printer.line('def spark_config():')
        with printer.with_indent():
            printer.append('return ')
            result.write(printer)
        printer.line('# pylint: enable=line-too-long')
        with open(output_file, 'wb') as f:
            f.write(printer.read().strip().encode())


if __name__ == "__main__":
    run()  # pylint:disable=E1120
