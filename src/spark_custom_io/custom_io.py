from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row
from pyspark import TaskContext
from typing import Iterator, Tuple

from json import dump, load


class SimpleCommitMessage(WriterCommitMessage):
    def __init__(self, partition_id: int, count: int):
        self.partition_id = partition_id
        self.count = count

class CustomWriter(DataSourceWriter):
    def __init__(self, options):
        self.prefix = options.get("prefix", "Writing row: ")
        self.outputPath = options.get("outputPath")
        self.rootPath = self.outputPath + "_TEST"
        os.makedirs(self.rootPath, exist_ok=True)

    def write(self, iterator: Iterator[Tuple]) -> SimpleCommitMessage:
        partition_id = TaskContext.get().partitionId()
        data = []
        with open(self.rootPath + f"/{partition_id}.txt", "a") as f:
            data = [row.asDict() for row in iterator]
            dump(data, f)
        
        return SimpleCommitMessage(partition_id=partition_id, count=len(data))

    def commit(self, messages: list[SimpleCommitMessage]) -> None:
        metadata = [{"partition": m.partition_id, "count": m.count} for m in messages]
        with open(self.rootPath + "/.metadata.json", "w") as f:
            dump(metadata, f)
        
        total = sum(m.count for m in messages)
        print(f"Driver: Wrote {total} rows.")

class CustomReader(DataSourceReader):
    def __init__(self, options, schema):
        self.inputPath = options.get("inputPath")

    def partitions(self):
        metadata = []
        with open(self.inputPath + "/.metadata.json", "r") as f:
            metadata = load(f)
        return [InputPartition({"start":0, "end":1, "idx":idx}) for idx, m in enumerate(metadata)]

    def read(self, partition):
        partition_info = partition.value
        data = []
        with open(self.inputPath + f"/{partition_info["idx"]}.txt", "r") as f:
            data = load(f)
        for d in data:
            yield Row(**d)

class CustomDataSource(DataSource):
    @classmethod
    def name(cls): return "customconsole"
    
    def writer(self, schema: StructType, overwrite: bool) -> CustomWriter:
        return CustomWriter(self.options)
    
    def schema(self):
        return StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])

    def reader(self, schema):
        return CustomReader(self.options, schema)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomWriterExample").getOrCreate()
    spark.dataSource.register(CustomDataSource) # Register custom source

    data = [("Alice", 25), ("Bob", 30)]
    df = spark.createDataFrame(data, ["name", "age"]) #

    df.write.format("customconsole").mode("append").option("prefix", "LOG:").option("outputPath", "/Volumes/workspace/testing_schema/testing_volumne/t1").save()
