import com.amazonaws.regions.Regions
import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}

import scala.collection.JavaConverters.seqAsJavaListConverter

object GlueClient {

  val maxPartitionsPerRequest: Int = 99

  def addPartitions(
    s3Prefix: String,
    region: Regions,
    dbName: String,
    tableName: String,
    partitionCols: Seq[String],
    partitionValues: Seq[Seq[String]]
  ): Unit = {
    require(s3Prefix.startsWith("s3://"), "S3 prefix doesn't start with `s3://`")
    require(partitionCols.nonEmpty, "partitionCols empty")
    require(
      partitionValues.forall(v => v.length == partitionCols.length),
      "Inequal partitions columns names and values"
    )

    val glue = glueClient(region)

    partitionValues
      .grouped(maxPartitionsPerRequest)
      .foreach { values =>
        val request = new BatchCreatePartitionRequest()
          .withDatabaseName(dbName)
          .withTableName(tableName)
          .withPartitionInputList(partitionInputs(s3Prefix, partitionCols, values).asJava)

        println(s"Request : $request")
        val response = glue.batchCreatePartition(request)
        println(s"Response : $response")
      }
  }

  def glueClient(region: Regions): AWSGlue =
    AWSGlueClient.builder().withRegion(region).build()

  def partitionInputs(
    s3Prefix: String,
    partitionCols: Seq[String],
    partitionValues: Seq[Seq[String]]
  ): Seq[PartitionInput] =
    partitionValues.map(v => partitionInput(s3Prefix, partitionCols, v))

  def partitionInput(s3Prefix: String, keys: Seq[String], values: Seq[String]): PartitionInput = {
    val path = s3PartitionPath(s3Prefix, keys, values)

    new PartitionInput()
      .withValues(values.asJava)
      .withStorageDescriptor(storageDescriptor(path))
  }

  def s3PartitionPath(prefix: String, keys: Seq[String], values: Seq[String]): String = {
    val suffix = keys.zip(values).map { case (k, v) => s"$k=$v" }.mkString("/")
    s"$prefix/$suffix/"
  }

  def storageDescriptor(s3Location: String): StorageDescriptor =
    new StorageDescriptor()
      .withLocation(s3Location)
      .withInputFormat(parquetInputFormat)
      .withOutputFormat(parquetOutputFormat)
      .withSerdeInfo(serDeInfo)
      .withParameters(storageParams)

  val parquetOutputFormat: String =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

  val parquetInputFormat: String =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"

  val storageParams: java.util.Map[String, String] = {
    val params = new java.util.HashMap[String, String]()
    params.put("classification", "parquet")
    params.put("typeOfData", "file")
    params
  }

  val serializer = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  val serDeInfo: SerDeInfo = {
    new SerDeInfo()
      .withName("SERDE")
      .withSerializationLibrary(serializer)
      .withParameters(
        java.util.Collections
          .singletonMap[String, String]("serialization.format", "1")
      )
  }
}
