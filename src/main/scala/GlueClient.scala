import com.amazonaws.regions.Regions
import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}

import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter}

object GlueClient {

  /**
    *
    * @param s3Prefix : the S3 base folder path of the data
    * @param region
    * @param dbName
    * @param tableName
    * @param partitionCols : Must be of type strings
    * @param dataCols : is the hive schema obtained by say "describe table"
    *               or df.schema.fields.map(f => (f.name, f.dataType.typeName))
    */
  @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
  def createTable(
    s3Prefix: String,
    region: Regions,
    dbName: String,
    tableName: String,
    partitionCols: Seq[String],
    dataCols: Seq[(String, String)]
  ): Unit = {
    require(s3Prefix.startsWith("s3://"), "S3 prefix doesn't start with `s3://`")
    require(partitionCols.nonEmpty, "partitionCols empty")
    val glue = glueClient(region)

    val request =
      new CreateTableRequest()
        .withDatabaseName(dbName)
        .withTableInput(tableInput(s3Prefix, tableName, partitionCols, dataCols))
    println(request)

    val response = glue.createTable(request)
    println(response)
  }

  def addPartitions(
    s3Prefix: String,
    region: Regions,
    dbName: String,
    tableName: String,
    partitionCols: Seq[String],
    partitionValues: Seq[Seq[String]]
  ): Unit = {
    validate(s3Prefix, partitionCols, partitionValues)

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

  def deletePartitions(
    region: Regions,
    dbName: String,
    tableName: String,
    partitionValues: Seq[Seq[String]]
  ): Unit = {

    val glue = glueClient(region)

    partitionValues
      .grouped(maxPartitionsPerRequest)
      .foreach { values =>
        val request = new BatchDeletePartitionRequest()
          .withDatabaseName(dbName)
          .withTableName(tableName)
          .withPartitionsToDelete(partitionValueLists(values).asJava)

        println(s"Request : $request")
        val response = glue.batchDeletePartition(request)
        println(s"Response : $response")
      }
  }

  def validate(
    s3Prefix: String,
    partitionCols: Seq[String],
    partitionValues: Seq[Seq[String]]
  ): Unit = {
    require(s3Prefix.startsWith("s3://"), "S3 prefix doesn't start with `s3://`")
    require(partitionCols.nonEmpty, "partitionCols empty")
    require(
      partitionValues.forall(v => v.length == partitionCols.length),
      "Inequal partitions columns names and values"
    )
  }

  val maxPartitionsPerRequest: Int = 99

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

  @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
  def tableInput(
    s3Prefix: String,
    tableName: String,
    partitionCols: Seq[String],
    dataCols: Seq[(String, String)]
  ): TableInput =
    new TableInput()
      .withName(tableName)
      .withTableType("EXTERNAL_TABLE")
      .withStorageDescriptor(storageDescriptor(s3Prefix, dataCols))
      .withPartitionKeys(partitionKeys(partitionCols).asJava)

  def partitionKeys(partitionCols: Seq[String]): Seq[Column] =
    columns(partitionCols.map(it => (it, "string")))

  @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
  def storageDescriptor(s3Location: String, dataCols: Seq[(String, String)]): StorageDescriptor =
    storageDescriptor(s3Location).withColumns(columns(dataCols).asJava)

  @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
  def columns(cols: Seq[(String, String)]): Seq[Column] =
    cols.map(it => new Column().withName(it._1).withType(it._2))

  def partitionValueLists(partitionValues: Seq[Seq[String]]): Seq[PartitionValueList] =
    partitionValues.map(it => {
      val list = new PartitionValueList()
      list.withValues(it.asJava)
    })

  def storageDescriptor(s3Location: String): StorageDescriptor =
    new StorageDescriptor()
      .withLocation(s3Location)
      .withInputFormat(parquetInputFormat)
      .withOutputFormat(parquetOutputFormat)
      .withSerdeInfo(serDeInfo)
      .withParameters(storageParams.asJava)

  val parquetOutputFormat: String =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

  val parquetInputFormat: String =
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"

  val storageParams: Map[String, String] =
    Map("classification" -> "parquet", "typeOfData" -> "file")

  val serializer: String = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  val serDeInfo: SerDeInfo = {
    new SerDeInfo()
      .withName("SERDE")
      .withSerializationLibrary(serializer)
      .withParameters(Map("serialization.format" -> "1").asJava)
  }
}
