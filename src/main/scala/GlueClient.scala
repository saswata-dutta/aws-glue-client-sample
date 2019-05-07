import java.util.Locale

import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}

import scala.util.matching.Regex

object GlueClient {

  val S3_BUCKET: String = "rivigo-data-lake"
  val S3_ROOT: String = "feature_store"

//  val response =
//    GlueClient.addPartitions(
//      "zoom",
//      "qc",
//      "consignment",
//      "v0000",
//      Seq(
//        "y=2018/m=08/d=04",
//        "y=2018/m=09/d=03",
//        "y=2018/m=09/d=02",
//        "y=2018/m=09/d=01"
//      )
//    )

  def addPartitions(client: String,
                    app: String,
                    entity: String,
                    version: String,
                    timeSuffixes: Seq[String]): BatchCreatePartitionResult = {

    val db = S3_ROOT
    val table = tableName(client, app, entity, version)
    val s3Prefix = s3PrefixPath(client, app, entity, version)
    batchCreatePartition(db, table, s3Prefix, timeSuffixes)
  }

  def s3PrefixPath(client: String,
                   app: String,
                   entity: String,
                   version: String): String = {
    s"s3://$S3_BUCKET/$S3_ROOT/$client/$app/$entity/data/$version"
  }

  val NON_WORD_RE: Regex = """\W""".r

  def sanitise(str: String): String = {
    val replaced = NON_WORD_RE.replaceAllIn(str.trim.toLowerCase(Locale.ENGLISH), "_")
    replaced.replaceAll("_{2,}", "_").stripPrefix("_").stripSuffix("_")
  }

  def tableName(client: String,
                app: String,
                entity: String,
                version: String): String = {
    Seq(client, app, entity, version).map(sanitise).mkString("_")
  }

  val glueClient: AWSGlue = {
    AWSGlueClient.builder().withRegion("ap-south-1").build()
  }

  def batchCreatePartition(
    dbName: String,
    tableName: String,
    s3pathPrefix: String,
    timeStrings: Seq[String]
  ): BatchCreatePartitionResult = {
    val request =
      new BatchCreatePartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tableName)
        .withPartitionInputList(partitionInputs(s3pathPrefix, timeStrings))

    glueClient.batchCreatePartition(request)
  }

  def partitionInputs(
    s3pathPrefix: String,
    timeStrings: Seq[String]
  ): java.util.List[PartitionInput] = {
    import scala.collection.JavaConverters.seqAsJavaListConverter
    val partitions = timeStrings.map(t => partitionInput(s3pathPrefix, t))
    //    scala.collection.JavaConverters.seqAsJavaList(partitions) // for 2.12
    partitions.asJava // scala 2.11
  }

  def partitionInput(s3pathPrefix: String,
                     timeString: String): PartitionInput = {
    val (y, m, d) = partitionValues(timeString)
    val s3Loc = s"$s3pathPrefix/$timeString/"
    new PartitionInput()
      .withValues(y, m, d)
      .withStorageDescriptor(storageDescriptor(s3Loc))
  }

  val YYYY_MM_DD: Regex = """^y=(\d{4})/m=(\d{2})/d=(\d{2})$""".r
  def partitionValues(timeString: String): (String, String, String) = {
    timeString match {
      case YYYY_MM_DD(y, m, d) => (y, m, d)
    }
  }

  def storageDescriptor(s3Location: String): StorageDescriptor = {
    new StorageDescriptor()
      .withLocation(s3Location)
      .withInputFormat(parquetInputFormat)
      .withOutputFormat(parquetOutputFormat)
      .withSerdeInfo(serDeInfo)
      .withParameters(storageParams)
  }

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
