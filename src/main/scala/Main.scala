import com.amazonaws.regions.Regions

object Main {

  def main(args: Array[String]): Unit = {
    val awsRegion: Regions = Regions.AP_SOUTHEAST_1
    val s3Prefix: String =
      "s3://rivigo-data-lake-sg/feature_store/prime/trip/sensors_30s/v0001/data"
    val dbName: String = "feature_store_sg"
    val tableName: String = "prime_trip_sensors_30s_v0001"
    val partitionCols: Seq[String] = Seq("y", "m", "d")
    val dataCols = Seq(
      ("epoch", "bigint"),
      ("latitude", "double"),
      ("longitude", "double"),
      ("speed", "double"),
      ("fuel", "double"),
      ("fuel_tank_one", "double"),
      ("fuel_tank_two", "double"),
      ("coolant", "double"),
      ("engine_oil_pressure", "double"),
      ("air_brake_pressure", "double"),
      ("rpm", "double"),
      ("battery_level", "double"),
      ("engine_voltage", "double"),
      ("x_axis", "double"),
      ("y_axis", "double"),
      ("z_axis", "double"),
      ("odometer", "double"),
      ("vehicle", "string"),
      ("engine_status", "string"),
      ("sensor_power_status", "string")
    )

    GlueClient.createTable(s3Prefix, awsRegion, dbName, tableName, partitionCols, dataCols)
  }
}
