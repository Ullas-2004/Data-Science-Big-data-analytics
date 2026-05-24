import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession

object StoreMongo {

  def main(args: Array[String]): Unit = {
    val mongoUri = if (args.length > 0) args(0) else "mongodb://localhost:27017/"
    val database = if (args.length > 1) args(1) else "ksrtc_db"
    val predictionsPath = if (args.length > 2) args(2) else "results/csv/predictions.csv"
    val pagerankPath = if (args.length > 3) args(3) else "results/csv/pagerank.csv"

    val spark = SparkSession.builder()
      .appName("KSRTC MongoDB Storage")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", mongoUri)
      .getOrCreate()

    try {
      if (Files.exists(Paths.get(predictionsPath))) {
        val predictions = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(predictionsPath)

        predictions.write
          .format("mongodb")
          .mode("append")
          .option("database", database)
          .option("collection", "route_predictions")
          .save()

        println(s"Stored predictions in $database.route_predictions")
      } else {
        println(s"Predictions file not found: $predictionsPath")
      }

      if (Files.exists(Paths.get(pagerankPath))) {
        val pagerankDF = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(pagerankPath)
          .toDF("stop_id", "importance_score")

        pagerankDF.write
          .format("mongodb")
          .mode("append")
          .option("database", database)
          .option("collection", "route_analysis")
          .save()

        println(s"Stored graph results in $database.route_analysis")
      } else {
        println(s"PageRank file not found: $pagerankPath")
      }
    } finally {
      spark.stop()
    }
  }
}
