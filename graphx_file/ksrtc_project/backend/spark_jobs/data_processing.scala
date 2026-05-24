import org.apache.spark.sql.SparkSession

object DataProcessing {

  def main(args: Array[String]): Unit = {
    val inputPath = if (args.length > 0) args(0) else "data/ksrtc_data.csv"
    val outputPath = if (args.length > 1) args(1) else "data/processed/cleaned_data"

    val spark = SparkSession.builder
      .appName("KSRTC Data Processing")
      .master("local[*]")
      .getOrCreate()

    try {
      val data = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputPath)

      val cleaned = data.dropDuplicates()

      cleaned.write
        .mode("overwrite")
        .parquet(outputPath)

      println(s"Cleaned dataset written to: $outputPath")
      println(s"Input rows: ${data.count()}, Output rows: ${cleaned.count()}")
    } finally {
      spark.stop()
    }
  }
}
