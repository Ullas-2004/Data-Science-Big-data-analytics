import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object KsrtcMasterPipeline {

  final case class Config(
      hdfsRoot: String = "hdfs://localhost:9000/ksrtc",
      mongoUri: String = "mongodb://localhost:27017/",
      mongoDb: String = "ksrtc_db",
      sourceStop: Long = -1L,
      writeMongo: Boolean = true
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder
      .appName("KSRTC Master Pipeline")
      .master("local[*]")
      .config("spark.mongodb.write.connection.uri", config.mongoUri)
      .getOrCreate()

    import spark.implicits._

    val rawBase = s"${config.hdfsRoot}/raw"
    val processedBase = s"${config.hdfsRoot}/processed"
    val resultsBase = s"${config.hdfsRoot}/results"

    try {
      val stops = readCsv(spark, s"$rawBase/synthetic_bus_stops.csv")
        .dropDuplicates()
        .na.drop(Seq("stop_id", "stop_name"))
        .withColumn("stop_id", col("stop_id").cast("long"))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))

      val busRoutes = readCsv(spark, s"$rawBase/cleaned_bus.csv").dropDuplicates()
      val gps = readCsv(spark, s"$rawBase/gps.csv").dropDuplicates()
      val weather = readCsv(spark, s"$rawBase/Bangalore_Weather_Data.csv").dropDuplicates()
      val schedule = readCsv(spark, s"$rawBase/scheduling.csv").dropDuplicates()
      val capacity = readCsv(spark, s"$rawBase/synthetic_bus_capacity.csv").dropDuplicates()

      writeParquet(stops, s"$processedBase/stops")
      writeParquet(busRoutes, s"$processedBase/routes")
      writeParquet(gps, s"$processedBase/gps")
      writeParquet(weather, s"$processedBase/weather")
      writeParquet(schedule, s"$processedBase/schedule")
      writeParquet(capacity, s"$processedBase/capacity")

      val edgeRows = stops
        .select("stop_id")
        .withColumn("next_stop", lead("stop_id", 1).over(Window.orderBy("stop_id")))
        .na.drop(Seq("next_stop"))
        .select(col("stop_id").cast("long"), col("next_stop").cast("long"))

      writeParquet(edgeRows, s"$processedBase/graph_routes")

      val vertices = stops
        .select("stop_id", "stop_name")
        .rdd
        .map(row => (row.getAs[Long]("stop_id"), row.getAs[String]("stop_name")))

      val baseEdges = edgeRows.rdd.map { row =>
        Edge(
          row.getAs[Long]("stop_id"),
          row.getAs[Long]("next_stop"),
          1.0
        )
      }
      val edges = baseEdges.flatMap(e => Seq(e, Edge(e.dstId, e.srcId, e.attr)))

      val graph = Graph(vertices, edges)
      val sourceStop = chooseSourceStop(config.sourceStop, vertices)

      val pageRankDf = graph.pageRank(0.0001).vertices.toDF("stop_id", "pagerank")
      val shortestDf = ShortestPaths
        .run(graph, Seq(sourceStop))
        .vertices
        .map { case (stopId, dists) =>
          val distance = dists.get(sourceStop).map(_.toLong).getOrElse(Long.MaxValue)
          (stopId, distance)
        }
        .toDF("stop_id", "distance_from_source")
      val connectedDf = graph.connectedComponents().vertices.toDF("stop_id", "component_id")

      writeCsv(pageRankDf, s"$resultsBase/pagerank")
      writeCsv(shortestDf, s"$resultsBase/shortest_paths")
      writeCsv(connectedDf, s"$resultsBase/connected_components")

      if (config.writeMongo) {
        writeMongo(stops, config.mongoDb, "bus_stops")
        writeMongo(busRoutes, config.mongoDb, "routes")
        writeMongo(gps, config.mongoDb, "gps_data")
        writeMongo(weather, config.mongoDb, "weather_data")
        writeMongo(schedule, config.mongoDb, "schedule_data")
        writeMongo(capacity, config.mongoDb, "bus_capacity")

        val routeAnalysis = pageRankDf
          .join(connectedDf, Seq("stop_id"), "left")
          .join(shortestDf, Seq("stop_id"), "left")
        writeMongo(routeAnalysis, config.mongoDb, "route_analysis")
      }

      println("Master pipeline completed successfully.")
      println(s"Processed outputs: $processedBase")
      println(s"Graph results: $resultsBase")
    } finally {
      spark.stop()
    }
  }

  private def readCsv(spark: SparkSession, path: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true").csv(path)

  private def writeParquet(df: DataFrame, path: String): Unit =
    df.write.mode("overwrite").parquet(path)

  private def writeCsv(df: DataFrame, path: String): Unit =
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)

  private def chooseSourceStop(preferred: Long, vertices: org.apache.spark.rdd.RDD[(VertexId, String)]): Long =
    if (preferred > 0) preferred else vertices.map(_._1).min()

  private def writeMongo(df: DataFrame, database: String, collection: String): Unit = {
    df.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", database)
      .option("collection", collection)
      .save()
    println(s"Stored $database.$collection")
  }

  private def parseArgs(args: Array[String]): Config = {
    var cfg = Config()
    var i = 0

    while (i < args.length) {
      args(i) match {
        case "--hdfs-root" if i + 1 < args.length =>
          cfg = cfg.copy(hdfsRoot = args(i + 1))
          i += 2
        case "--mongo-uri" if i + 1 < args.length =>
          cfg = cfg.copy(mongoUri = args(i + 1))
          i += 2
        case "--mongo-db" if i + 1 < args.length =>
          cfg = cfg.copy(mongoDb = args(i + 1))
          i += 2
        case "--source-stop" if i + 1 < args.length =>
          cfg = cfg.copy(sourceStop = args(i + 1).toLong)
          i += 2
        case "--skip-mongo" =>
          cfg = cfg.copy(writeMongo = false)
          i += 1
        case unknown =>
          throw new IllegalArgumentException(s"Unknown argument: $unknown")
      }
    }

    cfg
  }
}
