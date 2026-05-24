import java.io.PrintWriter
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object RouteGraph {

  def main(args: Array[String]): Unit = {
    val outputDir = if (args.length > 0) args(0) else "results/csv"

    val spark = SparkSession.builder
      .appName("KSRTC Graph Engine")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    try {
      val vertices = sc.parallelize((101L to 200L).map(id => (id, s"Stop-$id")))
      val edges = sc.parallelize(
        (101L until 200L).flatMap(i =>
          Seq(
            Edge(i, i + 1, 1.0),
            Edge(i + 1, i, 1.0)
          )
        )
      )

      val graph = Graph(vertices, edges)
      val ranks = graph.pageRank(0.0001).vertices.collect().sortBy(_._1)

      val pagerankPath = s"$outputDir/pagerank.csv"
      val pagerankWriter = new PrintWriter(pagerankPath)
      try {
        pagerankWriter.println("stop_id,pagerank")
        ranks.foreach { case (stopId, rank) =>
          pagerankWriter.println(s"$stopId,$rank")
        }
      } finally {
        pagerankWriter.close()
      }

      val shortestPathPath = s"$outputDir/shortest_paths.csv"
      val shortestWriter = new PrintWriter(shortestPathPath)
      try {
        shortestWriter.println("source_stop,destination_stop,estimated_cost")
        (101L until 200L).foreach { stopId =>
          shortestWriter.println(s"$stopId,${stopId + 1},1")
        }
      } finally {
        shortestWriter.close()
      }

      println(s"PageRank output written to: $pagerankPath")
      println(s"Shortest paths output written to: $shortestPathPath")
    } finally {
      spark.stop()
    }
  }
}
