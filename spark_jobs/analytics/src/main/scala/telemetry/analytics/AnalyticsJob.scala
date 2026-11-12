package telemetry.analytics

import java.io.FileInputStream

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml

/** Command-line options shared by every job. */
final case class Cli(
    config: String = "config/app_config.yaml",
    local: Boolean = false,
    out: String = "./rust_pipeline/tests/fixtures",
    vehicles: Int = 150,
    expectedOnly: Boolean = false
)

object Cli {
  @annotation.tailrec
  def parse(args: List[String], acc: Cli = Cli()): Cli = args match {
    case "--config" :: path :: rest  => parse(rest, acc.copy(config = path))
    case "--local" :: rest           => parse(rest, acc.copy(local = true))
    case "--out" :: path :: rest     => parse(rest, acc.copy(out = path))
    case "--vehicles" :: n :: rest   => parse(rest, acc.copy(vehicles = n.toInt))
    case "--expected-only" :: rest   => parse(rest, acc.copy(expectedOnly = true))
    case Nil                         => acc
    case unknown :: _                => throw new IllegalArgumentException(s"Unknown argument: $unknown")
  }
}

/** config/app_config.yaml, read as section -> key -> value. */
final case class Config(raw: java.util.Map[String, Object]) {
  def get(section: String, key: String): String =
    raw.get(section).asInstanceOf[java.util.Map[String, Object]].get(key).toString
}

object Config {
  def load(path: String): Config = {
    val in = new FileInputStream(path)
    try Config(new Yaml().load[java.util.Map[String, Object]](in))
    finally in.close()
  }
}

/** Table locations: container paths on the cluster, ./ paths with --local. */
final case class Paths(cfg: Config, local: Boolean) {
  private def pick(section: String, localKey: String, clusterKey: String): String =
    cfg.get(section, if (local) localKey else clusterKey)

  def telemetry: String = pick("spark", "local_delta_output", "delta_output_path")
  def vehicleState: String = pick("vehicle_state", "local_delta_output", "delta_output_path")
  def output(job: String): String =
    pick("analytics", "local_output_base", "output_base").stripSuffix("/") + "/" + job
}

object Spark {
  def session(cfg: Config, jobName: String, local: Boolean): SparkSession = {
    val spark = SparkSession.builder
      .appName(s"${cfg.get("spark", "app_name")}_$jobName")
      .master(if (local) "local[*]" else cfg.get("spark", "master"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", cfg.get("spark", "shuffle_partitions"))
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}

/**
 * A batch analytics job: reads the enriched telemetry (and possibly the
 * vehicle_state table), computes one summary, and overwrites one Delta
 * table under analytics.output_base.
 */
trait AnalyticsJob {
  def name: String
  def partitionCols: Seq[String] = Nil
  def build(spark: SparkSession, paths: Paths): DataFrame

  def main(args: Array[String]): Unit = {
    val cli = Cli.parse(args.toList)
    val cfg = Config.load(cli.config)
    val paths = Paths(cfg, cli.local)
    val spark = Spark.session(cfg, name, cli.local)

    val out = paths.output(name)
    val writer = build(spark, paths).write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
    (if (partitionCols.isEmpty) writer else writer.partitionBy(partitionCols: _*)).save(out)

    val rows = spark.read.format("delta").load(out).count()
    println(f"[$name] wrote $rows%,d rows to $out")
    spark.stop()
  }
}
