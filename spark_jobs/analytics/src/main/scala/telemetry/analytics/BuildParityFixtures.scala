package telemetry.analytics

import java.io.File

import org.apache.spark.sql.functions.{col, sha2}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Builds the fixtures the Rust ports are tested against:
 *
 *   <out>/input/telemetry/       a fixed slice of the telemetry table
 *   <out>/input/vehicle_state/   the same vehicles' state rows
 *   <out>/expected/<job>/        each job's output on that input
 *
 * Input is the first --vehicles vehicles by vin_hash. With --expected-only
 * the existing input is kept and only the expected outputs are rebuilt.
 */
object BuildParityFixtures {
  val Jobs: Seq[(String, (DataFrame, DataFrame) => DataFrame)] = Seq(
    BatteryHealthDaily.name -> ((t, _) => BatteryHealthDaily.summarize(t)),
    ChargingSessions.name -> ((t, _) => ChargingSessions.summarize(t)),
    OtaRollout.name -> ((t, _) => OtaRollout.summarize(t)),
    DiagnosticAlerts.name -> ((t, s) => DiagnosticAlerts.summarize(t, s))
  )

  /** One Parquet file per fixture, without Hadoop's _SUCCESS and .crc files. */
  private def writeFixture(df: DataFrame, path: String): Unit = {
    df.coalesce(1).write.mode("overwrite").parquet(path)
    Option(new File(path).listFiles()).getOrElse(Array.empty[File])
      .filter(f => f.getName == "_SUCCESS" || f.getName.endsWith(".crc"))
      .foreach(_.delete())
  }

  private def sampleInput(spark: SparkSession, paths: Paths, out: String, vehicles: Int): Unit = {
    val telemetry = spark.read.format("delta").load(paths.telemetry)
    val state = spark.read.format("delta").load(paths.vehicleState)
    val sample = telemetry.select("vin_hash").distinct().orderBy("vin_hash").limit(vehicles)

    writeFixture(telemetry.join(sample, "vin_hash"), s"$out/input/telemetry")
    writeFixture(
      state.withColumn("vin_hash", sha2(col("vin"), 256)).join(sample, "vin_hash").drop("vin_hash"),
      s"$out/input/vehicle_state"
    )
  }

  def main(args: Array[String]): Unit = {
    val cli = Cli.parse(args.toList)
    val cfg = Config.load(cli.config)
    val spark = Spark.session(cfg, "ParityFixtures", cli.local)
    // Plain microsecond timestamps instead of legacy INT96, for portable Parquet
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

    if (!cli.expectedOnly) sampleInput(spark, Paths(cfg, cli.local), cli.out, cli.vehicles)

    // Read the fixtures back so expected outputs come from exactly what Rust reads
    val telemetry = spark.read.parquet(s"${cli.out}/input/telemetry")
    val state = spark.read.parquet(s"${cli.out}/input/vehicle_state")
    println(f"Input: ${telemetry.count()}%,d telemetry rows, ${state.count()}%,d vehicles")
    Jobs.foreach { case (job, summarize) =>
      val result = summarize(telemetry, state)
      writeFixture(result, s"${cli.out}/expected/$job")
      println(f"Expected $job: ${result.count()}%,d rows")
    }
    spark.stop()
  }
}
