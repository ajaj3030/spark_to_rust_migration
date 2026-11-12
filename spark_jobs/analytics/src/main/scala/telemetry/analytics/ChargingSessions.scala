package telemetry.analytics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** One row per charging session. */
object ChargingSessions extends AnalyticsJob {
  val name = "charging_sessions"
  override val partitionCols: Seq[String] = Seq("session_date")

  def summarize(telemetry: DataFrame): DataFrame = {
    val sessions = telemetry
      .filter(col("event_type") === "CHARGING" && col("charge_session_id").isNotNull)
      .groupBy("charge_session_id", "vin_hash", "model_safe")
      .agg(
        min(to_timestamp(col("event_time"))).as("session_start"),
        max(to_timestamp(col("event_time"))).as("session_end"),
        count("*").as("events"),
        round(sum("energy_added_kwh"), 3).as("energy_added_kwh"),
        max("charging_power_kw").as("peak_power_kw"),
        min_by(col("charge_percent"), col("sequence_number")).as("soc_start"),
        max_by(col("charge_percent"), col("sequence_number")).as("soc_end"),
        max_by(col("charge_state"), col("sequence_number")).as("end_state"),
        max(col("is_fault").cast("int")).cast("boolean").as("had_fault")
      )

    val durationMinutes =
      round((col("session_end").cast("double") - col("session_start").cast("double")) / 60, 2)

    sessions
      .withColumn("duration_minutes", durationMinutes)
      .withColumn(
        "avg_power_kw",
        when(col("duration_minutes") > 0,
          round(col("energy_added_kwh") / (col("duration_minutes") / 60), 2))
      )
      .withColumn("session_date", to_date(col("session_start")))
  }

  def build(spark: SparkSession, paths: Paths): DataFrame =
    summarize(spark.read.format("delta").load(paths.telemetry))
}
