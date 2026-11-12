package telemetry.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Fleet impact of each diagnostic trouble code, for service triage. */
object DiagnosticAlerts extends AnalyticsJob {
  val name = "diagnostic_alerts"

  def summarize(telemetry: DataFrame, vehicleState: DataFrame): DataFrame = {
    // vehicle_state is keyed by raw VIN; hash it to join with telemetry
    val fleet = vehicleState.select(sha2(col("vin"), 256).as("vin_hash"), col("battery_pack_type"))
    val fleetSize = lit(vehicleState.count())

    val alerts = telemetry
      .filter(col("event_type") === "DIAGNOSTIC")
      // Diagnostic events carry no pack type; take it from the vehicle record
      .drop("battery_pack_type")
      .join(broadcast(fleet), Seq("vin_hash"), "left")
      .groupBy("dtc_code", "ecu", "model_safe", "battery_pack_type")
      .agg(
        count("*").as("events"),
        countDistinct("vin_hash").as("vehicles_affected"),
        sum(when(col("warning_state") === "WARNING_ON", 1).otherwise(0)).as("warning_on_events"),
        min(to_timestamp(col("event_time"))).as("first_seen"),
        max(to_timestamp(col("event_time"))).as("last_seen")
      )

    val byImpact = Window.partitionBy("ecu").orderBy(col("vehicles_affected").desc)
    alerts
      .withColumn("fleet_pct_affected", round(col("vehicles_affected") / fleetSize * 100, 3))
      .withColumn("rank_in_ecu", dense_rank().over(byImpact))
  }

  def build(spark: SparkSession, paths: Paths): DataFrame =
    summarize(
      spark.read.format("delta").load(paths.telemetry),
      spark.read.format("delta").load(paths.vehicleState)
    )
}
