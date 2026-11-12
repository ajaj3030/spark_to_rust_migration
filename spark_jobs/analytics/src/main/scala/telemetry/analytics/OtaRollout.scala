package telemetry.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/** OTA rollout status per target software version and model. */
object OtaRollout extends AnalyticsJob {
  val name = "ota_rollout"

  val UpdateStates: Seq[String] = Seq(
    "UPDATE_STATE_IN_PROGRESS",
    "UPDATE_STATE_WAITING_ON_BCM",
    "UPDATE_STATE_SUCCESS",
    "UPDATE_SUCCESS_WITH_WARNINGS",
    "UPDATE_STATE_FAILED",
    "UPDATE_FAILED_DRIVE_ALLOWED",
    "UPDATE_FAILED_NO_ACTION"
  )
  private val Succeeded = Seq("UPDATE_STATE_SUCCESS", "UPDATE_SUCCESS_WITH_WARNINGS")
  private val Failed = Seq("UPDATE_STATE_FAILED", "UPDATE_FAILED_DRIVE_ALLOWED", "UPDATE_FAILED_NO_ACTION")

  private def total(states: Seq[String]): Column = states.map(col).foldLeft(lit(0))(_ + _)
  private def pct(numerator: Column, denominator: Column): Column = round(numerator / denominator * 100, 2)

  def summarize(telemetry: DataFrame): DataFrame = {
    val newestFirst = Window
      .partitionBy("vin_hash", "target_software_version")
      .orderBy(col("sequence_number").desc)

    val latestPerVehicle = telemetry
      .filter(col("event_type") === "OTA")
      .withColumn("rn", row_number().over(newestFirst))
      .filter(col("rn") === 1)

    latestPerVehicle
      .groupBy("target_software_version", "model_safe")
      .pivot("update_state", UpdateStates)
      .count()
      .na.fill(0L, UpdateStates)
      .withColumn("vehicles", total(UpdateStates))
      .withColumn("success_rate_pct", pct(total(Succeeded), col("vehicles")))
      .withColumn("failure_rate_pct", pct(total(Failed), col("vehicles")))
  }

  def build(spark: SparkSession, paths: Paths): DataFrame =
    summarize(spark.read.format("delta").load(paths.telemetry))
}
