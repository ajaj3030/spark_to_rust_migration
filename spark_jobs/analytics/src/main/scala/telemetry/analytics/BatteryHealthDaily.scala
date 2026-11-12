package telemetry.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Battery health per vehicle per day, for warranty and service. */
object BatteryHealthDaily extends AnalyticsJob {
  val name = "battery_health_daily"
  override val partitionCols: Seq[String] = Seq("event_date")

  private val SecondsPerDay = 86400L

  def summarize(telemetry: DataFrame): DataFrame = {
    val daily = telemetry
      .filter(col("event_type") === "BATTERY")
      .groupBy("vin_hash", "event_date", "model_safe")
      .agg(
        count("*").as("readings"),
        round(avg("charge_percent"), 2).as("avg_charge_percent"),
        min("charge_percent").as("min_charge_percent"),
        max("max_cell_temp").as("peak_cell_temp"),
        round(avg("cell_temp_spread"), 3).as("avg_cell_temp_spread"),
        sum(col("thermal_alert").cast("int")).as("thermal_alert_count"),
        max_by(col("battery_health_level"), col("sequence_number")).as("health_level_end_of_day")
      )

    val daySeconds = col("event_date").cast("timestamp").cast("long")
    val lastThreeDays = Window
      .partitionBy("vin_hash")
      .orderBy(daySeconds)
      .rangeBetween(-2 * SecondsPerDay, 0)
    val byDay = Window.partitionBy("vin_hash").orderBy("event_date")

    daily
      .withColumn("peak_cell_temp_3d", max("peak_cell_temp").over(lastThreeDays))
      .withColumn(
        "health_change",
        round(col("health_level_end_of_day") - lag("health_level_end_of_day", 1).over(byDay), 2)
      )
  }

  def build(spark: SparkSession, paths: Paths): DataFrame =
    summarize(spark.read.format("delta").load(paths.telemetry))
}
