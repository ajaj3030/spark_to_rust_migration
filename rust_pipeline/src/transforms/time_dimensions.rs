use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike, Weekday};

pub struct TimeDimensions {
    pub event_date: i32, // days since 1970-01-01 for Arrow Date32
    pub event_year: i32,
    pub event_month: i32,
    pub event_day: i32,
    pub event_hour: i32,
    pub event_dayofweek: i32, // Spark convention: 1=Sun, 7=Sat
    pub is_weekend: bool,
    pub event_quarter: i32,
    pub date_str: String, // "yyyy-MM-dd"
}

static EPOCH: once_cell::sync::Lazy<NaiveDate> =
    once_cell::sync::Lazy::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

/// Spark parity: add_time_dimensions
/// Parses ISO-8601 timestamp and extracts temporal features.
pub fn apply(timestamp: &str) -> TimeDimensions {
    let dt = parse_timestamp(timestamp);
    let date = dt.date();
    let dow = spark_dayofweek(date.weekday());

    TimeDimensions {
        event_date: (date - *EPOCH).num_days() as i32,
        event_year: date.year(),
        event_month: date.month() as i32,
        event_day: date.day() as i32,
        event_hour: dt.hour() as i32,
        event_dayofweek: dow,
        is_weekend: dow == 1 || dow == 7,
        event_quarter: ((date.month() - 1) / 3 + 1) as i32,
        date_str: date.format("%Y-%m-%d").to_string(),
    }
}

fn parse_timestamp(ts: &str) -> NaiveDateTime {
    // Try multiple ISO-8601 formats
    if let Ok(dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.fZ") {
        return dt;
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%SZ") {
        return dt;
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.f") {
        return dt;
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S") {
        return dt;
    }
    // Handle timezone offset formats like +00:00, -05:00 etc.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return dt.naive_utc();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.f%:z") {
        return dt;
    }
    // Fallback: epoch
    chrono::DateTime::UNIX_EPOCH.naive_utc()
}

/// Convert chrono weekday to Spark's convention: 1=Sun, 2=Mon, ..., 7=Sat
fn spark_dayofweek(wd: Weekday) -> i32 {
    match wd {
        Weekday::Sun => 1,
        Weekday::Mon => 2,
        Weekday::Tue => 3,
        Weekday::Wed => 4,
        Weekday::Thu => 5,
        Weekday::Fri => 6,
        Weekday::Sat => 7,
    }
}
