/// Round like Spark's `round(col, scale)` on a DoubleType column.
///
/// Spark converts the double to a decimal via its shortest string form
/// (`BigDecimal(d)`) and rounds HALF_UP, so `round(1.005, 2) == 1.01`.
/// Naive `(x * 100).round() / 100` gives 1.0 because 1.005 * 100 is
/// 100.49999…; this matches Spark instead.
pub fn spark_round(x: f64, scale: u32) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let repr = format!("{}", x.abs());
    let (int_part, frac_part) = repr.split_once('.').unwrap_or((repr.as_str(), ""));
    let scale = scale as usize;
    if frac_part.len() <= scale {
        return x;
    }
    let kept_digits = format!("{}{}", int_part, &frac_part[..scale]);
    let mut scaled: u128 = kept_digits.parse().expect("digits from f64 Display");
    if frac_part.as_bytes()[scale] >= b'5' {
        scaled += 1;
    }
    let rounded = scaled as f64 / 10f64.powi(scale as i32);
    if x.is_sign_negative() { -rounded } else { rounded }
}

#[cfg(test)]
mod tests {
    use super::spark_round;

    #[test]
    fn rounds_half_up_on_decimal_value_like_spark() {
        assert_eq!(spark_round(1.005, 2), 1.01);
        assert_eq!(spark_round(2.675, 2), 2.68);
        assert_eq!(spark_round(-1.005, 2), -1.01);
    }

    #[test]
    fn leaves_values_with_fewer_decimals_untouched() {
        assert_eq!(spark_round(37.5, 2), 37.5);
        assert_eq!(spark_round(100.0, 3), 100.0);
    }

    #[test]
    fn rounds_coordinates_to_two_places() {
        assert_eq!(spark_round(37.548512, 2), 37.55);
        assert_eq!(spark_round(-121.988649, 2), -121.99);
    }
}
