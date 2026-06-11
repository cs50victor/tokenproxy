use std::time::SystemTime;

use chrono::DateTime;
use chrono::Datelike;
use chrono::FixedOffset;
use chrono::SecondsFormat;
use chrono::TimeDelta;
use chrono::Utc;
use humantime::parse_duration;

pub fn parse_rfc3339(value: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value).ok()
}

pub fn normalize_rfc3339(value: &str) -> Option<String> {
    format_rfc3339(parse_rfc3339(value)?)
}

pub fn format_rfc3339<Tz: chrono::TimeZone>(value: DateTime<Tz>) -> Option<String> {
    let year = value.year();
    if !(0..=9999).contains(&year) {
        return None;
    }
    Some(value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

pub fn rfc3339_after_duration(observed_at: &str, duration: &str) -> Option<String> {
    let observed_at = parse_rfc3339(observed_at)?;
    let duration = TimeDelta::from_std(parse_duration(duration).ok()?).ok()?;
    format_rfc3339(observed_at.checked_add_signed(duration)?)
}

pub fn rfc3339_after_seconds(observed_at: &str, seconds: f64) -> Option<String> {
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    let observed_at = parse_rfc3339(observed_at)?;
    let duration = std::time::Duration::try_from_secs_f64(seconds).ok()?;
    let duration = TimeDelta::from_std(duration).ok()?;
    format_rfc3339(observed_at.checked_add_signed(duration)?)
}

pub fn retry_after_deadline_ms(value: &str, now_ms: u64) -> Option<u64> {
    let value = value.trim();
    if let Some(seconds) = parse_retry_after_delta_seconds(value) {
        let now = DateTime::from_timestamp_millis(i64::try_from(now_ms).ok()?)?;
        let duration = TimeDelta::try_seconds(seconds)?;
        return Some(timestamp_ms(now.checked_add_signed(duration)?));
    }
    // Retry-After uses HTTP-date, including obsolete RFC 850 and asctime forms.
    let deadline = httpdate::parse_http_date(value).ok()?;
    Some(system_time_unix_ms(deadline))
}

fn parse_retry_after_delta_seconds(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let seconds = value.parse::<u64>().ok()?;
    i64::try_from(seconds).ok()
}

pub fn unix_ms_from_rfc3339(value: &str) -> Option<u64> {
    parse_rfc3339(value).and_then(|value| u64::try_from(value.timestamp_millis()).ok())
}

pub fn rfc3339_from_unix_ms(value: u64) -> Option<String> {
    let value = i64::try_from(value).ok()?;
    let timestamp = DateTime::from_timestamp_millis(value)?;
    format_rfc3339(timestamp)
}

pub fn unix_seconds_from_rfc3339(value: &str) -> Option<i64> {
    parse_rfc3339(value)
        .map(|value| value.timestamp())
        .filter(|value| *value >= 0)
}

pub fn timestamp_ms<Tz: chrono::TimeZone>(value: DateTime<Tz>) -> u64 {
    u64::try_from(value.timestamp_millis()).unwrap_or(0)
}

pub fn system_time_unix_ms(value: SystemTime) -> u64 {
    timestamp_ms(DateTime::<Utc>::from(value))
}

pub fn now_unix_ms() -> u64 {
    timestamp_ms(Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_rfc3339_with_chrono_datetime_type() {
        let parsed: chrono::DateTime<chrono::FixedOffset> =
            parse_rfc3339("2026-05-27T11:24:18.123-07:00").unwrap();

        assert_eq!(
            parsed.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "2026-05-27T11:24:18.123-07:00"
        );
    }

    #[test]
    fn should_normalize_rfc3339_with_chrono_datetime_type() {
        assert_eq!(
            normalize_rfc3339("2026-05-27T15:07:00+00:00").as_deref(),
            Some("2026-05-27T15:07:00Z")
        );
        assert_eq!(
            normalize_rfc3339("2026-05-27T11:24:18.120-07:00").as_deref(),
            Some("2026-05-27T11:24:18.120-07:00")
        );
        assert!(normalize_rfc3339("not-a-timestamp").is_none());
    }

    #[test]
    fn should_parse_openai_reset_duration_without_manual_units() {
        let reset_at = rfc3339_after_duration("2026-05-27T11:24:18-07:00", "1.5s");

        assert_eq!(reset_at.as_deref(), Some("2026-05-27T11:24:19.500-07:00"));
    }

    #[test]
    fn should_parse_usage_limit_seconds_without_manual_timestamp_math() {
        let reset_at = rfc3339_after_seconds("2026-05-27T11:24:18Z", 60.5);

        assert_eq!(reset_at.as_deref(), Some("2026-05-27T11:25:18.500Z"));
    }

    #[test]
    fn should_reject_invalid_usage_limit_seconds() {
        assert!(rfc3339_after_seconds("2026-05-27T11:24:18Z", -1.0).is_none());
        assert!(rfc3339_after_seconds("2026-05-27T11:24:18Z", f64::NAN).is_none());
    }

    #[test]
    fn should_reject_reset_times_that_overflow_timestamp_range() {
        assert!(rfc3339_after_seconds("9999-12-31T23:59:59Z", 2.0).is_none());
        assert!(rfc3339_after_duration("9999-12-31T23:59:59Z", "2s").is_none());
    }

    #[test]
    fn should_format_unix_milliseconds_as_rfc3339_utc() {
        let timestamp_ms = timestamp_ms(parse_rfc3339("2026-05-27T11:25:18Z").unwrap());

        assert_eq!(
            rfc3339_from_unix_ms(timestamp_ms).as_deref(),
            Some("2026-05-27T11:25:18Z")
        );
        assert!(rfc3339_from_unix_ms(253_402_300_800_000).is_none());
        assert!(rfc3339_from_unix_ms(u64::MAX).is_none());
    }

    #[test]
    fn should_reject_pre_epoch_rfc3339_unix_conversions() {
        assert!(unix_ms_from_rfc3339("1969-12-31T23:59:59Z").is_none());
        assert!(unix_seconds_from_rfc3339("1969-12-31T23:59:59Z").is_none());
    }

    #[test]
    fn should_parse_retry_after_delta_and_http_date_formats() {
        assert_eq!(retry_after_deadline_ms("12", 5_000), Some(17_000));
        assert_eq!(
            retry_after_deadline_ms("Thu, 01 Jan 1970 00:00:10 GMT", 5_000),
            Some(10_000)
        );
        assert_eq!(
            retry_after_deadline_ms("Thursday, 01-Jan-70 00:00:11 GMT", 5_000),
            Some(11_000)
        );
        assert_eq!(
            retry_after_deadline_ms("Thu Jan  1 00:00:12 1970", 5_000),
            Some(12_000)
        );
    }

    #[test]
    fn should_reject_retry_after_delta_that_overflows_timestamp_range() {
        assert!(retry_after_deadline_ms("9223372036854775808", 5_000).is_none());
    }

    #[test]
    fn should_reject_signed_retry_after_delta_seconds() {
        assert!(retry_after_deadline_ms("+12", 5_000).is_none());
        assert!(retry_after_deadline_ms("-12", 5_000).is_none());
    }
}
