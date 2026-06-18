use std::time::SystemTime;

use chrono::DateTime;
use chrono::Datelike;
use chrono::FixedOffset;
use chrono::Local;
use chrono::Offset;
use chrono::SecondsFormat;
use chrono::TimeDelta;
use chrono::Utc;
use humantime::parse_duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampPair {
    pub local: String,
    pub utc: String,
}

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

pub fn now_timestamp_pair() -> TimestampPair {
    let utc = Utc::now();
    let local_offset = Local::now().offset().fix();
    timestamp_pair_at(utc, local_offset)
}

pub fn now_rfc3339() -> String {
    format_rfc3339(Utc::now()).expect("UTC timestamp formats as RFC3339")
}

pub fn timestamp_pair_at(utc: DateTime<Utc>, local_offset: FixedOffset) -> TimestampPair {
    let local = format_rfc3339(utc.with_timezone(&local_offset))
        .expect("local timestamp formats as RFC3339");
    let utc = format_rfc3339(utc).expect("UTC timestamp formats as RFC3339");
    TimestampPair { local, utc }
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
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        let seconds = i64::try_from(value.parse::<u64>().ok()?).ok()?;
        let now = DateTime::from_timestamp_millis(i64::try_from(now_ms).ok()?)?;
        let duration = TimeDelta::try_seconds(seconds)?;
        return Some(timestamp_ms(now.checked_add_signed(duration)?));
    }
    // Retry-After uses HTTP-date, including obsolete RFC 850 and asctime forms.
    let deadline = httpdate::parse_http_date(value).ok()?;
    Some(system_time_unix_ms(deadline))
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
    use chrono::TimeZone;

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

    #[test]
    fn should_format_local_offset_and_utc_timestamps_from_same_instant() {
        let utc = DateTime::parse_from_rfc3339("2026-05-27T12:00:00Z")
            .unwrap()
            .to_utc();
        let offset = FixedOffset::west_opt(7 * 60 * 60).unwrap();

        let pair = timestamp_pair_at(utc, offset);

        assert_eq!(pair.local, "2026-05-27T05:00:00-07:00");
        assert_eq!(pair.utc, "2026-05-27T12:00:00Z");
    }

    #[test]
    fn should_format_current_timestamp_pair_as_rfc3339() {
        let pair = now_timestamp_pair();

        DateTime::parse_from_rfc3339(&pair.local).expect("local timestamp parses as RFC3339");
        DateTime::parse_from_rfc3339(&pair.utc).expect("UTC timestamp parses as RFC3339");
    }

    #[test]
    fn should_format_current_utc_timestamp_as_rfc3339() {
        let timestamp = now_rfc3339();

        DateTime::parse_from_rfc3339(&timestamp).expect("UTC timestamp parses as RFC3339");
    }

    #[test]
    fn should_format_utc_local_offset_as_z() {
        let utc = Utc.with_ymd_and_hms(2026, 5, 27, 12, 0, 0).unwrap();
        let offset = FixedOffset::east_opt(0).unwrap();

        let pair = timestamp_pair_at(utc, offset);

        assert_eq!(pair.local, "2026-05-27T12:00:00Z");
        assert_eq!(pair.utc, "2026-05-27T12:00:00Z");
    }
}
