use chrono::DateTime;
use chrono::FixedOffset;
use chrono::Local;
use chrono::Offset;
use chrono::Utc;

use crate::time_parse::format_rfc3339;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampPair {
    pub local: String,
    pub utc: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

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
