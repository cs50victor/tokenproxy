use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::time_parse::unix_seconds_from_rfc3339;
use crate::usage::UsageSnapshot;

const REQUEST_DURATION_BUCKETS_MS: &[u64] = &[5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];

#[derive(Debug, Clone)]
pub struct Metrics {
    requests_total: Arc<AtomicU64>,
    request_outcomes: Arc<Mutex<BTreeMap<RequestMetricKey, u64>>>,
    request_shapes: Arc<Mutex<BTreeMap<RequestShapeMetricKey, u64>>>,
    route_exclusions: Arc<Mutex<BTreeMap<RouteExclusionMetricKey, u64>>>,
    upstream_attempts_total: Arc<AtomicU64>,
    upstream_attempts: Arc<Mutex<BTreeMap<UpstreamAttemptMetricKey, u64>>>,
    active_websocket_sessions: Arc<AtomicU64>,
    websocket_events_total: Arc<AtomicU64>,
    websocket_event_outcomes: Arc<Mutex<BTreeMap<WebSocketEventMetricKey, u64>>>,
    sse_events_total: Arc<AtomicU64>,
    sse_event_outcomes: Arc<Mutex<BTreeMap<SseEventMetricKey, u64>>>,
    replay_items_total: Arc<AtomicU64>,
    replay_item_outcomes: Arc<Mutex<BTreeMap<ReplayItemMetricKey, u64>>>,
    cached_input_tokens: Arc<Mutex<BTreeMap<CachedInputTokensMetricKey, u64>>>,
    request_duration_buckets: Arc<Vec<AtomicU64>>,
    request_duration_count: Arc<AtomicU64>,
    request_duration_sum_ms: Arc<AtomicU64>,
    upstream_connect_duration_buckets: Arc<Vec<AtomicU64>>,
    upstream_connect_duration_count: Arc<AtomicU64>,
    upstream_connect_duration_sum_ms: Arc<AtomicU64>,
    ws_connect_duration_buckets: Arc<Vec<AtomicU64>>,
    ws_connect_duration_count: Arc<AtomicU64>,
    ws_connect_duration_sum_ms: Arc<AtomicU64>,
    first_event_duration_buckets: Arc<Vec<AtomicU64>>,
    first_event_duration_count: Arc<AtomicU64>,
    first_event_duration_sum_ms: Arc<AtomicU64>,
    request_duration_labels: Arc<Mutex<BTreeMap<RequestDurationMetricKey, HistogramCounts>>>,
    upstream_connect_duration_labels:
        Arc<Mutex<BTreeMap<UpstreamConnectDurationMetricKey, HistogramCounts>>>,
    ws_connect_duration_labels: Arc<Mutex<BTreeMap<WsConnectDurationMetricKey, HistogramCounts>>>,
    first_event_duration_labels: Arc<Mutex<BTreeMap<FirstEventDurationMetricKey, HistogramCounts>>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            requests_total: Arc::new(AtomicU64::new(0)),
            request_outcomes: Arc::new(Mutex::new(BTreeMap::new())),
            request_shapes: Arc::new(Mutex::new(BTreeMap::new())),
            route_exclusions: Arc::new(Mutex::new(BTreeMap::new())),
            upstream_attempts_total: Arc::new(AtomicU64::new(0)),
            upstream_attempts: Arc::new(Mutex::new(BTreeMap::new())),
            active_websocket_sessions: Arc::new(AtomicU64::new(0)),
            websocket_events_total: Arc::new(AtomicU64::new(0)),
            websocket_event_outcomes: Arc::new(Mutex::new(BTreeMap::new())),
            sse_events_total: Arc::new(AtomicU64::new(0)),
            sse_event_outcomes: Arc::new(Mutex::new(BTreeMap::new())),
            replay_items_total: Arc::new(AtomicU64::new(0)),
            replay_item_outcomes: Arc::new(Mutex::new(BTreeMap::new())),
            cached_input_tokens: Arc::new(Mutex::new(BTreeMap::new())),
            request_duration_buckets: Arc::new(
                REQUEST_DURATION_BUCKETS_MS
                    .iter()
                    .map(|_| AtomicU64::new(0))
                    .collect(),
            ),
            request_duration_count: Arc::new(AtomicU64::new(0)),
            request_duration_sum_ms: Arc::new(AtomicU64::new(0)),
            upstream_connect_duration_buckets: Arc::new(
                REQUEST_DURATION_BUCKETS_MS
                    .iter()
                    .map(|_| AtomicU64::new(0))
                    .collect(),
            ),
            upstream_connect_duration_count: Arc::new(AtomicU64::new(0)),
            upstream_connect_duration_sum_ms: Arc::new(AtomicU64::new(0)),
            ws_connect_duration_buckets: Arc::new(
                REQUEST_DURATION_BUCKETS_MS
                    .iter()
                    .map(|_| AtomicU64::new(0))
                    .collect(),
            ),
            ws_connect_duration_count: Arc::new(AtomicU64::new(0)),
            ws_connect_duration_sum_ms: Arc::new(AtomicU64::new(0)),
            first_event_duration_buckets: Arc::new(
                REQUEST_DURATION_BUCKETS_MS
                    .iter()
                    .map(|_| AtomicU64::new(0))
                    .collect(),
            ),
            first_event_duration_count: Arc::new(AtomicU64::new(0)),
            first_event_duration_sum_ms: Arc::new(AtomicU64::new(0)),
            request_duration_labels: Arc::new(Mutex::new(BTreeMap::new())),
            upstream_connect_duration_labels: Arc::new(Mutex::new(BTreeMap::new())),
            ws_connect_duration_labels: Arc::new(Mutex::new(BTreeMap::new())),
            first_event_duration_labels: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn increment_requests(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_request_outcome(
        &self,
        endpoint: &str,
        transport: &str,
        status_class: &str,
        model_family: &str,
        account_id_hash: &str,
    ) {
        let mut request_outcomes = self
            .request_outcomes
            .lock()
            .expect("request outcome metrics lock is not poisoned");
        let key = RequestMetricKey {
            endpoint: endpoint.to_string(),
            transport: transport.to_string(),
            status_class: status_class.to_string(),
            model_family: model_family.to_string(),
            account_id_hash: account_id_hash.to_string(),
        };
        *request_outcomes.entry(key).or_insert(0) += 1;
    }

    pub fn increment_request_shape(
        &self,
        endpoint: &str,
        model_family: &str,
        service_tier: &str,
        reasoning_effort: &str,
        verbosity: &str,
        store: &str,
    ) {
        let mut request_shapes = self
            .request_shapes
            .lock()
            .expect("request shape metrics lock is not poisoned");
        let key = RequestShapeMetricKey {
            endpoint: endpoint.to_string(),
            model_family: model_family.to_string(),
            service_tier: service_tier.to_string(),
            reasoning_effort: reasoning_effort.to_string(),
            verbosity: verbosity.to_string(),
            store: store.to_string(),
        };
        *request_shapes.entry(key).or_insert(0) += 1;
    }

    pub fn increment_route_exclusion(&self, reason: &str) {
        let mut route_exclusions = self
            .route_exclusions
            .lock()
            .expect("route exclusion metrics lock is not poisoned");
        let key = RouteExclusionMetricKey {
            reason: reason.to_string(),
        };
        *route_exclusions.entry(key).or_insert(0) += 1;
    }

    pub fn increment_upstream_attempt(
        &self,
        endpoint: &str,
        transport: &str,
        model_family: &str,
        account_id_hash: &str,
        retry_phase: &str,
        outcome: &str,
    ) {
        self.upstream_attempts_total.fetch_add(1, Ordering::Relaxed);
        let mut upstream_attempts = self
            .upstream_attempts
            .lock()
            .expect("upstream attempt metrics lock is not poisoned");
        let key = UpstreamAttemptMetricKey {
            endpoint: endpoint.to_string(),
            transport: transport.to_string(),
            model_family: model_family.to_string(),
            account_id_hash: account_id_hash.to_string(),
            retry_phase: retry_phase.to_string(),
            outcome: outcome.to_string(),
        };
        *upstream_attempts.entry(key).or_insert(0) += 1;
    }

    pub fn increment_active_websocket_sessions(&self) {
        self.active_websocket_sessions
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_active_websocket_sessions(&self) {
        let _ = self.active_websocket_sessions.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| value.checked_sub(1),
        );
    }

    pub fn increment_websocket_events(&self) {
        self.websocket_events_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_websocket_event_outcome(&self, event_type: &str, success: bool) {
        self.websocket_events_total.fetch_add(1, Ordering::Relaxed);
        let mut websocket_event_outcomes = self
            .websocket_event_outcomes
            .lock()
            .expect("websocket event metrics lock is not poisoned");
        let key = WebSocketEventMetricKey {
            event_type: event_type.to_string(),
            success,
        };
        *websocket_event_outcomes.entry(key).or_insert(0) += 1;
    }

    pub fn increment_sse_event_outcome(&self, event_type: &str, success: bool) {
        self.increment_sse_event_outcome_labeled(event_type, success, "unknown", "unknown");
    }

    pub fn increment_sse_event_outcome_labeled(
        &self,
        event_type: &str,
        success: bool,
        model_family: &str,
        account_id_hash: &str,
    ) {
        self.sse_events_total.fetch_add(1, Ordering::Relaxed);
        let mut sse_event_outcomes = self
            .sse_event_outcomes
            .lock()
            .expect("SSE event metrics lock is not poisoned");
        let key = SseEventMetricKey {
            event_type: event_type.to_string(),
            success,
            model_family: model_family.to_string(),
            account_id_hash: account_id_hash.to_string(),
        };
        *sse_event_outcomes.entry(key).or_insert(0) += 1;
    }

    pub fn add_replay_items(&self, count: u64) {
        self.replay_items_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_replay_items_for_reason(&self, transport: &str, reason: &str, count: u64) {
        self.replay_items_total.fetch_add(count, Ordering::Relaxed);
        let mut replay_item_outcomes = self
            .replay_item_outcomes
            .lock()
            .expect("replay item metrics lock is not poisoned");
        let key = ReplayItemMetricKey {
            transport: transport.to_string(),
            reason: reason.to_string(),
        };
        *replay_item_outcomes.entry(key).or_insert(0) += count;
    }

    pub fn add_cached_input_tokens(&self, endpoint: &str, model_family: &str, count: u64) {
        let mut cached_input_tokens = self
            .cached_input_tokens
            .lock()
            .expect("cached input token metrics lock is not poisoned");
        let key = CachedInputTokensMetricKey {
            endpoint: endpoint.to_string(),
            model_family: model_family.to_string(),
        };
        *cached_input_tokens.entry(key).or_insert(0) += count;
    }

    pub fn record_request_duration_ms(&self, duration_ms: u64) {
        record_histogram(
            &self.request_duration_buckets,
            &self.request_duration_count,
            &self.request_duration_sum_ms,
            duration_ms,
        );
    }

    pub fn record_request_duration_labeled(
        &self,
        endpoint: &str,
        transport: &str,
        model_family: &str,
        stream: &str,
        duration_ms: u64,
    ) {
        self.record_request_duration_ms(duration_ms);
        let mut labels = self
            .request_duration_labels
            .lock()
            .expect("request duration metrics lock is not poisoned");
        let key = RequestDurationMetricKey {
            endpoint: endpoint.to_string(),
            transport: transport.to_string(),
            model_family: model_family.to_string(),
            stream: stream.to_string(),
        };
        labels
            .entry(key)
            .or_insert_with(HistogramCounts::new)
            .record(duration_ms);
    }

    pub fn record_upstream_connect_duration_labeled(
        &self,
        origin: &str,
        transport: &str,
        duration_ms: u64,
    ) {
        record_histogram(
            &self.upstream_connect_duration_buckets,
            &self.upstream_connect_duration_count,
            &self.upstream_connect_duration_sum_ms,
            duration_ms,
        );
        let mut labels = self
            .upstream_connect_duration_labels
            .lock()
            .expect("upstream connect duration metrics lock is not poisoned");
        let key = UpstreamConnectDurationMetricKey {
            origin: origin.to_string(),
            transport: transport.to_string(),
        };
        labels
            .entry(key)
            .or_insert_with(HistogramCounts::new)
            .record(duration_ms);
    }

    pub fn record_ws_connect_duration_ms(&self, duration_ms: u64) {
        record_histogram(
            &self.ws_connect_duration_buckets,
            &self.ws_connect_duration_count,
            &self.ws_connect_duration_sum_ms,
            duration_ms,
        );
    }

    pub fn record_ws_connect_duration_labeled(
        &self,
        origin: &str,
        model_family: &str,
        reused: bool,
        duration_ms: u64,
    ) {
        self.record_ws_connect_duration_ms(duration_ms);
        let mut labels = self
            .ws_connect_duration_labels
            .lock()
            .expect("websocket connect duration metrics lock is not poisoned");
        let key = WsConnectDurationMetricKey {
            origin: origin.to_string(),
            model_family: model_family.to_string(),
            reused,
        };
        labels
            .entry(key)
            .or_insert_with(HistogramCounts::new)
            .record(duration_ms);
    }

    pub fn record_first_event_duration_ms(&self, duration_ms: u64) {
        record_histogram(
            &self.first_event_duration_buckets,
            &self.first_event_duration_count,
            &self.first_event_duration_sum_ms,
            duration_ms,
        );
    }

    pub fn record_first_event_duration_labeled(
        &self,
        endpoint: &str,
        transport: &str,
        model_family: &str,
        duration_ms: u64,
    ) {
        self.record_first_event_duration_ms(duration_ms);
        let mut labels = self
            .first_event_duration_labels
            .lock()
            .expect("first event duration metrics lock is not poisoned");
        let key = FirstEventDurationMetricKey {
            endpoint: endpoint.to_string(),
            transport: transport.to_string(),
            model_family: model_family.to_string(),
        };
        labels
            .entry(key)
            .or_insert_with(HistogramCounts::new)
            .record(duration_ms);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            request_outcomes: self
                .request_outcomes
                .lock()
                .expect("request outcome metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            request_shapes: self
                .request_shapes
                .lock()
                .expect("request shape metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            route_exclusions: self
                .route_exclusions
                .lock()
                .expect("route exclusion metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            upstream_attempts_total: self.upstream_attempts_total.load(Ordering::Relaxed),
            upstream_attempts: self
                .upstream_attempts
                .lock()
                .expect("upstream attempt metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            active_websocket_sessions: self.active_websocket_sessions.load(Ordering::Relaxed),
            websocket_events_total: self.websocket_events_total.load(Ordering::Relaxed),
            websocket_event_outcomes: self
                .websocket_event_outcomes
                .lock()
                .expect("websocket event metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            sse_events_total: self.sse_events_total.load(Ordering::Relaxed),
            sse_event_outcomes: self
                .sse_event_outcomes
                .lock()
                .expect("SSE event metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            replay_items_total: self.replay_items_total.load(Ordering::Relaxed),
            replay_item_outcomes: self
                .replay_item_outcomes
                .lock()
                .expect("replay item metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            cached_input_tokens: self
                .cached_input_tokens
                .lock()
                .expect("cached input token metrics lock is not poisoned")
                .iter()
                .map(|(key, count)| (key.clone(), *count))
                .collect(),
            request_duration_buckets: REQUEST_DURATION_BUCKETS_MS
                .iter()
                .copied()
                .zip(
                    self.request_duration_buckets
                        .iter()
                        .map(|bucket| bucket.load(Ordering::Relaxed)),
                )
                .collect(),
            request_duration_count: self.request_duration_count.load(Ordering::Relaxed),
            request_duration_sum_ms: self.request_duration_sum_ms.load(Ordering::Relaxed),
            upstream_connect_duration_buckets: REQUEST_DURATION_BUCKETS_MS
                .iter()
                .copied()
                .zip(
                    self.upstream_connect_duration_buckets
                        .iter()
                        .map(|bucket| bucket.load(Ordering::Relaxed)),
                )
                .collect(),
            upstream_connect_duration_count: self
                .upstream_connect_duration_count
                .load(Ordering::Relaxed),
            upstream_connect_duration_sum_ms: self
                .upstream_connect_duration_sum_ms
                .load(Ordering::Relaxed),
            ws_connect_duration_buckets: REQUEST_DURATION_BUCKETS_MS
                .iter()
                .copied()
                .zip(
                    self.ws_connect_duration_buckets
                        .iter()
                        .map(|bucket| bucket.load(Ordering::Relaxed)),
                )
                .collect(),
            ws_connect_duration_count: self.ws_connect_duration_count.load(Ordering::Relaxed),
            ws_connect_duration_sum_ms: self.ws_connect_duration_sum_ms.load(Ordering::Relaxed),
            first_event_duration_buckets: REQUEST_DURATION_BUCKETS_MS
                .iter()
                .copied()
                .zip(
                    self.first_event_duration_buckets
                        .iter()
                        .map(|bucket| bucket.load(Ordering::Relaxed)),
                )
                .collect(),
            first_event_duration_count: self.first_event_duration_count.load(Ordering::Relaxed),
            first_event_duration_sum_ms: self.first_event_duration_sum_ms.load(Ordering::Relaxed),
            request_duration_labels: self
                .request_duration_labels
                .lock()
                .expect("request duration metrics lock is not poisoned")
                .iter()
                .map(|(key, counts)| (key.clone(), counts.snapshot()))
                .collect(),
            upstream_connect_duration_labels: self
                .upstream_connect_duration_labels
                .lock()
                .expect("upstream connect duration metrics lock is not poisoned")
                .iter()
                .map(|(key, counts)| (key.clone(), counts.snapshot()))
                .collect(),
            ws_connect_duration_labels: self
                .ws_connect_duration_labels
                .lock()
                .expect("websocket connect duration metrics lock is not poisoned")
                .iter()
                .map(|(key, counts)| (key.clone(), counts.snapshot()))
                .collect(),
            first_event_duration_labels: self
                .first_event_duration_labels
                .lock()
                .expect("first event duration metrics lock is not poisoned")
                .iter()
                .map(|(key, counts)| (key.clone(), counts.snapshot()))
                .collect(),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestMetricKey {
    pub endpoint: String,
    pub transport: String,
    pub status_class: String,
    pub model_family: String,
    pub account_id_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestShapeMetricKey {
    pub endpoint: String,
    pub model_family: String,
    pub service_tier: String,
    pub reasoning_effort: String,
    pub verbosity: String,
    pub store: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RouteExclusionMetricKey {
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UpstreamAttemptMetricKey {
    pub endpoint: String,
    pub transport: String,
    pub model_family: String,
    pub account_id_hash: String,
    pub retry_phase: String,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WebSocketEventMetricKey {
    pub event_type: String,
    pub success: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SseEventMetricKey {
    pub event_type: String,
    pub success: bool,
    pub model_family: String,
    pub account_id_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplayItemMetricKey {
    pub transport: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CachedInputTokensMetricKey {
    pub endpoint: String,
    pub model_family: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequestDurationMetricKey {
    pub endpoint: String,
    pub transport: String,
    pub model_family: String,
    pub stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UpstreamConnectDurationMetricKey {
    pub origin: String,
    pub transport: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FirstEventDurationMetricKey {
    pub endpoint: String,
    pub transport: String,
    pub model_family: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WsConnectDurationMetricKey {
    pub origin: String,
    pub model_family: String,
    pub reused: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<(u64, u64)>,
    pub count: u64,
    pub sum_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistogramCounts {
    buckets: Vec<u64>,
    count: u64,
    sum_ms: u64,
}

impl HistogramCounts {
    fn new() -> Self {
        Self {
            buckets: vec![0; REQUEST_DURATION_BUCKETS_MS.len()],
            count: 0,
            sum_ms: 0,
        }
    }

    fn record(&mut self, duration_ms: u64) {
        self.count += 1;
        self.sum_ms += duration_ms;
        for (bucket, upper_bound) in self.buckets.iter_mut().zip(REQUEST_DURATION_BUCKETS_MS) {
            if duration_ms <= *upper_bound {
                *bucket += 1;
            }
        }
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: REQUEST_DURATION_BUCKETS_MS
                .iter()
                .copied()
                .zip(self.buckets.iter().copied())
                .collect(),
            count: self.count,
            sum_ms: self.sum_ms,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub requests_total: u64,
    pub request_outcomes: Vec<(RequestMetricKey, u64)>,
    pub request_shapes: Vec<(RequestShapeMetricKey, u64)>,
    pub route_exclusions: Vec<(RouteExclusionMetricKey, u64)>,
    pub upstream_attempts_total: u64,
    pub upstream_attempts: Vec<(UpstreamAttemptMetricKey, u64)>,
    pub active_websocket_sessions: u64,
    pub websocket_events_total: u64,
    pub websocket_event_outcomes: Vec<(WebSocketEventMetricKey, u64)>,
    pub sse_events_total: u64,
    pub sse_event_outcomes: Vec<(SseEventMetricKey, u64)>,
    pub replay_items_total: u64,
    pub replay_item_outcomes: Vec<(ReplayItemMetricKey, u64)>,
    pub cached_input_tokens: Vec<(CachedInputTokensMetricKey, u64)>,
    pub request_duration_buckets: Vec<(u64, u64)>,
    pub request_duration_count: u64,
    pub request_duration_sum_ms: u64,
    pub upstream_connect_duration_buckets: Vec<(u64, u64)>,
    pub upstream_connect_duration_count: u64,
    pub upstream_connect_duration_sum_ms: u64,
    pub ws_connect_duration_buckets: Vec<(u64, u64)>,
    pub ws_connect_duration_count: u64,
    pub ws_connect_duration_sum_ms: u64,
    pub first_event_duration_buckets: Vec<(u64, u64)>,
    pub first_event_duration_count: u64,
    pub first_event_duration_sum_ms: u64,
    pub request_duration_labels: Vec<(RequestDurationMetricKey, HistogramSnapshot)>,
    pub upstream_connect_duration_labels:
        Vec<(UpstreamConnectDurationMetricKey, HistogramSnapshot)>,
    pub ws_connect_duration_labels: Vec<(WsConnectDurationMetricKey, HistogramSnapshot)>,
    pub first_event_duration_labels: Vec<(FirstEventDurationMetricKey, HistogramSnapshot)>,
}

pub fn prometheus_text(snapshot: &MetricsSnapshot) -> String {
    prometheus_text_with_usage(snapshot, None)
}

pub fn prometheus_text_with_usage(
    snapshot: &MetricsSnapshot,
    usage: Option<&UsageSnapshot>,
) -> String {
    let mut output = format!(
        "# TYPE tokenproxy_requests_total counter\ntokenproxy_requests_total {}\n",
        snapshot.requests_total
    );
    for (key, count) in &snapshot.request_outcomes {
        output.push_str(&format!(
            "tokenproxy_requests_total{{endpoint=\"{}\",transport=\"{}\",status_class=\"{}\",model_family=\"{}\",account_id_hash=\"{}\"}} {}\n",
            escape_label(&key.endpoint),
            escape_label(&key.transport),
            escape_label(&key.status_class),
            escape_label(&key.model_family),
            escape_label(&key.account_id_hash),
            count
        ));
    }
    output.push_str("# TYPE tokenproxy_request_shape_total counter\n");
    for (key, count) in &snapshot.request_shapes {
        output.push_str(&format!(
            "tokenproxy_request_shape_total{{endpoint=\"{}\",model_family=\"{}\",service_tier=\"{}\",reasoning_effort=\"{}\",verbosity=\"{}\",store=\"{}\"}} {}\n",
            escape_label(&key.endpoint),
            escape_label(&key.model_family),
            escape_label(&key.service_tier),
            escape_label(&key.reasoning_effort),
            escape_label(&key.verbosity),
            escape_label(&key.store),
            count
        ));
    }
    output.push_str("# TYPE tokenproxy_route_exclusions_total counter\n");
    for (key, count) in &snapshot.route_exclusions {
        output.push_str(&format!(
            "tokenproxy_route_exclusions_total{{reason=\"{}\"}} {}\n",
            escape_label(&key.reason),
            count
        ));
    }
    output.push_str(&format!(
        "# TYPE tokenproxy_upstream_attempts_total counter\ntokenproxy_upstream_attempts_total {}\n",
        snapshot.upstream_attempts_total
    ));
    for (key, count) in &snapshot.upstream_attempts {
        output.push_str(&format!(
            "tokenproxy_upstream_attempts_total{{endpoint=\"{}\",transport=\"{}\",model_family=\"{}\",account_id_hash=\"{}\",retry_phase=\"{}\",outcome=\"{}\"}} {}\n",
            escape_label(&key.endpoint),
            escape_label(&key.transport),
            escape_label(&key.model_family),
            escape_label(&key.account_id_hash),
            escape_label(&key.retry_phase),
            escape_label(&key.outcome),
            count
        ));
    }
    output.push_str(&format!(
        "# TYPE tokenproxy_ws_events_total counter\ntokenproxy_ws_events_total {}\n",
        snapshot.websocket_events_total
    ));
    output.push_str(&format!(
        "# TYPE tokenproxy_active_websocket_sessions gauge\ntokenproxy_active_websocket_sessions {}\n",
        snapshot.active_websocket_sessions
    ));
    for (key, count) in &snapshot.websocket_event_outcomes {
        output.push_str(&format!(
            "tokenproxy_ws_events_total{{event_type=\"{}\",success=\"{}\"}} {}\n",
            escape_label(&key.event_type),
            key.success,
            count
        ));
    }
    output.push_str(&format!(
        "# TYPE tokenproxy_sse_events_total counter\ntokenproxy_sse_events_total {}\n",
        snapshot.sse_events_total
    ));
    for (key, count) in &snapshot.sse_event_outcomes {
        output.push_str(&format!(
            "tokenproxy_sse_events_total{{event_type=\"{}\",success=\"{}\",model_family=\"{}\",account_id_hash=\"{}\"}} {}\n",
            escape_label(&key.event_type),
            key.success,
            escape_label(&key.model_family),
            escape_label(&key.account_id_hash),
            count
        ));
    }
    output.push_str(&format!(
        "# TYPE tokenproxy_replay_items_total counter\ntokenproxy_replay_items_total {}\n",
        snapshot.replay_items_total
    ));
    for (key, count) in &snapshot.replay_item_outcomes {
        output.push_str(&format!(
            "tokenproxy_replay_items_total{{transport=\"{}\",reason=\"{}\"}} {}\n",
            escape_label(&key.transport),
            escape_label(&key.reason),
            count
        ));
    }
    output.push_str("# TYPE tokenproxy_cached_input_tokens_total counter\n");
    for (key, count) in &snapshot.cached_input_tokens {
        output.push_str(&format!(
            "tokenproxy_cached_input_tokens_total{{endpoint=\"{}\",model_family=\"{}\"}} {}\n",
            escape_label(&key.endpoint),
            escape_label(&key.model_family),
            count
        ));
    }
    append_histogram(
        &mut output,
        "tokenproxy_request_duration_ms",
        &snapshot.request_duration_buckets,
        snapshot.request_duration_count,
        snapshot.request_duration_sum_ms,
    );
    for (key, histogram) in &snapshot.request_duration_labels {
        append_labeled_histogram(
            &mut output,
            "tokenproxy_request_duration_ms",
            &histogram.buckets,
            histogram.count,
            histogram.sum_ms,
            &[
                ("endpoint", key.endpoint.as_str()),
                ("transport", key.transport.as_str()),
                ("model_family", key.model_family.as_str()),
                ("stream", key.stream.as_str()),
            ],
        );
    }
    append_histogram(
        &mut output,
        "tokenproxy_upstream_connect_duration_ms",
        &snapshot.upstream_connect_duration_buckets,
        snapshot.upstream_connect_duration_count,
        snapshot.upstream_connect_duration_sum_ms,
    );
    for (key, histogram) in &snapshot.upstream_connect_duration_labels {
        append_labeled_histogram(
            &mut output,
            "tokenproxy_upstream_connect_duration_ms",
            &histogram.buckets,
            histogram.count,
            histogram.sum_ms,
            &[
                ("origin", key.origin.as_str()),
                ("transport", key.transport.as_str()),
            ],
        );
    }
    append_histogram(
        &mut output,
        "tokenproxy_ws_connect_duration_ms",
        &snapshot.ws_connect_duration_buckets,
        snapshot.ws_connect_duration_count,
        snapshot.ws_connect_duration_sum_ms,
    );
    for (key, histogram) in &snapshot.ws_connect_duration_labels {
        let reused = key.reused.to_string();
        append_labeled_histogram(
            &mut output,
            "tokenproxy_ws_connect_duration_ms",
            &histogram.buckets,
            histogram.count,
            histogram.sum_ms,
            &[
                ("origin", key.origin.as_str()),
                ("model_family", key.model_family.as_str()),
                ("reused", reused.as_str()),
            ],
        );
    }
    append_histogram(
        &mut output,
        "tokenproxy_first_event_duration_ms",
        &snapshot.first_event_duration_buckets,
        snapshot.first_event_duration_count,
        snapshot.first_event_duration_sum_ms,
    );
    for (key, histogram) in &snapshot.first_event_duration_labels {
        append_labeled_histogram(
            &mut output,
            "tokenproxy_first_event_duration_ms",
            &histogram.buckets,
            histogram.count,
            histogram.sum_ms,
            &[
                ("endpoint", key.endpoint.as_str()),
                ("transport", key.transport.as_str()),
                ("model_family", key.model_family.as_str()),
            ],
        );
    }
    if let Some(usage) = usage {
        append_usage_metrics(&mut output, usage);
    }
    output
}

fn append_labeled_histogram(
    output: &mut String,
    name: &str,
    buckets: &[(u64, u64)],
    count: u64,
    sum_ms: u64,
    labels: &[(&str, &str)],
) {
    let labels = metric_labels(labels);
    for (upper_bound, count) in buckets {
        output.push_str(&format!(
            "{name}_bucket{{{labels},le=\"{}\"}} {}\n",
            upper_bound, count
        ));
    }
    output.push_str(&format!(
        "{name}_bucket{{{labels},le=\"+Inf\"}} {count}\n{name}_sum{{{labels}}} {sum_ms}\n{name}_count{{{labels}}} {count}\n"
    ));
}

fn append_histogram(
    output: &mut String,
    name: &str,
    buckets: &[(u64, u64)],
    count: u64,
    sum_ms: u64,
) {
    output.push_str(&format!("# TYPE {name} histogram\n"));
    for (upper_bound, count) in buckets {
        output.push_str(&format!(
            "{name}_bucket{{le=\"{}\"}} {}\n",
            upper_bound, count
        ));
    }
    output.push_str(&format!(
        "{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_sum {sum_ms}\n{name}_count {count}\n"
    ));
}

fn record_histogram(buckets: &[AtomicU64], count: &AtomicU64, sum: &AtomicU64, duration_ms: u64) {
    count.fetch_add(1, Ordering::Relaxed);
    sum.fetch_add(duration_ms, Ordering::Relaxed);
    for (bucket, upper_bound) in buckets.iter().zip(REQUEST_DURATION_BUCKETS_MS) {
        if duration_ms <= *upper_bound {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn append_usage_metrics(output: &mut String, usage: &UsageSnapshot) {
    output.push_str("# TYPE tokenproxy_account_health gauge\n");
    for account in &usage.accounts {
        for state in [
            "open",
            "unknown",
            "throttled",
            "usage_limited",
            "auth_failed",
            "disabled",
        ] {
            let value = u8::from(account.health == state);
            output.push_str(&format!(
                "tokenproxy_account_health{{account_id_hash=\"{}\",state=\"{}\"}} {}\n",
                escape_label(&account.account_id_hash),
                state,
                value
            ));
        }
        for window in &account.usage {
            if let Some(percent) = window.remaining_percent {
                output.push_str(&format!(
                    "tokenproxy_account_usage_remaining_percent{{server_id=\"{}\",account_id_hash=\"{}\",window=\"{}\",source=\"{}\"}} {}\n",
                    escape_label(&usage.server_id),
                    escape_label(&account.account_id_hash),
                    escape_label(&window.window),
                    escape_label(&window.source),
                    percent
                ));
            }
            if let Some(reset_at) = window
                .reset_at
                .as_deref()
                .and_then(rfc3339_timestamp_seconds)
            {
                output.push_str(&format!(
                    "tokenproxy_account_usage_reset_timestamp_seconds{{server_id=\"{}\",account_id_hash=\"{}\",window=\"{}\",source=\"{}\"}} {}\n",
                    escape_label(&usage.server_id),
                    escape_label(&account.account_id_hash),
                    escape_label(&window.window),
                    escape_label(&window.source),
                    reset_at
                ));
            }
        }
    }
}

fn metric_labels(labels: &[(&str, &str)]) -> String {
    labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape_label(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn rfc3339_timestamp_seconds(value: &str) -> Option<i64> {
    unix_seconds_from_rfc3339(value)
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::{AccountUsage, UsageWindow};

    #[test]
    fn should_render_usage_health_and_reset_gauges_without_display_names() {
        let usage = UsageSnapshot {
            server_id: "tokenproxy-local".to_string(),
            observed_at: "2026-05-27T11:24:18Z".to_string(),
            accounts: vec![AccountUsage {
                account_id_hash: "acct_1234".to_string(),
                display_name: Some("secret label".to_string()),
                health: "usage_limited".to_string(),
                usage: vec![UsageWindow {
                    window: "codex_usage_limit".to_string(),
                    limit: None,
                    remaining: Some(0),
                    remaining_percent: Some(0.0),
                    rate_limit_pressure: "limited".to_string(),
                    reset_after: Some("60s".to_string()),
                    reset_at: Some("2026-05-27T11:25:18Z".to_string()),
                    source: "usage_limit_reached_error".to_string(),
                    observed_at: "2026-05-27T11:24:18Z".to_string(),
                    limited: true,
                }],
                cooldown_until: Some("2026-05-27T11:25:18Z".to_string()),
            }],
        };

        let text = prometheus_text_with_usage(&MetricsSnapshot::default(), Some(&usage));

        assert!(text.contains(
            r#"tokenproxy_account_health{account_id_hash="acct_1234",state="usage_limited"} 1"#
        ));
        assert!(
            text.contains(
                r#"tokenproxy_account_health{account_id_hash="acct_1234",state="open"} 0"#
            )
        );
        assert!(text.contains(
            r#"tokenproxy_account_health{account_id_hash="acct_1234",state="disabled"} 0"#
        ));
        assert!(text.contains(
            r#"tokenproxy_account_usage_remaining_percent{server_id="tokenproxy-local",account_id_hash="acct_1234",window="codex_usage_limit",source="usage_limit_reached_error"} 0"#
        ));
        assert!(text.contains(
            r#"tokenproxy_account_usage_reset_timestamp_seconds{server_id="tokenproxy-local",account_id_hash="acct_1234",window="codex_usage_limit",source="usage_limit_reached_error"} 1779881118"#
        ));
        assert!(!text.contains("secret label"));
    }

    #[test]
    fn should_not_render_pre_epoch_usage_reset_timestamp_gauge() {
        let usage = UsageSnapshot {
            server_id: "tokenproxy-local".to_string(),
            observed_at: "2026-05-27T11:24:18Z".to_string(),
            accounts: vec![AccountUsage {
                account_id_hash: "acct_1234".to_string(),
                display_name: None,
                health: "usage_limited".to_string(),
                usage: vec![UsageWindow {
                    window: "codex_usage_limit".to_string(),
                    limit: None,
                    remaining: Some(0),
                    remaining_percent: None,
                    rate_limit_pressure: "limited".to_string(),
                    reset_after: None,
                    reset_at: Some("1969-12-31T23:59:59Z".to_string()),
                    source: "usage_limit_reached_error".to_string(),
                    observed_at: "2026-05-27T11:24:18Z".to_string(),
                    limited: true,
                }],
                cooldown_until: None,
            }],
        };

        let text = prometheus_text_with_usage(&MetricsSnapshot::default(), Some(&usage));

        assert!(!text.contains("tokenproxy_account_usage_reset_timestamp_seconds"));
    }
}

#[cfg(test)]
mod counter_tests {
    use super::*;

    #[test]
    fn should_render_counter_metrics_without_usage_snapshot() {
        let snapshot = MetricsSnapshot {
            requests_total: 1,
            request_outcomes: vec![(
                RequestMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    transport: "http".to_string(),
                    status_class: "2xx".to_string(),
                    model_family: "gpt".to_string(),
                    account_id_hash: "acct_primary".to_string(),
                },
                1,
            )],
            upstream_attempts_total: 2,
            upstream_attempts: vec![(
                UpstreamAttemptMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    transport: "http".to_string(),
                    model_family: "gpt".to_string(),
                    account_id_hash: "acct_primary".to_string(),
                    retry_phase: "initial".to_string(),
                    outcome: "2xx".to_string(),
                },
                2,
            )],
            websocket_events_total: 3,
            websocket_event_outcomes: vec![(
                WebSocketEventMetricKey {
                    event_type: "upstream_text".to_string(),
                    success: true,
                },
                3,
            )],
            replay_items_total: 4,
            replay_item_outcomes: vec![(
                ReplayItemMetricKey {
                    transport: "websocket".to_string(),
                    reason: "full_replay".to_string(),
                },
                4,
            )],
            cached_input_tokens: vec![(
                CachedInputTokensMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    model_family: "gpt".to_string(),
                },
                40,
            )],
            ..MetricsSnapshot::default()
        };

        let text = prometheus_text(&snapshot);

        assert!(text.contains("tokenproxy_requests_total 1"));
        assert!(text.contains(
            r#"tokenproxy_requests_total{endpoint="/v1/responses",transport="http",status_class="2xx",model_family="gpt",account_id_hash="acct_primary"} 1"#
        ));
        assert!(text.contains("tokenproxy_upstream_attempts_total 2"));
        assert!(text.contains(
            r#"tokenproxy_upstream_attempts_total{endpoint="/v1/responses",transport="http",model_family="gpt",account_id_hash="acct_primary",retry_phase="initial",outcome="2xx"} 2"#
        ));
        assert!(text.contains("tokenproxy_ws_events_total 3"));
        assert!(text.contains(
            r#"tokenproxy_ws_events_total{event_type="upstream_text",success="true"} 3"#
        ));
        assert!(text.contains("tokenproxy_replay_items_total 4"));
        assert!(text.contains(
            r#"tokenproxy_replay_items_total{transport="websocket",reason="full_replay"} 4"#
        ));
        assert!(text.contains(
            r#"tokenproxy_cached_input_tokens_total{endpoint="/v1/responses",model_family="gpt"} 40"#
        ));
        assert!(!text.contains("tokenproxy_account_health"));
    }

    #[test]
    fn should_accumulate_cached_input_tokens_by_endpoint_and_model_family() {
        let metrics = Metrics::default();

        metrics.add_cached_input_tokens("/v1/responses", "gpt", 17);
        metrics.add_cached_input_tokens("/v1/responses", "gpt", 23);
        metrics.add_cached_input_tokens("/v1/chat/completions", "gpt", 5);

        let snapshot = metrics.snapshot();

        assert_eq!(
            snapshot.cached_input_tokens,
            vec![
                (
                    CachedInputTokensMetricKey {
                        endpoint: "/v1/chat/completions".to_string(),
                        model_family: "gpt".to_string(),
                    },
                    5,
                ),
                (
                    CachedInputTokensMetricKey {
                        endpoint: "/v1/responses".to_string(),
                        model_family: "gpt".to_string(),
                    },
                    40,
                ),
            ]
        );
    }

    #[test]
    fn should_accumulate_labeled_request_outcomes() {
        let metrics = Metrics::default();

        metrics.increment_requests();
        metrics.increment_request_outcome("/v1/responses", "http", "2xx", "gpt", "acct_primary");
        metrics.increment_request_outcome("/v1/responses", "http", "2xx", "gpt", "acct_primary");
        metrics.increment_request_outcome(
            "/v1/chat/completions",
            "http",
            "4xx",
            "gpt",
            "acct_backup",
        );

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(
            snapshot.request_outcomes,
            vec![
                (
                    RequestMetricKey {
                        endpoint: "/v1/chat/completions".to_string(),
                        transport: "http".to_string(),
                        status_class: "4xx".to_string(),
                        model_family: "gpt".to_string(),
                        account_id_hash: "acct_backup".to_string(),
                    },
                    1,
                ),
                (
                    RequestMetricKey {
                        endpoint: "/v1/responses".to_string(),
                        transport: "http".to_string(),
                        status_class: "2xx".to_string(),
                        model_family: "gpt".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                    },
                    2,
                ),
            ]
        );
    }

    #[test]
    fn should_record_and_render_route_exclusion_counters() {
        let metrics = Metrics::default();

        metrics.increment_route_exclusion("model_unsupported");
        metrics.increment_route_exclusion("model_unsupported");
        metrics.increment_route_exclusion("throttled_cooldown");

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.route_exclusions,
            vec![
                (
                    RouteExclusionMetricKey {
                        reason: "model_unsupported".to_string(),
                    },
                    2,
                ),
                (
                    RouteExclusionMetricKey {
                        reason: "throttled_cooldown".to_string(),
                    },
                    1,
                ),
            ]
        );
        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_route_exclusions_total counter"));
        assert!(
            text.contains(r#"tokenproxy_route_exclusions_total{reason="model_unsupported"} 2"#)
        );
    }

    #[test]
    fn should_accumulate_labeled_upstream_attempt_counters() {
        let metrics = Metrics::default();

        metrics.increment_upstream_attempt(
            "/v1/responses",
            "http",
            "gpt",
            "acct_primary",
            "initial",
            "5xx",
        );
        metrics.increment_upstream_attempt(
            "/v1/responses",
            "http",
            "gpt",
            "acct_primary",
            "retry",
            "2xx",
        );

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.upstream_attempts_total, 2);
        assert_eq!(
            snapshot.upstream_attempts,
            vec![
                (
                    UpstreamAttemptMetricKey {
                        endpoint: "/v1/responses".to_string(),
                        transport: "http".to_string(),
                        model_family: "gpt".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                        retry_phase: "initial".to_string(),
                        outcome: "5xx".to_string(),
                    },
                    1,
                ),
                (
                    UpstreamAttemptMetricKey {
                        endpoint: "/v1/responses".to_string(),
                        transport: "http".to_string(),
                        model_family: "gpt".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                        retry_phase: "retry".to_string(),
                        outcome: "2xx".to_string(),
                    },
                    1,
                ),
            ]
        );
    }

    #[test]
    fn should_record_and_render_request_shape_counters() {
        let metrics = Metrics::default();

        metrics.increment_request_shape("/v1/responses", "gpt", "priority", "high", "low", "true");

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.request_shapes,
            vec![(
                RequestShapeMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    model_family: "gpt".to_string(),
                    service_tier: "priority".to_string(),
                    reasoning_effort: "high".to_string(),
                    verbosity: "low".to_string(),
                    store: "true".to_string(),
                },
                1,
            )]
        );
        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_request_shape_total counter"));
        assert!(text.contains(
            r#"tokenproxy_request_shape_total{endpoint="/v1/responses",model_family="gpt",service_tier="priority",reasoning_effort="high",verbosity="low",store="true"} 1"#
        ));
    }

    #[test]
    fn should_accumulate_labeled_websocket_and_replay_counters() {
        let metrics = Metrics::default();

        metrics.increment_websocket_event_outcome("upstream_text", true);
        metrics.increment_websocket_event_outcome("upstream_text", true);
        metrics.increment_websocket_event_outcome("downstream_parse", false);
        metrics.add_replay_items_for_reason("websocket", "full_replay", 3);
        metrics.add_replay_items_for_reason("websocket", "previous_response_not_found", 2);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.websocket_events_total, 3);
        assert_eq!(
            snapshot.websocket_event_outcomes,
            vec![
                (
                    WebSocketEventMetricKey {
                        event_type: "downstream_parse".to_string(),
                        success: false,
                    },
                    1,
                ),
                (
                    WebSocketEventMetricKey {
                        event_type: "upstream_text".to_string(),
                        success: true,
                    },
                    2,
                ),
            ]
        );
        assert_eq!(snapshot.replay_items_total, 5);
        assert_eq!(
            snapshot.replay_item_outcomes,
            vec![
                (
                    ReplayItemMetricKey {
                        transport: "websocket".to_string(),
                        reason: "full_replay".to_string(),
                    },
                    3,
                ),
                (
                    ReplayItemMetricKey {
                        transport: "websocket".to_string(),
                        reason: "previous_response_not_found".to_string(),
                    },
                    2,
                ),
            ]
        );
    }

    #[test]
    fn should_accumulate_labeled_sse_event_counters() {
        let metrics = Metrics::default();

        metrics.increment_sse_event_outcome_labeled(
            "response.completed",
            true,
            "gpt-5",
            "acct_primary",
        );
        metrics.increment_sse_event_outcome_labeled(
            "response.output_text.delta",
            true,
            "gpt-5",
            "acct_primary",
        );
        metrics.increment_sse_event_outcome_labeled(
            "response.failed",
            false,
            "gpt-5",
            "acct_primary",
        );

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.sse_events_total, 3);
        assert_eq!(
            snapshot.sse_event_outcomes,
            vec![
                (
                    SseEventMetricKey {
                        event_type: "response.completed".to_string(),
                        success: true,
                        model_family: "gpt-5".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                    },
                    1,
                ),
                (
                    SseEventMetricKey {
                        event_type: "response.failed".to_string(),
                        success: false,
                        model_family: "gpt-5".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                    },
                    1,
                ),
                (
                    SseEventMetricKey {
                        event_type: "response.output_text.delta".to_string(),
                        success: true,
                        model_family: "gpt-5".to_string(),
                        account_id_hash: "acct_primary".to_string(),
                    },
                    1,
                ),
            ]
        );
        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_sse_events_total counter"));
        assert!(text.contains(
            r#"tokenproxy_sse_events_total{event_type="response.failed",success="false",model_family="gpt-5",account_id_hash="acct_primary"} 1"#
        ));
    }

    #[test]
    fn should_track_active_websocket_sessions_without_underflow() {
        let metrics = Metrics::default();

        metrics.decrement_active_websocket_sessions();
        assert_eq!(metrics.snapshot().active_websocket_sessions, 0);

        metrics.increment_active_websocket_sessions();
        metrics.increment_active_websocket_sessions();
        metrics.decrement_active_websocket_sessions();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.active_websocket_sessions, 1);
        assert!(
            prometheus_text(&snapshot)
                .contains("# TYPE tokenproxy_active_websocket_sessions gauge")
        );
        assert!(prometheus_text(&snapshot).contains("tokenproxy_active_websocket_sessions 1"));
    }

    #[test]
    fn should_record_and_render_request_duration_histogram() {
        let metrics = Metrics::default();

        metrics.record_request_duration_ms(7);
        metrics.record_request_duration_ms(260);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.request_duration_count, 2);
        assert_eq!(snapshot.request_duration_sum_ms, 267);
        assert_eq!(snapshot.request_duration_buckets[0], (5, 0));
        assert_eq!(snapshot.request_duration_buckets[1], (10, 1));
        assert_eq!(snapshot.request_duration_buckets[6], (500, 2));

        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_request_duration_ms histogram"));
        assert!(text.contains(r#"tokenproxy_request_duration_ms_bucket{le="10"} 1"#));
        assert!(text.contains(r#"tokenproxy_request_duration_ms_bucket{le="+Inf"} 2"#));
        assert!(text.contains("tokenproxy_request_duration_ms_sum 267"));
        assert!(text.contains("tokenproxy_request_duration_ms_count 2"));
    }

    #[test]
    fn should_record_and_render_websocket_connect_duration_histogram() {
        let metrics = Metrics::default();

        metrics.record_ws_connect_duration_ms(42);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.ws_connect_duration_count, 1);
        assert_eq!(snapshot.ws_connect_duration_sum_ms, 42);
        assert_eq!(snapshot.ws_connect_duration_buckets[2], (25, 0));
        assert_eq!(snapshot.ws_connect_duration_buckets[3], (50, 1));

        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_ws_connect_duration_ms histogram"));
        assert!(text.contains(r#"tokenproxy_ws_connect_duration_ms_bucket{le="50"} 1"#));
        assert!(text.contains("tokenproxy_ws_connect_duration_ms_sum 42"));
        assert!(text.contains("tokenproxy_ws_connect_duration_ms_count 1"));
    }

    #[test]
    fn should_record_and_render_first_event_duration_histogram() {
        let metrics = Metrics::default();

        metrics.record_first_event_duration_ms(125);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.first_event_duration_count, 1);
        assert_eq!(snapshot.first_event_duration_sum_ms, 125);
        assert_eq!(snapshot.first_event_duration_buckets[4], (100, 0));
        assert_eq!(snapshot.first_event_duration_buckets[5], (250, 1));

        let text = prometheus_text(&snapshot);
        assert!(text.contains("# TYPE tokenproxy_first_event_duration_ms histogram"));
        assert!(text.contains(r#"tokenproxy_first_event_duration_ms_bucket{le="250"} 1"#));
        assert!(text.contains("tokenproxy_first_event_duration_ms_sum 125"));
        assert!(text.contains("tokenproxy_first_event_duration_ms_count 1"));
    }

    #[test]
    fn should_record_and_render_labeled_duration_histograms() {
        let metrics = Metrics::default();

        metrics.record_request_duration_labeled("/v1/responses", "http", "gpt", "true", 7);
        metrics.record_upstream_connect_duration_labeled("api.openai.com", "http", 18);
        metrics.record_first_event_duration_labeled("/v1/responses", "websocket", "gpt", 125);
        metrics.record_ws_connect_duration_labeled("api.openai.com", "gpt", false, 42);

        let snapshot = metrics.snapshot();
        let text = prometheus_text(&snapshot);

        assert!(text.contains(
            r#"tokenproxy_request_duration_ms_bucket{endpoint="/v1/responses",transport="http",model_family="gpt",stream="true",le="10"} 1"#
        ));
        assert!(text.contains(
            r#"tokenproxy_request_duration_ms_sum{endpoint="/v1/responses",transport="http",model_family="gpt",stream="true"} 7"#
        ));
        assert!(text.contains(
            r#"tokenproxy_upstream_connect_duration_ms_bucket{origin="api.openai.com",transport="http",le="25"} 1"#
        ));
        assert!(text.contains(
            r#"tokenproxy_upstream_connect_duration_ms_sum{origin="api.openai.com",transport="http"} 18"#
        ));
        assert!(text.contains(
            r#"tokenproxy_first_event_duration_ms_bucket{endpoint="/v1/responses",transport="websocket",model_family="gpt",le="250"} 1"#
        ));
        assert!(text.contains(
            r#"tokenproxy_ws_connect_duration_ms_bucket{origin="api.openai.com",model_family="gpt",reused="false",le="50"} 1"#
        ));
        assert_eq!(snapshot.request_duration_count, 1);
        assert_eq!(snapshot.upstream_connect_duration_count, 1);
        assert_eq!(snapshot.first_event_duration_count, 1);
        assert_eq!(snapshot.ws_connect_duration_count, 1);
    }
}
