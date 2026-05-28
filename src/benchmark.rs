use serde::Deserialize;
use serde::Serialize;

use crate::build_info;
use crate::model::model_family_label;

pub const BENCHMARK_SCHEMA: &str = "tokenproxy.benchmark.v1";

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BenchmarkRecord {
    pub schema: &'static str,
    pub timestamp_local: String,
    pub timestamp_utc: String,
    pub case: String,
    pub mode: String,
    pub endpoint: String,
    pub transport: String,
    pub model: String,
    pub service_tier: String,
    pub reasoning_effort: String,
    pub verbosity: String,
    pub store: bool,
    pub remote_ip: Option<String>,
    pub resolved_ips: Vec<String>,
    pub tls_version: Option<String>,
    pub dns_ms: Option<f64>,
    pub connect_ms: Option<f64>,
    pub tls_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub first_event_ms: Option<f64>,
    pub total_ms: Option<f64>,
    pub status: Option<u16>,
    pub usage_cached_tokens: Option<u64>,
    pub usage_cached_tokens_field: String,
    pub reasoning_tokens: Option<u64>,
    pub input_bytes: Option<u64>,
    pub output_bytes: Option<u64>,
    pub account_id_hash: Option<String>,
    pub tokenproxy_request_id: Option<String>,
    pub upstream_request_id: Option<String>,
    pub error_code: Option<String>,
    pub openai_status_indicator: Option<String>,
    pub openai_status_description: Option<String>,
    pub openai_active_incident_count: Option<u64>,
    pub openai_responses_component_status: Option<String>,
    pub openai_codex_component_status: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProbeMeasurements {
    pub dns_ms: Option<f64>,
    pub connect_ms: Option<f64>,
    pub tls_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
    pub total_ms: Option<f64>,
    pub input_bytes: Option<u64>,
    pub output_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProbeRecordInput {
    pub timestamp_local: String,
    pub timestamp_utc: String,
    pub endpoint: String,
    pub transport: String,
    pub status: Option<u16>,
    pub remote_ip: Option<String>,
    pub resolved_ips: Vec<String>,
    pub tls_version: Option<String>,
    pub measurements: ProbeMeasurements,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkMode {
    Direct,
    ThroughProxy,
}

impl BenchmarkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            BenchmarkMode::Direct => "direct",
            BenchmarkMode::ThroughProxy => "proxy",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageCachedTokensField {
    InputTokensDetails,
    PromptTokensDetails,
    Absent,
}

impl UsageCachedTokensField {
    pub fn as_str(self) -> &'static str {
        match self {
            UsageCachedTokensField::InputTokensDetails => "input_tokens_details",
            UsageCachedTokensField::PromptTokensDetails => "prompt_tokens_details",
            UsageCachedTokensField::Absent => "absent",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowRecordInput {
    pub timestamp_local: String,
    pub timestamp_utc: String,
    pub case: String,
    pub mode: BenchmarkMode,
    pub endpoint: String,
    pub transport: String,
    pub model: String,
    pub service_tier: String,
    pub reasoning_effort: String,
    pub verbosity: String,
    pub store: bool,
    pub status: Option<u16>,
    pub remote_ip: Option<String>,
    pub resolved_ips: Vec<String>,
    pub measurements: ProbeMeasurements,
    pub first_event_ms: Option<f64>,
    pub account_id_hash: Option<String>,
    pub tokenproxy_request_id: Option<String>,
    pub upstream_request_id: Option<String>,
    pub usage_cached_tokens: Option<u64>,
    pub usage_cached_tokens_field: UsageCachedTokensField,
    pub reasoning_tokens: Option<u64>,
    pub error_code: Option<String>,
}

impl BenchmarkRecord {
    pub fn probe(input: ProbeRecordInput) -> Self {
        Self {
            schema: BENCHMARK_SCHEMA,
            timestamp_local: input.timestamp_local,
            timestamp_utc: input.timestamp_utc,
            case: "openai_endpoint_probe".to_string(),
            mode: "direct".to_string(),
            endpoint: input.endpoint,
            transport: input.transport,
            model: "unknown".to_string(),
            service_tier: "unknown".to_string(),
            reasoning_effort: "unset".to_string(),
            verbosity: "unset".to_string(),
            store: false,
            remote_ip: input.remote_ip,
            resolved_ips: input.resolved_ips,
            tls_version: input.tls_version,
            dns_ms: input.measurements.dns_ms,
            connect_ms: input.measurements.connect_ms,
            tls_ms: input.measurements.tls_ms,
            ttfb_ms: input.measurements.ttfb_ms,
            first_event_ms: None,
            total_ms: input.measurements.total_ms,
            status: input.status,
            usage_cached_tokens: Some(0),
            usage_cached_tokens_field: "absent".to_string(),
            reasoning_tokens: Some(0),
            input_bytes: input.measurements.input_bytes,
            output_bytes: input.measurements.output_bytes,
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            error_code: input.error_code,
            openai_status_indicator: None,
            openai_status_description: None,
            openai_active_incident_count: None,
            openai_responses_component_status: None,
            openai_codex_component_status: None,
        }
    }

    pub fn workflow(input: WorkflowRecordInput) -> Self {
        Self {
            schema: BENCHMARK_SCHEMA,
            timestamp_local: input.timestamp_local,
            timestamp_utc: input.timestamp_utc,
            case: input.case,
            mode: input.mode.as_str().to_string(),
            endpoint: input.endpoint,
            transport: input.transport,
            model: input.model,
            service_tier: input.service_tier,
            reasoning_effort: input.reasoning_effort,
            verbosity: input.verbosity,
            store: input.store,
            remote_ip: input.remote_ip,
            resolved_ips: input.resolved_ips,
            tls_version: None,
            dns_ms: input.measurements.dns_ms,
            connect_ms: input.measurements.connect_ms,
            tls_ms: input.measurements.tls_ms,
            ttfb_ms: input.measurements.ttfb_ms,
            first_event_ms: input.first_event_ms,
            total_ms: input.measurements.total_ms,
            status: input.status,
            usage_cached_tokens: input.usage_cached_tokens,
            usage_cached_tokens_field: input.usage_cached_tokens_field.as_str().to_string(),
            reasoning_tokens: input.reasoning_tokens,
            input_bytes: input.measurements.input_bytes,
            output_bytes: input.measurements.output_bytes,
            account_id_hash: input.account_id_hash,
            tokenproxy_request_id: input.tokenproxy_request_id,
            upstream_request_id: input.upstream_request_id,
            error_code: input.error_code,
            openai_status_indicator: None,
            openai_status_description: None,
            openai_active_incident_count: None,
            openai_responses_component_status: None,
            openai_codex_component_status: None,
        }
    }

    pub fn attach_openai_status_context(&mut self, context: OpenAiStatusContext) {
        self.openai_status_indicator = context.indicator;
        self.openai_status_description = context.description;
        self.openai_active_incident_count = context.active_incident_count;
        self.openai_responses_component_status = context.responses_component_status;
        self.openai_codex_component_status = context.codex_component_status;
    }
}

pub fn record_jsonl_line(record: &BenchmarkRecord) -> String {
    let mut line = serde_json::to_string(record).expect("benchmark record serializes");
    line.push('\n');
    line
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BenchmarkSummary {
    pub schema_version: &'static str,
    pub run_id: String,
    pub timestamp_local: String,
    pub timestamp_utc: String,
    pub environment: BenchmarkEnvironment,
    pub case: BenchmarkCase,
    pub network_observation: NetworkObservation,
    pub summary: BenchmarkRunSummary,
    pub decision: String,
    pub sample_count: usize,
    pub error_count: usize,
    pub total_ms: Option<DistributionSummary>,
    pub first_event_ms: Option<DistributionSummary>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BenchmarkEnvironment {
    pub os: String,
    pub kernel: String,
    pub cpu: String,
    pub network: String,
    pub region_hint: String,
    pub tokenproxy_git_sha: String,
    pub rustc: String,
    pub curl: String,
    pub submodules: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BenchmarkCase {
    pub name: String,
    pub endpoint: String,
    pub transport: String,
    pub model_family: String,
    pub service_tier: String,
    pub reasoning_effort: String,
    pub verbosity: String,
    pub store: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct NetworkObservation {
    pub resolved_ips: Vec<String>,
    pub alpn: String,
    pub tls_version: Option<String>,
    pub dns_ms: Option<f64>,
    pub connect_ms: Option<f64>,
    pub tls_ms: Option<f64>,
    pub ttfb_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BenchmarkRunSummary {
    pub samples: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub first_event_p50_ms: Option<f64>,
    pub first_event_p95_ms: Option<f64>,
    pub first_event_p99_ms: Option<f64>,
    pub cached_input_tokens: u64,
    pub proxy_overhead_p50_ms: Option<f64>,
    pub proxy_overhead_p99_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DistributionSummary {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

pub fn summarize_records(
    run_id: &str,
    timestamp_local: &str,
    timestamp_utc: &str,
    records: &[BenchmarkRecord],
) -> BenchmarkSummary {
    let mut totals = records
        .iter()
        .filter_map(|record| record.total_ms)
        .collect::<Vec<_>>();
    totals.sort_by(f64::total_cmp);
    let mut first_events = records
        .iter()
        .filter_map(|record| record.first_event_ms)
        .collect::<Vec<_>>();
    first_events.sort_by(f64::total_cmp);
    BenchmarkSummary {
        schema_version: BENCHMARK_SCHEMA,
        run_id: run_id.to_string(),
        timestamp_local: timestamp_local.to_string(),
        timestamp_utc: timestamp_utc.to_string(),
        environment: BenchmarkEnvironment {
            os: build_info::os().to_string(),
            kernel: build_info::kernel().to_string(),
            cpu: build_info::cpu().to_string(),
            network: runtime_env_or_unknown("TOKENPROXY_PROBE_NETWORK"),
            region_hint: runtime_env_or_unknown("TOKENPROXY_REGION_HINT"),
            tokenproxy_git_sha: build_info::git_sha().to_string(),
            rustc: build_info::rustc_version().to_string(),
            curl: build_info::curl_version().to_string(),
            submodules: build_info::submodule_status().to_string(),
        },
        case: benchmark_case(records),
        network_observation: network_observation(records),
        summary: benchmark_run_summary(records.len(), &totals, &first_events, records),
        decision: benchmark_decision(records),
        sample_count: records.len(),
        error_count: records
            .iter()
            .filter(|record| record.error_code.is_some())
            .count(),
        total_ms: distribution(&totals),
        first_event_ms: distribution(&first_events),
    }
}

fn benchmark_case(records: &[BenchmarkRecord]) -> BenchmarkCase {
    records.first().map_or_else(
        || BenchmarkCase {
            name: "unknown".to_string(),
            endpoint: "unknown".to_string(),
            transport: "unknown".to_string(),
            model_family: "unknown".to_string(),
            service_tier: "unknown".to_string(),
            reasoning_effort: "unset".to_string(),
            verbosity: "unset".to_string(),
            store: false,
        },
        |record| BenchmarkCase {
            name: record.case.clone(),
            endpoint: record.endpoint.clone(),
            transport: record.transport.clone(),
            model_family: benchmark_model_family(&record.model),
            service_tier: record.service_tier.clone(),
            reasoning_effort: record.reasoning_effort.clone(),
            verbosity: record.verbosity.clone(),
            store: record.store,
        },
    )
}

fn benchmark_model_family(model: &str) -> String {
    model_family_label(model)
}

fn network_observation(records: &[BenchmarkRecord]) -> NetworkObservation {
    let resolved_ips = records
        .iter()
        .flat_map(|record| {
            record
                .resolved_ips
                .iter()
                .cloned()
                .chain(record.remote_ip.clone())
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let first = records.first();
    NetworkObservation {
        resolved_ips,
        alpn: first
            .map(|record| alpn_from_transport(&record.transport))
            .unwrap_or("unknown")
            .to_string(),
        tls_version: first.and_then(|record| record.tls_version.clone()),
        dns_ms: first.and_then(|record| record.dns_ms),
        connect_ms: first.and_then(|record| record.connect_ms),
        tls_ms: first.and_then(|record| record.tls_ms),
        ttfb_ms: first.and_then(|record| record.ttfb_ms),
    }
}

fn alpn_from_transport(transport: &str) -> &'static str {
    match transport {
        "http2" => "h2",
        "http1" => "http/1.1",
        "http3" => "h3",
        _ => "unknown",
    }
}

fn benchmark_run_summary(
    samples: usize,
    sorted_total_ms: &[f64],
    sorted_first_event_ms: &[f64],
    records: &[BenchmarkRecord],
) -> BenchmarkRunSummary {
    let proxy_overhead = proxy_overhead_summary(records);
    BenchmarkRunSummary {
        samples,
        p50_ms: percentile_option(sorted_total_ms, 50.0),
        p95_ms: percentile_option(sorted_total_ms, 95.0),
        p99_ms: percentile_option(sorted_total_ms, 99.0),
        first_event_p50_ms: percentile_option(sorted_first_event_ms, 50.0),
        first_event_p95_ms: percentile_option(sorted_first_event_ms, 95.0),
        first_event_p99_ms: percentile_option(sorted_first_event_ms, 99.0),
        cached_input_tokens: records
            .iter()
            .filter_map(|record| record.usage_cached_tokens)
            .sum(),
        proxy_overhead_p50_ms: proxy_overhead.p50_ms,
        proxy_overhead_p99_ms: proxy_overhead.p99_ms,
    }
}

#[derive(Debug, Default, PartialEq)]
struct ProxyOverheadSummary {
    p50_ms: Option<f64>,
    p99_ms: Option<f64>,
}

fn proxy_overhead_summary(records: &[BenchmarkRecord]) -> ProxyOverheadSummary {
    let mut direct = records
        .iter()
        .filter(|record| record.mode == "direct")
        .filter_map(|record| record.total_ms)
        .collect::<Vec<_>>();
    let mut proxy = records
        .iter()
        .filter(|record| record.mode == "proxy")
        .filter_map(|record| record.total_ms)
        .collect::<Vec<_>>();
    direct.sort_by(f64::total_cmp);
    proxy.sort_by(f64::total_cmp);

    let Some(direct_p50) = percentile_option(&direct, 50.0) else {
        return ProxyOverheadSummary::default();
    };
    let Some(proxy_p50) = percentile_option(&proxy, 50.0) else {
        return ProxyOverheadSummary::default();
    };
    let Some(direct_p99) = percentile_option(&direct, 99.0) else {
        return ProxyOverheadSummary::default();
    };
    let Some(proxy_p99) = percentile_option(&proxy, 99.0) else {
        return ProxyOverheadSummary::default();
    };

    ProxyOverheadSummary {
        p50_ms: Some(proxy_p50 - direct_p50),
        p99_ms: Some(proxy_p99 - direct_p99),
    }
}

fn percentile_option(sorted_values: &[f64], percentile_rank: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    Some(percentile(sorted_values, percentile_rank))
}

fn benchmark_decision(records: &[BenchmarkRecord]) -> String {
    match records.first() {
        Some(record) if record.case == "openai_endpoint_probe" => {
            "record endpoint probe latency; no proxy overhead decision from direct probe"
                .to_string()
        }
        Some(record) => format!(
            "record benchmark case {} in {} mode",
            record.case, record.mode
        ),
        None => "no benchmark records captured".to_string(),
    }
}

fn runtime_env_or_unknown(key: &str) -> String {
    std::env::var(key)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn summary_json(summary: &BenchmarkSummary) -> String {
    serde_json::to_string_pretty(summary).expect("benchmark summary serializes")
}

fn distribution(values: &[f64]) -> Option<DistributionSummary> {
    if values.is_empty() {
        return None;
    }
    Some(DistributionSummary {
        min: values[0],
        p50: percentile(values, 50.0),
        p95: percentile(values, 95.0),
        p99: percentile(values, 99.0),
        max: values[values.len() - 1],
    })
}

fn percentile(sorted_values: &[f64], percentile: f64) -> f64 {
    let rank = ((percentile / 100.0) * (sorted_values.len() as f64)).ceil() as usize;
    sorted_values[rank.saturating_sub(1).min(sorted_values.len() - 1)]
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenAiStatusContext {
    pub indicator: Option<String>,
    pub description: Option<String>,
    pub active_incident_count: Option<u64>,
    pub responses_component_status: Option<String>,
    pub codex_component_status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusPayload {
    status: Option<StatusIndicator>,
}

#[derive(Debug, Deserialize)]
struct StatusIndicator {
    indicator: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SummaryPayload {
    incidents: Option<Vec<StatusIncident>>,
    components: Option<Vec<StatusComponent>>,
}

#[derive(Debug, Deserialize)]
struct StatusIncident {
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusComponent {
    name: Option<String>,
    status: Option<String>,
}

pub fn parse_openai_status_context(
    status_json: &[u8],
    summary_json: &[u8],
) -> Result<OpenAiStatusContext, serde_json::Error> {
    let status = serde_json::from_slice::<StatusPayload>(status_json)?;
    let summary = serde_json::from_slice::<SummaryPayload>(summary_json)?;
    let active_incident_count = summary.incidents.as_ref().map(|incidents| {
        incidents
            .iter()
            .filter(|incident| {
                !matches!(incident.status.as_deref(), Some("resolved" | "postmortem"))
            })
            .count() as u64
    });
    let responses_component_status = component_status(&summary, "Responses");
    let codex_component_status = component_status(&summary, "Codex API");

    Ok(OpenAiStatusContext {
        indicator: status
            .status
            .as_ref()
            .and_then(|status| status.indicator.clone()),
        description: status.status.and_then(|status| status.description),
        active_incident_count,
        responses_component_status,
        codex_component_status,
    })
}

fn component_status(summary: &SummaryPayload, component_name: &str) -> Option<String> {
    summary.components.as_ref().and_then(|components| {
        components
            .iter()
            .find(|component| component.name.as_deref() == Some(component_name))
            .and_then(|component| component.status.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_input(total_ms: f64, error_code: Option<&str>) -> ProbeRecordInput {
        ProbeRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            status: Some(401),
            remote_ip: None,
            resolved_ips: Vec::new(),
            tls_version: None,
            measurements: ProbeMeasurements {
                total_ms: Some(total_ms),
                input_bytes: Some(0),
                output_bytes: Some(0),
                ..ProbeMeasurements::default()
            },
            error_code: error_code.map(ToOwned::to_owned),
        }
    }

    fn workflow_record(case: &str, mode: BenchmarkMode, total_ms: f64) -> BenchmarkRecord {
        BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: case.to_string(),
            mode,
            endpoint: "/v1/responses".to_string(),
            transport: "sse".to_string(),
            model: "gpt-5.5".to_string(),
            service_tier: "default".to_string(),
            reasoning_effort: "low".to_string(),
            verbosity: "low".to_string(),
            store: false,
            status: Some(200),
            remote_ip: None,
            resolved_ips: Vec::new(),
            measurements: ProbeMeasurements {
                total_ms: Some(total_ms),
                ..ProbeMeasurements::default()
            },
            first_event_ms: None,
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            usage_cached_tokens: None,
            usage_cached_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: None,
            error_code: None,
        })
    }

    #[test]
    fn should_render_probe_record_as_one_jsonl_line_with_required_schema() {
        let record = BenchmarkRecord::probe(ProbeRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            status: Some(401),
            remote_ip: Some("172.66.0.243".to_string()),
            resolved_ips: Vec::new(),
            tls_version: Some("TLSv1.3".to_string()),
            measurements: ProbeMeasurements {
                dns_ms: Some(1.464),
                connect_ms: Some(9.631),
                tls_ms: Some(21.938),
                ttfb_ms: Some(93.092),
                total_ms: Some(93.182),
                input_bytes: Some(0),
                output_bytes: Some(0),
            },
            error_code: None,
        });

        let line = record_jsonl_line(&record);
        assert!(line.ends_with('\n'));
        let json: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(json["schema"], BENCHMARK_SCHEMA);
        assert_eq!(json["timestamp_local"], "2026-05-27T05:00:00-07:00");
        assert_eq!(json["timestamp_utc"], "2026-05-27T12:00:00Z");
        assert_eq!(json["case"], "openai_endpoint_probe");
        assert_eq!(json["endpoint"], "/v1/models");
        assert_eq!(json["transport"], "http2");
        assert_eq!(json["tls_version"], "TLSv1.3");
        assert_eq!(json["status"], 401);
        assert_eq!(json["dns_ms"], 1.464);
        assert_eq!(json["connect_ms"], 9.631);
        assert_eq!(json["tls_ms"], 21.938);
        assert_eq!(json["ttfb_ms"], 93.092);
        assert_eq!(json["usage_cached_tokens_field"], "absent");
        assert!(json["openai_status_indicator"].is_null());
    }

    #[test]
    fn should_render_workflow_record_with_direct_or_proxy_mode() {
        let record = BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: "models_http_get".to_string(),
            mode: BenchmarkMode::ThroughProxy,
            endpoint: "/v1/models".to_string(),
            transport: "http1".to_string(),
            model: "gpt-5.5".to_string(),
            service_tier: "default".to_string(),
            reasoning_effort: "unset".to_string(),
            verbosity: "unset".to_string(),
            store: false,
            status: Some(200),
            remote_ip: Some("127.0.0.1".to_string()),
            resolved_ips: vec!["127.0.0.1".to_string()],
            measurements: ProbeMeasurements {
                ttfb_ms: Some(2.0),
                total_ms: Some(3.0),
                input_bytes: Some(0),
                output_bytes: Some(42),
                ..ProbeMeasurements::default()
            },
            first_event_ms: Some(2.5),
            account_id_hash: Some("acct_hash".to_string()),
            tokenproxy_request_id: Some("tp_1".to_string()),
            upstream_request_id: Some("req_1".to_string()),
            usage_cached_tokens: Some(0),
            usage_cached_tokens_field: UsageCachedTokensField::InputTokensDetails,
            reasoning_tokens: Some(0),
            error_code: None,
        });

        let line = record_jsonl_line(&record);
        let json: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(json["case"], "models_http_get");
        assert_eq!(json["mode"], "proxy");
        assert_eq!(json["endpoint"], "/v1/models");
        assert_eq!(json["account_id_hash"], "acct_hash");
        assert_eq!(json["tokenproxy_request_id"], "tp_1");
        assert_eq!(json["upstream_request_id"], "req_1");
        assert_eq!(json["first_event_ms"], 2.5);
        assert_eq!(json["usage_cached_tokens_field"], "input_tokens_details");
    }

    #[test]
    fn should_mark_workflow_cached_token_field_absent_when_usage_is_not_observed() {
        let record = BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: "models_http_get".to_string(),
            mode: BenchmarkMode::Direct,
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            model: "unknown".to_string(),
            service_tier: "unknown".to_string(),
            reasoning_effort: "unset".to_string(),
            verbosity: "unset".to_string(),
            store: false,
            status: Some(401),
            remote_ip: None,
            resolved_ips: Vec::new(),
            measurements: ProbeMeasurements::default(),
            first_event_ms: None,
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            usage_cached_tokens: None,
            usage_cached_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: None,
            error_code: None,
        });

        let json: serde_json::Value = serde_json::from_str(&record_jsonl_line(&record)).unwrap();

        assert!(json["usage_cached_tokens"].is_null());
        assert_eq!(json["usage_cached_tokens_field"], "absent");
    }

    #[test]
    fn should_summarize_proxy_workflow_case_without_calling_it_probe() {
        let record = BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: "models_http_get".to_string(),
            mode: BenchmarkMode::ThroughProxy,
            endpoint: "/v1/models".to_string(),
            transport: "http1".to_string(),
            model: "gpt-5.5".to_string(),
            service_tier: "default".to_string(),
            reasoning_effort: "unset".to_string(),
            verbosity: "unset".to_string(),
            store: false,
            status: Some(200),
            remote_ip: None,
            resolved_ips: Vec::new(),
            measurements: ProbeMeasurements {
                total_ms: Some(3.0),
                ..ProbeMeasurements::default()
            },
            first_event_ms: None,
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            usage_cached_tokens: Some(0),
            usage_cached_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: Some(0),
            error_code: None,
        });

        let summary = summarize_records(
            "run",
            "2026-05-27T05:00:00-07:00",
            "2026-05-27T12:00:00Z",
            &[record],
        );

        assert_eq!(summary.case.name, "models_http_get");
        assert_eq!(
            summary.decision,
            "record benchmark case models_http_get in proxy mode"
        );
    }

    #[test]
    fn should_render_probe_record_with_openai_status_context() {
        let mut record = BenchmarkRecord::probe(probe_input(42.0, None));
        record.attach_openai_status_context(OpenAiStatusContext {
            indicator: Some("minor".to_string()),
            description: Some("Partial System Degradation".to_string()),
            active_incident_count: Some(1),
            responses_component_status: Some("operational".to_string()),
            codex_component_status: Some("operational".to_string()),
        });

        let json: serde_json::Value = serde_json::from_str(&record_jsonl_line(&record)).unwrap();

        assert_eq!(json["openai_status_indicator"], "minor");
        assert_eq!(
            json["openai_status_description"],
            "Partial System Degradation"
        );
        assert_eq!(json["openai_active_incident_count"], 1);
        assert_eq!(json["openai_responses_component_status"], "operational");
        assert_eq!(json["openai_codex_component_status"], "operational");
    }

    #[test]
    fn should_parse_openai_status_context_from_public_status_payloads() {
        let status =
            br#"{"status":{"indicator":"minor","description":"Partial System Degradation"}}"#;
        let summary = br#"{"incidents":[{"status":"monitoring"},{"status":"resolved"}],"components":[{"name":"Responses","status":"operational"},{"name":"Codex API","status":"partial_outage"}]}"#;

        let context = parse_openai_status_context(status, summary).unwrap();

        assert_eq!(context.indicator.as_deref(), Some("minor"));
        assert_eq!(
            context.description.as_deref(),
            Some("Partial System Degradation")
        );
        assert_eq!(context.active_incident_count, Some(1));
        assert_eq!(
            context.responses_component_status.as_deref(),
            Some("operational")
        );
        assert_eq!(
            context.codex_component_status.as_deref(),
            Some("partial_outage")
        );
    }

    #[test]
    fn should_render_probe_record_with_passive_timing_fields() {
        let record = BenchmarkRecord::probe(ProbeRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            status: Some(401),
            remote_ip: Some("172.66.0.243".to_string()),
            resolved_ips: Vec::new(),
            tls_version: Some("TLSv1.3".to_string()),
            measurements: ProbeMeasurements {
                dns_ms: Some(1.4),
                connect_ms: Some(8.1),
                tls_ms: Some(20.2),
                ttfb_ms: Some(90.3),
                total_ms: Some(91.0),
                input_bytes: Some(0),
                output_bytes: Some(128),
            },
            error_code: None,
        });

        let json: serde_json::Value = serde_json::from_str(&record_jsonl_line(&record)).unwrap();
        assert_eq!(json["dns_ms"], 1.4);
        assert_eq!(json["connect_ms"], 8.1);
        assert_eq!(json["tls_ms"], 20.2);
        assert_eq!(json["ttfb_ms"], 90.3);
        assert_eq!(json["total_ms"], 91.0);
        assert_eq!(json["input_bytes"], 0);
        assert_eq!(json["output_bytes"], 128);
    }

    #[test]
    fn should_summarize_total_duration_distribution_and_errors() {
        let mut first_input = probe_input(10.0, None);
        first_input.endpoint = "/v1/responses".to_string();
        let mut second_input = probe_input(30.0, Some("connect_error"));
        second_input.timestamp_local = "2026-05-27T05:00:01-07:00".to_string();
        second_input.timestamp_utc = "2026-05-27T12:00:01Z".to_string();
        second_input.endpoint = "/v1/responses".to_string();
        second_input.status = None;
        let mut first = BenchmarkRecord::probe(first_input);
        let second = BenchmarkRecord::probe(second_input);
        first.total_ms = Some(20.0);

        let summary = summarize_records(
            "run-1",
            "2026-05-27T05:00:02-07:00",
            "2026-05-27T12:00:02Z",
            &[first, second],
        );

        assert_eq!(summary.schema_version, BENCHMARK_SCHEMA);
        assert_eq!(summary.timestamp_local, "2026-05-27T05:00:02-07:00");
        assert_eq!(summary.timestamp_utc, "2026-05-27T12:00:02Z");
        assert_eq!(summary.sample_count, 2);
        assert_eq!(summary.error_count, 1);
        assert_eq!(
            summary.total_ms,
            Some(DistributionSummary {
                min: 20.0,
                p50: 20.0,
                p95: 30.0,
                p99: 30.0,
                max: 30.0,
            })
        );
        let json = summary_json(&summary);
        assert!(json.contains(r#""schema_version": "tokenproxy.benchmark.v1""#));
    }

    #[test]
    fn should_summarize_first_event_duration_distribution() {
        let first = BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: "responses_sse".to_string(),
            mode: BenchmarkMode::Direct,
            endpoint: "/v1/responses".to_string(),
            transport: "sse".to_string(),
            model: "gpt-5.5".to_string(),
            service_tier: "default".to_string(),
            reasoning_effort: "low".to_string(),
            verbosity: "low".to_string(),
            store: false,
            status: Some(200),
            remote_ip: None,
            resolved_ips: Vec::new(),
            measurements: ProbeMeasurements {
                total_ms: Some(20.0),
                ..ProbeMeasurements::default()
            },
            first_event_ms: Some(5.0),
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            usage_cached_tokens: None,
            usage_cached_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: None,
            error_code: None,
        });
        let mut second = first.clone();
        second.total_ms = Some(40.0);
        second.first_event_ms = Some(15.0);
        second.timestamp_utc = "2026-05-27T12:00:01Z".to_string();

        let summary = summarize_records(
            "run-1",
            "2026-05-27T05:00:02-07:00",
            "2026-05-27T12:00:02Z",
            &[first, second],
        );

        assert_eq!(
            summary.first_event_ms,
            Some(DistributionSummary {
                min: 5.0,
                p50: 5.0,
                p95: 15.0,
                p99: 15.0,
                max: 15.0,
            })
        );
        assert_eq!(summary.summary.first_event_p50_ms, Some(5.0));
        assert_eq!(summary.summary.first_event_p95_ms, Some(15.0));
        assert_eq!(summary.summary.first_event_p99_ms, Some(15.0));
    }

    #[test]
    fn should_summarize_proxy_overhead_when_direct_and_proxy_records_are_present() {
        let direct_fast = workflow_record("direct", BenchmarkMode::Direct, 10.0);
        let direct_slow = workflow_record("direct", BenchmarkMode::Direct, 30.0);
        let proxy_fast = workflow_record("proxy", BenchmarkMode::ThroughProxy, 12.0);
        let proxy_slow = workflow_record("proxy", BenchmarkMode::ThroughProxy, 38.0);

        let summary = summarize_records(
            "run-1",
            "2026-05-27T05:00:02-07:00",
            "2026-05-27T12:00:02Z",
            &[direct_fast, direct_slow, proxy_fast, proxy_slow],
        );

        assert_eq!(summary.summary.proxy_overhead_p50_ms, Some(2.0));
        assert_eq!(summary.summary.proxy_overhead_p99_ms, Some(8.0));
    }

    #[test]
    fn should_include_build_environment_in_summary_json() {
        let record = BenchmarkRecord::probe(probe_input(42.0, None));

        let summary = summarize_records(
            "2026-05-27T12-00-00Z-local",
            "2026-05-27T05:00:00-07:00",
            "2026-05-27T12:00:00Z",
            &[record],
        );
        let json = summary_json(&summary);

        assert!(json.contains(r#""environment""#));
        assert!(json.contains(r#""os""#));
        assert!(json.contains(r#""kernel""#));
        assert!(json.contains(r#""cpu""#));
        assert!(json.contains(r#""network""#));
        assert!(json.contains(r#""region_hint""#));
        assert!(json.contains(r#""tokenproxy_git_sha""#));
        assert!(json.contains(r#""rustc""#));
        assert!(json.contains(r#""curl""#));
    }

    #[test]
    fn should_include_case_network_observation_and_decision_in_summary_json() {
        let record = BenchmarkRecord::probe(ProbeRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            status: Some(401),
            remote_ip: Some("172.66.0.243".to_string()),
            resolved_ips: vec!["162.159.140.245".to_string(), "172.66.0.243".to_string()],
            tls_version: Some("TLSv1.3".to_string()),
            measurements: ProbeMeasurements {
                dns_ms: Some(1.0),
                connect_ms: Some(2.0),
                tls_ms: Some(3.0),
                ttfb_ms: Some(4.0),
                total_ms: Some(5.0),
                input_bytes: Some(0),
                output_bytes: Some(128),
            },
            error_code: None,
        });

        let summary = summarize_records(
            "2026-05-27T12-00-00Z-local",
            "2026-05-27T05:00:00-07:00",
            "2026-05-27T12:00:00Z",
            &[record],
        );
        let json: serde_json::Value = serde_json::from_str(&summary_json(&summary)).unwrap();

        assert_eq!(json["case"]["name"], "openai_endpoint_probe");
        assert_eq!(json["case"]["endpoint"], "/v1/models");
        assert_eq!(json["case"]["transport"], "http2");
        assert_eq!(json["case"]["model_family"], "unknown");
        assert_eq!(json["case"]["store"], false);
        assert_eq!(
            json["network_observation"]["resolved_ips"][0],
            "162.159.140.245"
        );
        assert_eq!(
            json["network_observation"]["resolved_ips"][1],
            "172.66.0.243"
        );
        assert_eq!(json["network_observation"]["alpn"], "h2");
        assert_eq!(json["network_observation"]["tls_version"], "TLSv1.3");
        assert_eq!(json["network_observation"]["dns_ms"], 1.0);
        assert_eq!(json["summary"]["samples"], 1);
        assert_eq!(json["summary"]["p50_ms"], 5.0);
        assert_eq!(json["summary"]["cached_input_tokens"], 0);
        assert!(
            json["decision"]
                .as_str()
                .unwrap()
                .contains("endpoint probe")
        );
    }

    #[test]
    fn should_summarize_gpt_model_versions_by_major_family() {
        let mut record = BenchmarkRecord::probe(probe_input(42.0, None));
        record.model = "gpt-5.5".to_string();

        let summary = summarize_records(
            "2026-05-27T12-00-00Z-local",
            "2026-05-27T05:00:00-07:00",
            "2026-05-27T12:00:00Z",
            &[record],
        );

        assert_eq!(summary.case.model_family, "gpt-5");
    }
}
