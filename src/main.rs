use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use http::Version;
use rustls_pki_types::ServerName;
use tokenproxy::benchmark::{
    BenchmarkMode, BenchmarkRecord, OpenAiStatusContext, ProbeMeasurements, ProbeRecordInput,
    UsageCachedTokensField, WorkflowRecordInput, parse_openai_status_context, record_jsonl_line,
    summarize_records, summary_json,
};
use tokenproxy::config::{
    EffectiveConfig, ProcessEnv, StdFileProvider, load_effective_config, parse_config,
};
use tokenproxy::logging::{
    LogFormat, StartupConfigSummary, StartupLogLine, shutdown_forced_log_line, startup_log_line,
};
use tokenproxy::server::{AppState, app};
use tokenproxy::time_parse::rfc3339_utc_slug;
use tokenproxy::timestamps::now_timestamp_pair;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as UpstreamBenchmarkMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const PROBE_HOST: &str = "api.openai.com";
const PROBE_PORT: u16 = 443;
const PROBE_ENDPOINT: &str = "/v1/models";
const PROBE_URL: &str = "https://api.openai.com/v1/models";
const OPENAI_STATUS_URL: &str = "https://status.openai.com/api/v2/status.json";
const OPENAI_STATUS_SUMMARY_URL: &str = "https://status.openai.com/api/v2/summary.json";
const PROBE_TIMEOUT: Duration = Duration::from_millis(3_000);

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Small Rust proxy for OpenAI-compatible agent traffic"
)]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    bind: Option<std::net::SocketAddr>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    probe_network: bool,
    #[arg(long, requires = "probe_network")]
    probe_auth: bool,
    #[arg(long)]
    probe_artifact_dir: Option<PathBuf>,
    #[arg(long)]
    benchmark: bool,
    #[arg(long, value_enum, default_value_t = BenchmarkModeArg::Direct)]
    benchmark_mode: BenchmarkModeArg,
    #[arg(long, value_enum, default_value_t = BenchmarkWorkflowCaseArg::ModelsHttpGet)]
    benchmark_case: BenchmarkWorkflowCaseArg,
    #[arg(long, default_value_t = 1)]
    benchmark_samples: u16,
    #[arg(long)]
    benchmark_url: Option<String>,
    #[arg(long)]
    benchmark_bearer_env: Option<String>,
    #[arg(long)]
    log_json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchmarkModeArg {
    Direct,
    ThroughProxy,
    Compare,
}

impl BenchmarkModeArg {
    fn as_record_mode(self) -> BenchmarkMode {
        match self {
            BenchmarkModeArg::Direct => BenchmarkMode::Direct,
            BenchmarkModeArg::ThroughProxy => BenchmarkMode::ThroughProxy,
            BenchmarkModeArg::Compare => unreachable!("compare mode expands before sampling"),
        }
    }

    fn default_base_url(self, transport: &str) -> &'static str {
        match (self, transport) {
            (BenchmarkModeArg::Direct, "websocket") => "wss://api.openai.com",
            (BenchmarkModeArg::ThroughProxy, "websocket") => "ws://127.0.0.1:8787",
            (BenchmarkModeArg::Direct, _) => "https://api.openai.com",
            (BenchmarkModeArg::ThroughProxy, _) => "http://127.0.0.1:8787",
            (BenchmarkModeArg::Compare, _) => {
                unreachable!("compare mode expands before URL selection")
            }
        }
    }
}

const DIRECT_BENCHMARK_MODES: [BenchmarkModeArg; 1] = [BenchmarkModeArg::Direct];
const THROUGH_PROXY_BENCHMARK_MODES: [BenchmarkModeArg; 1] = [BenchmarkModeArg::ThroughProxy];
const COMPARE_BENCHMARK_MODES: [BenchmarkModeArg; 2] =
    [BenchmarkModeArg::Direct, BenchmarkModeArg::ThroughProxy];

fn benchmark_modes_to_run(mode: BenchmarkModeArg) -> &'static [BenchmarkModeArg] {
    match mode {
        BenchmarkModeArg::Direct => &DIRECT_BENCHMARK_MODES,
        BenchmarkModeArg::ThroughProxy => &THROUGH_PROXY_BENCHMARK_MODES,
        BenchmarkModeArg::Compare => &COMPARE_BENCHMARK_MODES,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BenchmarkWorkflowCaseArg {
    ModelsHttpGet,
    ChatCompletionsJson,
    ResponsesSse,
    ResponsesHttpToolLoop,
    ResponsesWebsocketToolLoop,
    ResponsesWebsocketContinuationMixedPool,
    CompactedLongAgentSession,
    AccountFailoverBeforeStreamCommit,
    AccountFailureAfterStreamCommit,
    OpenaiStatusIncidentReplay,
}

struct BenchmarkRequestSpec {
    case_name: &'static str,
    method: reqwest::Method,
    endpoint: &'static str,
    transport: &'static str,
    model: &'static str,
    service_tier: &'static str,
    reasoning_effort: &'static str,
    verbosity: &'static str,
    store: bool,
    capture_first_event_ms: bool,
    body: Option<Vec<u8>>,
}

fn benchmark_default_url(mode: BenchmarkModeArg, request_spec: &BenchmarkRequestSpec) -> String {
    format!(
        "{}{}",
        mode.default_base_url(request_spec.transport),
        request_spec.endpoint
    )
}

fn benchmark_request_spec(workflow_case: BenchmarkWorkflowCaseArg) -> BenchmarkRequestSpec {
    match workflow_case {
        BenchmarkWorkflowCaseArg::ModelsHttpGet => BenchmarkRequestSpec {
            case_name: "models_http_get",
            method: reqwest::Method::GET,
            endpoint: "/v1/models",
            transport: "http",
            model: "unknown",
            service_tier: "unknown",
            reasoning_effort: "unset",
            verbosity: "unset",
            store: false,
            capture_first_event_ms: false,
            body: None,
        },
        BenchmarkWorkflowCaseArg::ChatCompletionsJson => BenchmarkRequestSpec {
            case_name: "chat_completions_json",
            method: reqwest::Method::POST,
            endpoint: "/v1/chat/completions",
            transport: "http",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "unset",
            verbosity: "unset",
            store: false,
            capture_first_event_ms: false,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "messages": [
                    {
                        "role": "user",
                        "content": "Return the tokenproxy benchmark fixture string."
                    }
                ],
                "stream": false,
                "service_tier": "default"
            }))),
        },
        BenchmarkWorkflowCaseArg::ResponsesSse => BenchmarkRequestSpec {
            case_name: "responses_sse",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "sse",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "input": "Return the tokenproxy benchmark fixture string.",
                "stream": true,
                "store": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::ResponsesHttpToolLoop => BenchmarkRequestSpec {
            case_name: "responses_http_25_tool_loop",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "sse",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "input": "Run the tokenproxy 25-turn tool-loop benchmark fixture.",
                "tools": [{
                    "type": "function",
                    "name": "tokenproxy_fixture_tool",
                    "description": "Returns deterministic fixture output for benchmark replay.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "turn": {"type": "integer"}
                        },
                        "required": ["turn"],
                        "additionalProperties": false
                    }
                }],
                "tool_choice": "auto",
                "parallel_tool_calls": false,
                "stream": true,
                "store": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::ResponsesWebsocketToolLoop => BenchmarkRequestSpec {
            case_name: "responses_ws_25_tool_loop",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "websocket",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": "Run the tokenproxy 25-turn WebSocket tool-loop benchmark fixture.",
                "tools": [{
                    "type": "function",
                    "name": "tokenproxy_fixture_tool",
                    "description": "Returns deterministic fixture output for benchmark replay.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "turn": {"type": "integer"}
                        },
                        "required": ["turn"],
                        "additionalProperties": false
                    }
                }],
                "tool_choice": "auto",
                "parallel_tool_calls": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::ResponsesWebsocketContinuationMixedPool => BenchmarkRequestSpec {
            case_name: "responses_ws_continuation_mixed_pool",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "websocket",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "previous_response_id": "resp_tokenproxy_fixture",
                "input": [{"type": "message", "role": "user", "content": "Continue the benchmark fixture."}],
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::CompactedLongAgentSession => BenchmarkRequestSpec {
            case_name: "compacted_long_agent_session",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses/compact",
            transport: "http",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: false,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "input": [
                    {"type": "message", "role": "user", "content": "Compact the deterministic long-session benchmark fixture."},
                    {"type": "function_call_output", "call_id": "call_fixture_24", "output": "fixture-output-24"}
                ],
                "store": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::AccountFailoverBeforeStreamCommit => BenchmarkRequestSpec {
            case_name: "account_failover_before_stream_commit",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "sse",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "input": "Trigger the precommit failover benchmark fixture.",
                "stream": true,
                "store": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::AccountFailureAfterStreamCommit => BenchmarkRequestSpec {
            case_name: "account_failure_after_stream_commit",
            method: reqwest::Method::POST,
            endpoint: "/v1/responses",
            transport: "sse",
            model: "gpt-5.5",
            service_tier: "default",
            reasoning_effort: "low",
            verbosity: "low",
            store: false,
            capture_first_event_ms: true,
            body: Some(json_bytes(serde_json::json!({
                "model": "gpt-5.5",
                "input": "Trigger the postcommit stream-failure benchmark fixture.",
                "stream": true,
                "store": false,
                "service_tier": "default",
                "reasoning": {
                    "effort": "low"
                },
                "text": {
                    "verbosity": "low"
                }
            }))),
        },
        BenchmarkWorkflowCaseArg::OpenaiStatusIncidentReplay => BenchmarkRequestSpec {
            case_name: "openai_status_incident_replay",
            method: reqwest::Method::GET,
            endpoint: "/v1/models",
            transport: "http",
            model: "unknown",
            service_tier: "unknown",
            reasoning_effort: "unset",
            verbosity: "unset",
            store: false,
            capture_first_event_ms: false,
            body: None,
        },
    }
}

fn json_bytes(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&value).expect("benchmark request fixture serializes")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.probe_network && !cli.probe_auth {
        run_probe_network(cli.probe_artifact_dir.as_deref(), None).await?;
        return Ok(());
    }

    if cli.benchmark {
        run_http_benchmark(
            cli.probe_artifact_dir.as_deref(),
            cli.benchmark_mode,
            cli.benchmark_case,
            cli.benchmark_samples,
            cli.benchmark_url.as_deref(),
            cli.benchmark_bearer_env.as_deref(),
        )
        .await?;
        return Ok(());
    }

    let config_path = cli
        .config
        .or_else(|| std::env::var_os("TOKENPROXY_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("./tokenproxy.toml"));

    let raw = std::fs::read_to_string(&config_path)?;
    let mut config = parse_config(&raw)?;
    if let Some(bind) = cli.bind {
        config.server.bind = bind;
    }
    let bind = config.server.bind;
    let effective = load_effective_config(config, &ProcessEnv, &StdFileProvider)?;
    let log_format = if cli.log_json {
        LogFormat::Json
    } else {
        LogFormat::Text
    };
    let startup_config = startup_config_summary(&effective);

    if cli.probe_network {
        run_probe_network(
            cli.probe_artifact_dir.as_deref(),
            probe_bearer_token(cli.probe_auth, &effective),
        )
        .await?;
        return Ok(());
    }

    if cli.dry_run {
        let timestamps = now_timestamp_pair();
        println!(
            "{}",
            startup_log_line(StartupLogLine {
                format: log_format,
                event: "config_ok",
                timestamp_local: &timestamps.local,
                timestamp_utc: &timestamps.utc,
                server_id: &effective.config.server.id,
                bind: &bind.to_string(),
                enabled_accounts: effective.accounts.len(),
                config: &startup_config,
            },)
        );
        return Ok(());
    }

    let shutdown_grace = Duration::from_millis(effective.config.server.shutdown_grace_ms);
    let timestamps = now_timestamp_pair();
    eprintln!(
        "{}",
        startup_log_line(StartupLogLine {
            format: log_format,
            event: "listening",
            timestamp_local: &timestamps.local,
            timestamp_utc: &timestamps.utc,
            server_id: &effective.config.server.id,
            bind: &bind.to_string(),
            enabled_accounts: effective.accounts.len(),
            config: &startup_config,
        },)
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state =
        AppState::new_with_log_format_and_shutdown(effective, log_format, shutdown_tx.clone())?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });

    let server = axum::serve(listener, app(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));
    tokio::select! {
        result = server => result?,
        () = force_shutdown_after(shutdown_rx, shutdown_grace) => {
            let timestamps = now_timestamp_pair();
            eprintln!(
                "{}",
                shutdown_forced_log_line(
                    log_format,
                    &timestamps.local,
                    &timestamps.utc,
                    shutdown_grace.as_millis(),
                )
            );
        }
    }
    Ok(())
}

fn startup_config_summary(effective: &EffectiveConfig) -> StartupConfigSummary {
    let model_count = effective
        .accounts
        .iter()
        .flat_map(|account| account.config.models.iter())
        .collect::<BTreeSet<_>>()
        .len();
    let configured_accounts = if effective.config.accounts.is_empty() {
        effective
            .accounts
            .iter()
            .map(|account| account.config.clone())
            .collect::<Vec<_>>()
    } else {
        effective.config.accounts.clone()
    };
    let account_status_labels = configured_accounts
        .iter()
        .map(|account| if account.enabled { "open" } else { "disabled" }.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    StartupConfigSummary {
        max_body_bytes: effective.config.server.max_body_bytes,
        shutdown_grace_ms: effective.config.server.shutdown_grace_ms,
        connect_ms: effective.config.timeouts.connect_ms,
        request_header_ms: effective.config.timeouts.request_header_ms,
        stream_idle_ms: effective.config.timeouts.stream_idle_ms,
        websocket_connect_ms: effective.config.timeouts.websocket_connect_ms,
        websocket_idle_ms: effective.config.timeouts.websocket_idle_ms,
        pool_idle_ms: effective.config.timeouts.pool_idle_ms,
        max_precommit_retries: effective.config.retry.max_precommit_retries,
        honor_retry_after: effective.config.retry.honor_retry_after,
        metrics: effective.config.observability.metrics,
        request_body_dumps: effective.config.observability.request_body_dumps,
        allow_openai_request_headers: effective.config.server.allow_openai_request_headers,
        model_count,
        account_status_labels,
    }
}

async fn wait_for_shutdown(mut shutdown_rx: watch::Receiver<bool>) {
    if *shutdown_rx.borrow() {
        return;
    }
    while shutdown_rx.changed().await.is_ok() {
        if *shutdown_rx.borrow() {
            return;
        }
    }
}

async fn force_shutdown_after(shutdown_rx: watch::Receiver<bool>, grace: Duration) {
    wait_for_shutdown(shutdown_rx).await;
    tokio::time::sleep(grace).await;
}

async fn run_probe_network(
    artifact_dir: Option<&std::path::Path>,
    bearer_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let passive_probe = run_passive_transport_probe(PROBE_HOST, PROBE_PORT).await;
    let client = reqwest::Client::builder()
        .connect_timeout(PROBE_TIMEOUT)
        .timeout(PROBE_TIMEOUT)
        .build()?;
    let status_context = fetch_openai_status_context(&client).await.ok();
    let request_started = Instant::now();
    let mut request = client.get(PROBE_URL);
    if let Some(bearer_token) = bearer_token {
        request = request.bearer_auth(bearer_token);
    }
    let response = request.send().await;
    let response_header_ms = elapsed_ms(request_started);
    let timestamps = now_timestamp_pair();
    let mut record = match response {
        Ok(response) => {
            let status = response.status().as_u16();
            let transport = transport_from_http_version(response.version());
            let response_remote_ip = response
                .remote_addr()
                .map(|address| address.ip().to_string());
            let body = response.bytes().await;
            let total_ms = elapsed_ms(request_started);
            match body {
                Ok(body) => BenchmarkRecord::probe(ProbeRecordInput {
                    timestamp_local: timestamps.local.clone(),
                    timestamp_utc: timestamps.utc.clone(),
                    endpoint: PROBE_ENDPOINT.to_string(),
                    transport: transport.to_string(),
                    status: Some(status),
                    remote_ip: response_remote_ip.or(passive_probe.remote_ip),
                    resolved_ips: passive_probe.resolved_ips,
                    tls_version: passive_probe.tls_version,
                    measurements: ProbeMeasurements {
                        dns_ms: passive_probe.dns_ms,
                        connect_ms: passive_probe.connect_ms,
                        tls_ms: passive_probe.tls_ms,
                        ttfb_ms: Some(response_header_ms),
                        total_ms: Some(total_ms),
                        input_bytes: Some(0),
                        output_bytes: Some(body.len() as u64),
                    },
                    error_code: None,
                }),
                Err(_) => BenchmarkRecord::probe(ProbeRecordInput {
                    timestamp_local: timestamps.local.clone(),
                    timestamp_utc: timestamps.utc.clone(),
                    endpoint: PROBE_ENDPOINT.to_string(),
                    transport: transport.to_string(),
                    status: Some(status),
                    remote_ip: response_remote_ip.or(passive_probe.remote_ip),
                    resolved_ips: passive_probe.resolved_ips,
                    tls_version: passive_probe.tls_version,
                    measurements: ProbeMeasurements {
                        dns_ms: passive_probe.dns_ms,
                        connect_ms: passive_probe.connect_ms,
                        tls_ms: passive_probe.tls_ms,
                        ttfb_ms: Some(response_header_ms),
                        total_ms: Some(total_ms),
                        input_bytes: Some(0),
                        output_bytes: None,
                    },
                    error_code: Some("body_read_error".to_string()),
                }),
            }
        }
        Err(error) => {
            let total_ms = elapsed_ms(request_started);
            BenchmarkRecord::probe(ProbeRecordInput {
                timestamp_local: timestamps.local.clone(),
                timestamp_utc: timestamps.utc.clone(),
                endpoint: PROBE_ENDPOINT.to_string(),
                transport: passive_probe.transport_label().to_string(),
                status: error.status().map(|status| status.as_u16()),
                remote_ip: passive_probe.remote_ip,
                resolved_ips: passive_probe.resolved_ips,
                tls_version: passive_probe.tls_version,
                measurements: ProbeMeasurements {
                    dns_ms: passive_probe.dns_ms,
                    connect_ms: passive_probe.connect_ms,
                    tls_ms: passive_probe.tls_ms,
                    ttfb_ms: None,
                    total_ms: Some(total_ms),
                    input_bytes: Some(0),
                    output_bytes: None,
                },
                error_code: Some(
                    passive_probe
                        .error_code
                        .unwrap_or_else(|| "request_error".to_string()),
                ),
            })
        }
    };
    if let Some(status_context) = status_context {
        record.attach_openai_status_context(status_context);
    }
    let jsonl = record_jsonl_line(&record);
    print!("{jsonl}");
    if let Some(artifact_dir) = artifact_dir {
        write_probe_artifacts(artifact_dir, &record, &jsonl)?;
    }
    Ok(())
}

async fn run_http_benchmark(
    artifact_dir: Option<&std::path::Path>,
    mode: BenchmarkModeArg,
    workflow_case: BenchmarkWorkflowCaseArg,
    samples: u16,
    benchmark_url: Option<&str>,
    bearer_env: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let samples = samples.max(1);
    let request_spec = benchmark_request_spec(workflow_case);
    if mode == BenchmarkModeArg::Compare && benchmark_url.is_some() {
        return Err("compare benchmark mode uses built-in direct and through-proxy URLs; run direct or through-proxy mode when --benchmark-url is set".into());
    }
    let client = reqwest::Client::builder()
        .connect_timeout(PROBE_TIMEOUT)
        .timeout(PROBE_TIMEOUT)
        .build()?;
    let bearer_token = bearer_env.and_then(|key| std::env::var(key).ok());
    let status_context = fetch_openai_status_context(&client).await.ok();
    let modes = benchmark_modes_to_run(mode);
    let mut records = Vec::with_capacity(samples as usize * modes.len());

    for mode_to_run in modes {
        let default_url = benchmark_default_url(*mode_to_run, &request_spec);
        let url = benchmark_url.unwrap_or(default_url.as_str());
        let parsed_url = reqwest::Url::parse(url)?;
        let endpoint = parsed_url.path().to_string();

        for _ in 0..samples {
            let mut record = if request_spec.transport == "websocket" {
                run_one_websocket_benchmark_sample(
                    url,
                    &endpoint,
                    mode_to_run.as_record_mode(),
                    &request_spec,
                    bearer_token.as_deref(),
                )
                .await
            } else {
                run_one_http_benchmark_sample(
                    &client,
                    url,
                    &endpoint,
                    mode_to_run.as_record_mode(),
                    &request_spec,
                    bearer_token.as_deref(),
                )
                .await
            };
            if let Some(status_context) = status_context.clone() {
                record.attach_openai_status_context(status_context);
            }
            records.push(record);
        }
    }

    let jsonl = records
        .iter()
        .map(record_jsonl_line)
        .collect::<Vec<_>>()
        .join("");
    print!("{jsonl}");
    if let Some(artifact_dir) = artifact_dir {
        write_benchmark_artifacts(artifact_dir, &records, &jsonl)?;
    }
    Ok(())
}

async fn run_one_http_benchmark_sample(
    client: &reqwest::Client,
    url: &str,
    endpoint: &str,
    mode: BenchmarkMode,
    request_spec: &BenchmarkRequestSpec,
    bearer_token: Option<&str>,
) -> BenchmarkRecord {
    let request_started = Instant::now();
    let input_bytes = request_spec.body.as_ref().map_or(0, Vec::len) as u64;
    let mut request = client.request(request_spec.method.clone(), url);
    if let Some(bearer_token) = bearer_token {
        request = request.bearer_auth(bearer_token);
    }
    if let Some(body) = &request_spec.body {
        request = request
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body.clone());
    }
    let response = request.send().await;
    let response_header_ms = elapsed_ms(request_started);
    let timestamps = now_timestamp_pair();

    match response {
        Ok(response) => {
            let status = response.status().as_u16();
            let transport = benchmark_transport_label(response.version(), request_spec).to_string();
            let response_remote_ip = response
                .remote_addr()
                .map(|address| address.ip().to_string());
            let upstream_request_id = header_string(response.headers(), "x-request-id")
                .or_else(|| header_string(response.headers(), "openai-request-id"));
            let tokenproxy_request_id =
                header_string(response.headers(), "x-tokenproxy-request-id");
            let body = read_benchmark_response_body(
                response,
                request_started,
                request_spec.capture_first_event_ms,
                request_spec.transport,
            )
            .await;
            let total_ms = elapsed_ms(request_started);
            BenchmarkRecord::workflow(WorkflowRecordInput {
                timestamp_local: timestamps.local,
                timestamp_utc: timestamps.utc,
                case: request_spec.case_name.to_string(),
                mode,
                endpoint: endpoint.to_string(),
                transport,
                model: request_spec.model.to_string(),
                service_tier: request_spec.service_tier.to_string(),
                reasoning_effort: request_spec.reasoning_effort.to_string(),
                verbosity: request_spec.verbosity.to_string(),
                store: request_spec.store,
                status: Some(status),
                remote_ip: response_remote_ip.clone(),
                resolved_ips: response_remote_ip.into_iter().collect(),
                measurements: ProbeMeasurements {
                    ttfb_ms: Some(response_header_ms),
                    total_ms: Some(total_ms),
                    input_bytes: Some(input_bytes),
                    output_bytes: body.output_bytes,
                    ..ProbeMeasurements::default()
                },
                first_event_ms: body.first_event_ms,
                account_id_hash: None,
                tokenproxy_request_id,
                upstream_request_id,
                usage_cached_tokens: body.usage.cached_input_tokens,
                usage_cached_tokens_field: body.usage.cached_input_tokens_field,
                reasoning_tokens: body.usage.reasoning_tokens,
                error_code: body.error_code,
            })
        }
        Err(error) => BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: timestamps.local,
            timestamp_utc: timestamps.utc,
            case: request_spec.case_name.to_string(),
            mode,
            endpoint: endpoint.to_string(),
            transport: request_spec.transport.to_string(),
            model: request_spec.model.to_string(),
            service_tier: request_spec.service_tier.to_string(),
            reasoning_effort: request_spec.reasoning_effort.to_string(),
            verbosity: request_spec.verbosity.to_string(),
            store: request_spec.store,
            status: error.status().map(|status| status.as_u16()),
            remote_ip: None,
            resolved_ips: Vec::new(),
            measurements: ProbeMeasurements {
                total_ms: Some(elapsed_ms(request_started)),
                input_bytes: Some(input_bytes),
                output_bytes: None,
                ..ProbeMeasurements::default()
            },
            first_event_ms: None,
            account_id_hash: None,
            tokenproxy_request_id: None,
            upstream_request_id: None,
            usage_cached_tokens: None,
            usage_cached_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: None,
            error_code: Some("request_error".to_string()),
        }),
    }
}

async fn run_one_websocket_benchmark_sample(
    url: &str,
    endpoint: &str,
    mode: BenchmarkMode,
    request_spec: &BenchmarkRequestSpec,
    bearer_token: Option<&str>,
) -> BenchmarkRecord {
    let request_started = Instant::now();
    let input_bytes = request_spec.body.as_ref().map_or(0, Vec::len) as u64;
    let timestamps = now_timestamp_pair();
    let request = build_websocket_benchmark_request(url, bearer_token);

    let request = match request {
        Ok(request) => request,
        Err(_) => {
            return benchmark_websocket_error_record(
                request_spec,
                endpoint,
                mode,
                BenchmarkWebSocketErrorInput {
                    timestamps,
                    request_started,
                    input_bytes,
                    error_code: "websocket_request_build_error",
                    status: None,
                },
            );
        }
    };

    let connect_result = tokio::time::timeout(PROBE_TIMEOUT, connect_async(request)).await;
    let (mut socket, response) = match connect_result {
        Ok(Ok(connected)) => connected,
        Ok(Err(_)) => {
            return benchmark_websocket_error_record(
                request_spec,
                endpoint,
                mode,
                BenchmarkWebSocketErrorInput {
                    timestamps,
                    request_started,
                    input_bytes,
                    error_code: "websocket_connect_error",
                    status: None,
                },
            );
        }
        Err(_) => {
            return benchmark_websocket_error_record(
                request_spec,
                endpoint,
                mode,
                BenchmarkWebSocketErrorInput {
                    timestamps,
                    request_started,
                    input_bytes,
                    error_code: "websocket_connect_timeout",
                    status: None,
                },
            );
        }
    };

    let connect_ms = elapsed_ms(request_started);
    let status = response.status().as_u16();
    let upstream_request_id = http_header_string(response.headers(), "x-request-id")
        .or_else(|| http_header_string(response.headers(), "openai-request-id"));
    let tokenproxy_request_id = http_header_string(response.headers(), "x-tokenproxy-request-id");

    if let Some(body) = &request_spec.body {
        let payload = String::from_utf8_lossy(body).into_owned();
        if socket
            .send(UpstreamBenchmarkMessage::Text(payload.into()))
            .await
            .is_err()
        {
            return benchmark_websocket_error_record(
                request_spec,
                endpoint,
                mode,
                BenchmarkWebSocketErrorInput {
                    timestamps,
                    request_started,
                    input_bytes,
                    error_code: "websocket_send_error",
                    status: Some(status),
                },
            );
        }
    }

    let body = read_benchmark_websocket_body(&mut socket, request_started).await;
    BenchmarkRecord::workflow(WorkflowRecordInput {
        timestamp_local: timestamps.local,
        timestamp_utc: timestamps.utc,
        case: request_spec.case_name.to_string(),
        mode,
        endpoint: endpoint.to_string(),
        transport: "websocket".to_string(),
        model: request_spec.model.to_string(),
        service_tier: request_spec.service_tier.to_string(),
        reasoning_effort: request_spec.reasoning_effort.to_string(),
        verbosity: request_spec.verbosity.to_string(),
        store: request_spec.store,
        status: Some(status),
        remote_ip: None,
        resolved_ips: Vec::new(),
        measurements: ProbeMeasurements {
            connect_ms: Some(connect_ms),
            ttfb_ms: body.first_event_ms,
            total_ms: Some(elapsed_ms(request_started)),
            input_bytes: Some(input_bytes),
            output_bytes: body.output_bytes,
            ..ProbeMeasurements::default()
        },
        first_event_ms: body.first_event_ms,
        account_id_hash: None,
        tokenproxy_request_id,
        upstream_request_id,
        usage_cached_tokens: body.usage.cached_input_tokens,
        usage_cached_tokens_field: body.usage.cached_input_tokens_field,
        reasoning_tokens: body.usage.reasoning_tokens,
        error_code: body.error_code,
    })
}

fn build_websocket_benchmark_request(
    url: &str,
    bearer_token: Option<&str>,
) -> Result<http::Request<()>, Box<dyn std::error::Error>> {
    let mut request = url.into_client_request()?;
    if let Some(bearer_token) = bearer_token {
        let authorization = format!("Bearer {bearer_token}");
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&authorization)?,
        );
    }
    Ok(request)
}

async fn read_benchmark_websocket_body(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    request_started: Instant,
) -> BenchmarkBodyRead {
    let read_result = tokio::time::timeout(PROBE_TIMEOUT, async {
        let mut output_bytes = 0_u64;
        let mut first_event_ms = None;
        let mut usage = BenchmarkUsageMetadata::default();
        while let Some(message) = socket.next().await {
            let message = message.map_err(|_| "websocket_read_error")?;
            match message {
                UpstreamBenchmarkMessage::Text(text) => {
                    if first_event_ms.is_none() && !text.is_empty() {
                        first_event_ms = Some(elapsed_ms(request_started));
                    }
                    output_bytes = output_bytes.saturating_add(text.len() as u64);
                    usage.merge(benchmark_usage_metadata_from_bytes(
                        text.as_bytes(),
                        "websocket",
                    ));
                    if websocket_benchmark_message_is_terminal(text.as_str()) {
                        break;
                    }
                }
                UpstreamBenchmarkMessage::Binary(bytes) => {
                    if first_event_ms.is_none() && !bytes.is_empty() {
                        first_event_ms = Some(elapsed_ms(request_started));
                    }
                    output_bytes = output_bytes.saturating_add(bytes.len() as u64);
                }
                UpstreamBenchmarkMessage::Close(_) => break,
                UpstreamBenchmarkMessage::Ping(_) | UpstreamBenchmarkMessage::Pong(_) => {}
                UpstreamBenchmarkMessage::Frame(_) => {}
            }
        }
        Ok::<BenchmarkBodyRead, &'static str>(BenchmarkBodyRead {
            output_bytes: Some(output_bytes),
            first_event_ms,
            usage,
            error_code: None,
        })
    })
    .await;

    match read_result {
        Ok(Ok(read)) => read,
        Ok(Err(error_code)) => BenchmarkBodyRead {
            output_bytes: None,
            first_event_ms: None,
            usage: BenchmarkUsageMetadata::default(),
            error_code: Some(error_code.to_string()),
        },
        Err(_) => BenchmarkBodyRead {
            output_bytes: None,
            first_event_ms: None,
            usage: BenchmarkUsageMetadata::default(),
            error_code: Some("websocket_read_timeout".to_string()),
        },
    }
}

fn websocket_benchmark_message_is_terminal(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|event_type| matches!(event_type.as_str(), "response.completed" | "error"))
}

struct BenchmarkWebSocketErrorInput<'a> {
    timestamps: tokenproxy::timestamps::TimestampPair,
    request_started: Instant,
    input_bytes: u64,
    error_code: &'a str,
    status: Option<u16>,
}

fn benchmark_websocket_error_record(
    request_spec: &BenchmarkRequestSpec,
    endpoint: &str,
    mode: BenchmarkMode,
    error: BenchmarkWebSocketErrorInput<'_>,
) -> BenchmarkRecord {
    BenchmarkRecord::workflow(WorkflowRecordInput {
        timestamp_local: error.timestamps.local,
        timestamp_utc: error.timestamps.utc,
        case: request_spec.case_name.to_string(),
        mode,
        endpoint: endpoint.to_string(),
        transport: "websocket".to_string(),
        model: request_spec.model.to_string(),
        service_tier: request_spec.service_tier.to_string(),
        reasoning_effort: request_spec.reasoning_effort.to_string(),
        verbosity: request_spec.verbosity.to_string(),
        store: request_spec.store,
        status: error.status,
        remote_ip: None,
        resolved_ips: Vec::new(),
        measurements: ProbeMeasurements {
            total_ms: Some(elapsed_ms(error.request_started)),
            input_bytes: Some(error.input_bytes),
            output_bytes: None,
            ..ProbeMeasurements::default()
        },
        first_event_ms: None,
        account_id_hash: None,
        tokenproxy_request_id: None,
        upstream_request_id: None,
        usage_cached_tokens: None,
        usage_cached_tokens_field: UsageCachedTokensField::Absent,
        reasoning_tokens: None,
        error_code: Some(error.error_code.to_string()),
    })
}

fn benchmark_transport_label(
    version: reqwest::Version,
    request_spec: &BenchmarkRequestSpec,
) -> &'static str {
    if request_spec.transport == "http" {
        transport_from_http_version(version)
    } else {
        request_spec.transport
    }
}

async fn fetch_openai_status_context(
    client: &reqwest::Client,
) -> Result<OpenAiStatusContext, Box<dyn std::error::Error>> {
    let status = client
        .get(OPENAI_STATUS_URL)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let summary = client
        .get(OPENAI_STATUS_SUMMARY_URL)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    Ok(parse_openai_status_context(&status, &summary)?)
}

struct BenchmarkBodyRead {
    output_bytes: Option<u64>,
    first_event_ms: Option<f64>,
    usage: BenchmarkUsageMetadata,
    error_code: Option<String>,
}

async fn read_benchmark_response_body(
    response: reqwest::Response,
    request_started: Instant,
    capture_first_event_ms: bool,
    transport: &str,
) -> BenchmarkBodyRead {
    let mut output_bytes = 0_u64;
    let mut first_event_ms = None;
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return BenchmarkBodyRead {
                output_bytes: None,
                first_event_ms,
                usage: BenchmarkUsageMetadata::default(),
                error_code: Some("body_read_error".to_string()),
            };
        };
        if capture_first_event_ms && first_event_ms.is_none() && !chunk.is_empty() {
            first_event_ms = Some(elapsed_ms(request_started));
        }
        output_bytes = output_bytes.saturating_add(chunk.len() as u64);
        body.extend_from_slice(&chunk);
    }

    BenchmarkBodyRead {
        output_bytes: Some(output_bytes),
        first_event_ms,
        usage: benchmark_usage_metadata_from_bytes(&body, transport),
        error_code: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchmarkUsageMetadata {
    cached_input_tokens: Option<u64>,
    cached_input_tokens_field: UsageCachedTokensField,
    reasoning_tokens: Option<u64>,
}

impl Default for BenchmarkUsageMetadata {
    fn default() -> Self {
        Self {
            cached_input_tokens: None,
            cached_input_tokens_field: UsageCachedTokensField::Absent,
            reasoning_tokens: None,
        }
    }
}

fn benchmark_usage_metadata_from_bytes(body: &[u8], transport: &str) -> BenchmarkUsageMetadata {
    if transport == "sse" {
        return benchmark_usage_metadata_from_sse(body);
    }

    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .map(|value| benchmark_usage_metadata_from_value(&value))
        .unwrap_or_default()
}

fn benchmark_usage_metadata_from_sse(body: &[u8]) -> BenchmarkUsageMetadata {
    let Ok(text) = std::str::from_utf8(body) else {
        return BenchmarkUsageMetadata::default();
    };
    let mut metadata = BenchmarkUsageMetadata::default();
    for data in text
        .lines()
        .filter_map(|line| line.strip_prefix("data:").map(str::trim))
    {
        if data == "[DONE]" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(data) else {
            continue;
        };
        metadata.merge(benchmark_usage_metadata_from_value(&value));
    }
    metadata
}

fn benchmark_usage_metadata_from_value(value: &serde_json::Value) -> BenchmarkUsageMetadata {
    let Some(usage) = value
        .get("usage")
        .or_else(|| value.pointer("/response/usage"))
    else {
        return BenchmarkUsageMetadata::default();
    };

    let input_cached = usage
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(serde_json::Value::as_u64);
    let prompt_cached = usage
        .pointer("/prompt_tokens_details/cached_tokens")
        .and_then(serde_json::Value::as_u64);
    let cached_input_tokens = input_cached
        .into_iter()
        .chain(prompt_cached)
        .fold(None, |total: Option<u64>, count| {
            Some(total.unwrap_or(0).saturating_add(count))
        });
    let cached_input_tokens_field = if input_cached.is_some() {
        UsageCachedTokensField::InputTokensDetails
    } else if prompt_cached.is_some() {
        UsageCachedTokensField::PromptTokensDetails
    } else {
        UsageCachedTokensField::Absent
    };

    BenchmarkUsageMetadata {
        cached_input_tokens,
        cached_input_tokens_field,
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(serde_json::Value::as_u64),
    }
}

impl BenchmarkUsageMetadata {
    fn merge(&mut self, observed: BenchmarkUsageMetadata) {
        if observed.cached_input_tokens.is_some() {
            self.cached_input_tokens = observed.cached_input_tokens;
            self.cached_input_tokens_field = observed.cached_input_tokens_field;
        }
        if observed.reasoning_tokens.is_some() {
            self.reasoning_tokens = observed.reasoning_tokens;
        }
    }
}

#[derive(Debug, Default)]
struct PassiveProbe {
    remote_ip: Option<String>,
    resolved_ips: Vec<String>,
    tls_version: Option<String>,
    dns_ms: Option<f64>,
    connect_ms: Option<f64>,
    tls_ms: Option<f64>,
    alpn: Option<Vec<u8>>,
    error_code: Option<String>,
}

impl PassiveProbe {
    fn transport_label(&self) -> &'static str {
        transport_from_alpn(self.alpn.as_deref())
    }
}

async fn run_passive_transport_probe(host: &str, port: u16) -> PassiveProbe {
    let mut probe = PassiveProbe::default();
    let dns_started = Instant::now();
    let addresses = match tokio::net::lookup_host((host, port)).await {
        Ok(addresses) => addresses.collect::<Vec<_>>(),
        Err(_) => {
            probe.dns_ms = Some(elapsed_ms(dns_started));
            probe.error_code = Some("dns_error".to_string());
            return probe;
        }
    };
    probe.dns_ms = Some(elapsed_ms(dns_started));
    probe.resolved_ips = addresses
        .iter()
        .map(|address| address.ip().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let Some(address) = addresses.into_iter().next() else {
        probe.error_code = Some("dns_empty".to_string());
        return probe;
    };
    probe.remote_ip = Some(address.ip().to_string());

    let connect_started = Instant::now();
    let stream = match connect_with_timeout(address).await {
        Ok(stream) => stream,
        Err(error_code) => {
            probe.connect_ms = Some(elapsed_ms(connect_started));
            probe.error_code = Some(error_code);
            return probe;
        }
    };
    probe.connect_ms = Some(elapsed_ms(connect_started));

    let tls_started = Instant::now();
    match tls_handshake_with_timeout(host, stream).await {
        Ok(handshake) => {
            probe.tls_ms = Some(elapsed_ms(tls_started));
            probe.tls_version = handshake.tls_version;
            probe.alpn = handshake.alpn;
        }
        Err(error_code) => {
            probe.tls_ms = Some(elapsed_ms(tls_started));
            probe.error_code = Some(error_code);
        }
    }
    probe
}

#[derive(Debug, Default)]
struct TlsHandshakeProbe {
    tls_version: Option<String>,
    alpn: Option<Vec<u8>>,
}

async fn connect_with_timeout(address: SocketAddr) -> Result<TcpStream, String> {
    match tokio::time::timeout(PROBE_TIMEOUT, TcpStream::connect(address)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(_)) => Err("connect_error".to_string()),
        Err(_) => Err("connect_timeout".to_string()),
    }
}

async fn tls_handshake_with_timeout(
    host: &str,
    stream: TcpStream,
) -> Result<TlsHandshakeProbe, String> {
    let connector = TlsConnector::from(Arc::new(tls_client_config()));
    let server_name = ServerName::try_from(host.to_string()).map_err(|_| "tls_name_error")?;
    match tokio::time::timeout(PROBE_TIMEOUT, connector.connect(server_name, stream)).await {
        Ok(Ok(stream)) => {
            let connection = stream.get_ref().1;
            Ok(TlsHandshakeProbe {
                tls_version: connection.protocol_version().map(tls_version_label),
                alpn: connection.alpn_protocol().map(|alpn| alpn.to_vec()),
            })
        }
        Ok(Err(_)) => Err("tls_error".to_string()),
        Err(_) => Err("tls_timeout".to_string()),
    }
}

fn tls_version_label(version: rustls::ProtocolVersion) -> String {
    match version {
        rustls::ProtocolVersion::TLSv1_2 => "TLSv1.2".to_string(),
        rustls::ProtocolVersion::TLSv1_3 => "TLSv1.3".to_string(),
        other => format!("{other:?}"),
    }
}

fn tls_client_config() -> rustls::ClientConfig {
    let root_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    config
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn probe_bearer_token(probe_auth: bool, effective: &EffectiveConfig) -> Option<&str> {
    if probe_auth {
        effective
            .accounts
            .first()
            .map(|account| account.bearer_token.as_str())
    } else {
        None
    }
}

fn write_probe_artifacts(
    artifact_dir: &std::path::Path,
    record: &BenchmarkRecord,
    jsonl: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_named_benchmark_artifacts(artifact_dir, "probe", std::slice::from_ref(record), jsonl)
}

fn write_benchmark_artifacts(
    artifact_dir: &std::path::Path,
    records: &[BenchmarkRecord],
    jsonl: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_named_benchmark_artifacts(artifact_dir, "benchmark", records, jsonl)
}

fn write_named_benchmark_artifacts(
    artifact_dir: &std::path::Path,
    artifact_prefix: &str,
    records: &[BenchmarkRecord],
    jsonl: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(artifact_dir)?;
    let records_path = artifact_dir.join(format!("{artifact_prefix}-records.jsonl"));
    let summary_path = artifact_dir.join(format!("{artifact_prefix}-summary.json"));
    std::fs::write(&records_path, jsonl)?;
    let first_record = records
        .first()
        .ok_or("cannot write empty benchmark artifact")?;
    let run_id = probe_run_id(&first_record.timestamp_utc);
    let summary = summarize_records(
        &run_id,
        &first_record.timestamp_local,
        &first_record.timestamp_utc,
        records,
    );
    std::fs::write(summary_path, summary_json(&summary))?;
    Ok(())
}

fn probe_run_id(timestamp_utc: &str) -> String {
    format!(
        "{}-openai-endpoint-probe",
        rfc3339_utc_slug(timestamp_utc).expect("probe timestamp is generated as RFC3339")
    )
}

fn transport_from_http_version(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 => "http1",
        Version::HTTP_2 => "http2",
        Version::HTTP_3 => "http3",
        _ => "unknown",
    }
}

fn header_string(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    http_header_string(headers, name)
}

fn http_header_string(headers: &http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn transport_from_alpn(alpn: Option<&[u8]>) -> &'static str {
    match alpn {
        Some(b"h2") => "http2",
        Some(b"http/1.1") => "http1",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokenproxy::config::{AccountConfig, Config, EffectiveAccount};

    #[test]
    fn should_render_cargo_package_version_without_config() {
        let err = Cli::try_parse_from(["tokenproxy", "--version"])
            .expect_err("--version should render through clap before config loading");

        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
        assert!(err.to_string().contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn should_accept_probe_auth_only_as_explicit_probe_flag() {
        let cli = Cli::try_parse_from(["tokenproxy", "--probe-network", "--probe-auth"]).unwrap();

        assert!(cli.probe_network);
        assert!(cli.probe_auth);
    }

    #[test]
    fn should_accept_direct_or_proxy_benchmark_mode() {
        let cli = Cli::try_parse_from([
            "tokenproxy",
            "--benchmark",
            "--benchmark-mode",
            "through-proxy",
            "--benchmark-case",
            "chat-completions-json",
            "--benchmark-samples",
            "3",
            "--benchmark-url",
            "http://127.0.0.1:8787/v1/models",
        ])
        .unwrap();

        assert!(cli.benchmark);
        assert_eq!(cli.benchmark_mode, BenchmarkModeArg::ThroughProxy);
        assert_eq!(
            cli.benchmark_case,
            BenchmarkWorkflowCaseArg::ChatCompletionsJson
        );
        assert_eq!(cli.benchmark_samples, 3);
        assert_eq!(
            cli.benchmark_url.as_deref(),
            Some("http://127.0.0.1:8787/v1/models")
        );
    }

    #[test]
    fn should_accept_compare_benchmark_mode_for_paired_overhead_artifacts() {
        let cli = Cli::try_parse_from([
            "tokenproxy",
            "--benchmark",
            "--benchmark-mode",
            "compare",
            "--benchmark-case",
            "chat-completions-json",
            "--benchmark-samples",
            "2",
        ])
        .unwrap();

        assert_eq!(cli.benchmark_mode, BenchmarkModeArg::Compare);
        assert_eq!(
            benchmark_modes_to_run(cli.benchmark_mode),
            &[BenchmarkModeArg::Direct, BenchmarkModeArg::ThroughProxy]
        );
        assert_eq!(cli.benchmark_samples, 2);
    }

    #[test]
    fn should_build_chat_completions_benchmark_request_spec() {
        let spec = benchmark_request_spec(BenchmarkWorkflowCaseArg::ChatCompletionsJson);

        assert_eq!(spec.case_name, "chat_completions_json");
        assert_eq!(spec.method, reqwest::Method::POST);
        assert_eq!(spec.endpoint, "/v1/chat/completions");
        assert_eq!(spec.model, "gpt-5.5");
        assert_eq!(spec.service_tier, "default");
        assert!(!spec.store);
        let body: serde_json::Value =
            serde_json::from_slice(spec.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["model"], "gpt-5.5");
        assert_eq!(body["stream"], false);
        assert_eq!(body["messages"][0]["role"], "user");
    }

    #[test]
    fn should_build_responses_sse_benchmark_request_spec() {
        let spec = benchmark_request_spec(BenchmarkWorkflowCaseArg::ResponsesSse);

        assert_eq!(spec.case_name, "responses_sse");
        assert_eq!(spec.method, reqwest::Method::POST);
        assert_eq!(spec.endpoint, "/v1/responses");
        assert_eq!(spec.model, "gpt-5.5");
        assert_eq!(spec.reasoning_effort, "low");
        assert_eq!(spec.verbosity, "low");
        let body: serde_json::Value =
            serde_json::from_slice(spec.body.as_deref().unwrap()).unwrap();
        assert_eq!(body["stream"], true);
        assert_eq!(body["store"], false);
        assert_eq!(body["reasoning"]["effort"], "low");
        assert_eq!(body["text"]["verbosity"], "low");
    }

    #[test]
    fn should_build_required_agent_workflow_benchmark_request_specs() {
        let cases = [
            (
                BenchmarkWorkflowCaseArg::ResponsesHttpToolLoop,
                "responses_http_25_tool_loop",
                "/v1/responses",
                "sse",
            ),
            (
                BenchmarkWorkflowCaseArg::ResponsesWebsocketToolLoop,
                "responses_ws_25_tool_loop",
                "/v1/responses",
                "websocket",
            ),
            (
                BenchmarkWorkflowCaseArg::ResponsesWebsocketContinuationMixedPool,
                "responses_ws_continuation_mixed_pool",
                "/v1/responses",
                "websocket",
            ),
            (
                BenchmarkWorkflowCaseArg::CompactedLongAgentSession,
                "compacted_long_agent_session",
                "/v1/responses/compact",
                "http",
            ),
            (
                BenchmarkWorkflowCaseArg::AccountFailoverBeforeStreamCommit,
                "account_failover_before_stream_commit",
                "/v1/responses",
                "sse",
            ),
            (
                BenchmarkWorkflowCaseArg::AccountFailureAfterStreamCommit,
                "account_failure_after_stream_commit",
                "/v1/responses",
                "sse",
            ),
            (
                BenchmarkWorkflowCaseArg::OpenaiStatusIncidentReplay,
                "openai_status_incident_replay",
                "/v1/models",
                "http",
            ),
        ];

        for (workflow_case, expected_case_name, expected_endpoint, expected_transport) in cases {
            let spec = benchmark_request_spec(workflow_case);

            assert_eq!(spec.case_name, expected_case_name);
            assert_eq!(spec.endpoint, expected_endpoint);
            assert_eq!(spec.transport, expected_transport);
        }
    }

    #[test]
    fn should_reject_probe_auth_without_probe_network() {
        assert!(Cli::try_parse_from(["tokenproxy", "--probe-auth"]).is_err());
    }

    #[test]
    fn should_map_http_version_to_benchmark_transport_label() {
        assert_eq!(transport_from_http_version(Version::HTTP_11), "http1");
        assert_eq!(transport_from_http_version(Version::HTTP_2), "http2");
        assert_eq!(transport_from_http_version(Version::HTTP_3), "http3");
    }

    #[test]
    fn should_preserve_workflow_transport_for_sse_records() {
        let http = benchmark_request_spec(BenchmarkWorkflowCaseArg::ChatCompletionsJson);
        let sse = benchmark_request_spec(BenchmarkWorkflowCaseArg::ResponsesSse);

        assert_eq!(benchmark_transport_label(Version::HTTP_2, &http), "http2");
        assert_eq!(benchmark_transport_label(Version::HTTP_2, &sse), "sse");
    }

    #[test]
    fn should_map_tls_alpn_to_benchmark_transport_label() {
        assert_eq!(transport_from_alpn(Some(b"h2")), "http2");
        assert_eq!(transport_from_alpn(Some(b"http/1.1")), "http1");
        assert_eq!(transport_from_alpn(None), "unknown");
    }

    #[test]
    fn should_map_tls_protocol_version_to_probe_label() {
        assert_eq!(
            tls_version_label(rustls::ProtocolVersion::TLSv1_2),
            "TLSv1.2"
        );
        assert_eq!(
            tls_version_label(rustls::ProtocolVersion::TLSv1_3),
            "TLSv1.3"
        );
    }

    #[test]
    fn should_extract_benchmark_usage_metadata_from_json_response_body() {
        let metadata = benchmark_usage_metadata_from_bytes(
            br#"{"usage":{"input_tokens_details":{"cached_tokens":17},"output_tokens_details":{"reasoning_tokens":31}}}"#,
            "http",
        );

        assert_eq!(metadata.cached_input_tokens, Some(17));
        assert_eq!(
            metadata.cached_input_tokens_field,
            UsageCachedTokensField::InputTokensDetails
        );
        assert_eq!(metadata.reasoning_tokens, Some(31));
    }

    #[test]
    fn should_extract_benchmark_usage_metadata_from_sse_response_completed_frame() {
        let metadata = benchmark_usage_metadata_from_bytes(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"prompt_tokens_details\":{\"cached_tokens\":23},\"output_tokens_details\":{\"reasoning_tokens\":29}}}}\n\n",
            "sse",
        );

        assert_eq!(metadata.cached_input_tokens, Some(23));
        assert_eq!(
            metadata.cached_input_tokens_field,
            UsageCachedTokensField::PromptTokensDetails
        );
        assert_eq!(metadata.reasoning_tokens, Some(29));
    }

    #[test]
    fn should_build_probe_run_id_from_utc_timestamp() {
        assert_eq!(
            probe_run_id("2026-05-27T12:00:00Z"),
            "2026-05-27T12-00-00-000000000Z-openai-endpoint-probe"
        );
        assert_eq!(
            probe_run_id("2026-05-27T05:00:00.123456789-07:00"),
            "2026-05-27T12-00-00-123456789Z-openai-endpoint-probe"
        );
    }

    #[test]
    fn should_summarize_startup_model_count_and_status_labels() {
        let mut config = Config::default();
        config.accounts = vec![
            effective_account("primary", &["gpt-5.5", "gpt-5.3-codex"]).config,
            effective_account("secondary", &["gpt-5.5", "gpt-5.4"]).config,
            AccountConfig {
                id: "paused".to_string(),
                enabled: false,
                ..AccountConfig::default()
            },
        ];
        let effective = EffectiveConfig {
            config,
            downstream_token: "client".to_string(),
            account_hash_key: "hash-key".to_string(),
            accounts: vec![
                effective_account("primary", &["gpt-5.5", "gpt-5.3-codex"]),
                effective_account("secondary", &["gpt-5.5", "gpt-5.4"]),
            ],
        };

        let summary = startup_config_summary(&effective);

        assert_eq!(summary.model_count, 3);
        assert_eq!(summary.account_status_labels, vec!["disabled", "open"]);
    }

    #[test]
    fn should_write_probe_jsonl_and_summary_artifacts() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let artifact_dir =
            std::env::temp_dir().join(format!("tokenproxy-probe-artifacts-{unique}"));
        let record = BenchmarkRecord::probe(ProbeRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            endpoint: "/v1/models".to_string(),
            transport: "http2".to_string(),
            status: Some(401),
            remote_ip: None,
            resolved_ips: Vec::new(),
            tls_version: None,
            measurements: ProbeMeasurements {
                total_ms: Some(42.0),
                input_bytes: Some(0),
                output_bytes: Some(0),
                ..ProbeMeasurements::default()
            },
            error_code: None,
        });
        let jsonl = record_jsonl_line(&record);

        write_probe_artifacts(&artifact_dir, &record, &jsonl).unwrap();

        let records = std::fs::read_to_string(artifact_dir.join("probe-records.jsonl")).unwrap();
        let summary = std::fs::read_to_string(artifact_dir.join("probe-summary.json")).unwrap();
        assert_eq!(records, jsonl);
        assert!(summary.contains(r#""schema_version": "tokenproxy.benchmark.v1""#));
        assert!(summary.contains(r#""timestamp_local": "2026-05-27T05:00:00-07:00""#));
        assert!(summary.contains(r#""timestamp_utc": "2026-05-27T12:00:00Z""#));
        assert!(summary.contains(r#""sample_count": 1"#));
    }

    #[test]
    fn should_write_benchmark_jsonl_and_summary_artifacts() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let artifact_dir =
            std::env::temp_dir().join(format!("tokenproxy-benchmark-artifacts-{unique}"));
        let record = BenchmarkRecord::workflow(WorkflowRecordInput {
            timestamp_local: "2026-05-27T05:00:00-07:00".to_string(),
            timestamp_utc: "2026-05-27T12:00:00Z".to_string(),
            case: "chat_completions_json".to_string(),
            mode: BenchmarkMode::ThroughProxy,
            endpoint: "/v1/chat/completions".to_string(),
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
                total_ms: Some(42.0),
                input_bytes: Some(128),
                output_bytes: Some(256),
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
        });
        let jsonl = record_jsonl_line(&record);

        write_benchmark_artifacts(&artifact_dir, &[record], &jsonl).unwrap();

        let records =
            std::fs::read_to_string(artifact_dir.join("benchmark-records.jsonl")).unwrap();
        let summary = std::fs::read_to_string(artifact_dir.join("benchmark-summary.json")).unwrap();
        assert_eq!(records, jsonl);
        assert!(summary.contains(r#""name": "chat_completions_json""#));
        assert!(summary.contains(r#""sample_count": 1"#));
    }

    #[tokio::test]
    async fn should_wait_for_shutdown_signal_before_starting_force_timer() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let force_shutdown = force_shutdown_after(shutdown_rx, Duration::from_millis(1));
        tokio::pin!(force_shutdown);

        tokio::select! {
            () = &mut force_shutdown => panic!("force timer completed before shutdown signal"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }

        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_millis(50), &mut force_shutdown)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires binding a local TCP listener for fake SSE benchmark target"]
    async fn should_record_first_event_ms_for_responses_sse_benchmark_case() {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            let body = b"event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens_details\":{\"cached_tokens\":17},\"output_tokens_details\":{\"reasoning_tokens\":31}}}}\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nx-request-id: req_sse\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .connect_timeout(PROBE_TIMEOUT)
            .timeout(PROBE_TIMEOUT)
            .build()
            .unwrap();
        let spec = benchmark_request_spec(BenchmarkWorkflowCaseArg::ResponsesSse);

        let record = run_one_http_benchmark_sample(
            &client,
            &format!("http://{address}/v1/responses"),
            "/v1/responses",
            BenchmarkMode::Direct,
            &spec,
            None,
        )
        .await;
        server.await.unwrap();

        assert_eq!(record.case, "responses_sse");
        assert_eq!(record.status, Some(200));
        assert!(record.first_event_ms.is_some());
        assert_eq!(record.output_bytes, Some(237));
        assert_eq!(record.upstream_request_id.as_deref(), Some("req_sse"));
        assert_eq!(record.usage_cached_tokens, Some(17));
        assert_eq!(record.usage_cached_tokens_field, "input_tokens_details");
        assert_eq!(record.reasoning_tokens, Some(31));
    }

    #[tokio::test]
    #[ignore = "requires binding a local TCP listener for fake WebSocket benchmark target"]
    async fn should_record_first_event_ms_for_responses_websocket_benchmark_case() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let request = websocket.next().await.unwrap().unwrap();
            assert!(request.is_text());
            websocket
                .send(UpstreamBenchmarkMessage::Text(
                    r#"{"type":"response.created"}"#.into(),
                ))
                .await
                .unwrap();
            websocket
                .send(UpstreamBenchmarkMessage::Text(
                    r#"{"type":"response.completed","response":{"usage":{"prompt_tokens_details":{"cached_tokens":23},"output_tokens_details":{"reasoning_tokens":29}}}}"#.into(),
                ))
                .await
                .unwrap();
        });
        let spec = benchmark_request_spec(BenchmarkWorkflowCaseArg::ResponsesWebsocketToolLoop);

        let record = run_one_websocket_benchmark_sample(
            &format!("ws://{address}/v1/responses"),
            "/v1/responses",
            BenchmarkMode::Direct,
            &spec,
            None,
        )
        .await;
        server.await.unwrap();

        assert_eq!(record.case, "responses_ws_25_tool_loop");
        assert_eq!(record.transport, "websocket");
        assert_eq!(record.status, Some(101));
        assert!(record.first_event_ms.is_some());
        assert_eq!(record.output_bytes, Some(172));
        assert_eq!(record.usage_cached_tokens, Some(23));
        assert_eq!(record.usage_cached_tokens_field, "prompt_tokens_details");
        assert_eq!(record.reasoning_tokens, Some(29));
        assert!(record.error_code.is_none());
    }

    fn effective_account(id: &str, models: &[&str]) -> EffectiveAccount {
        EffectiveAccount {
            config: AccountConfig {
                id: id.to_string(),
                models: models.iter().map(|model| model.to_string()).collect(),
                ..AccountConfig::default()
            },
            bearer_token: "upstream".to_string(),
            chatgpt_auth: None,
            prompt_cache_key_seed: None,
        }
    }
}
