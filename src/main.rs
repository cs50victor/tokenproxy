use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser};
use tokenproxy::config::{
    EffectiveConfig, ProcessEnv, StdFileProvider, load_effective_config,
    parse_config_with_cli_overrides,
};
use tokenproxy::logging::{
    LogFormat, StartupConfigSummary, StartupLogLine, shutdown_forced_log_line, startup_log_line,
};
use tokenproxy::server::{AppState, app};
use tokenproxy::time_parse::now_timestamp_pair;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Small Rust proxy for OpenAI-compatible agent traffic"
)]
struct Cli {
    #[arg(long, value_name = "FILE", help = "Path to tokenproxy.toml")]
    config: Option<PathBuf>,
    #[arg(
        short = 'c',
        long = "config-override",
        value_name = "key=value",
        action = ArgAction::Append,
        help = "Override a config value using a dotted path; the value is parsed as TOML, matching Codex CLI -c behavior"
    )]
    config_overrides: Vec<String>,
    #[arg(long)]
    bind: Option<std::net::SocketAddr>,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    log_json: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let config_path = cli
        .config
        .or_else(|| std::env::var_os("TOKENPROXY_CONFIG").map(PathBuf::from))
        .or_else(|| {
            let default_path = PathBuf::from("./tokenproxy.toml");
            if cli.config_overrides.is_empty() || default_path.is_file() {
                Some(default_path)
            } else {
                None
            }
        });

    let raw_config = config_path
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?;
    let mut config = parse_config_with_cli_overrides(raw_config.as_deref(), &cli.config_overrides)?;
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
    let bind_label = bind.to_string();

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
                bind: &bind_label,
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
            bind: &bind_label,
            enabled_accounts: effective.accounts.len(),
            config: &startup_config,
        },)
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state =
        AppState::new_with_log_format_and_shutdown(effective, log_format, shutdown_tx.clone())?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
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
    let account_status_labels = effective
        .config
        .accounts
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
    fn should_accept_codex_style_config_overrides() {
        let cli = Cli::try_parse_from([
            "tokenproxy",
            "-c",
            "server.id=from-cli",
            "-c",
            "server.max_body_bytes=2048",
        ])
        .unwrap();

        assert_eq!(
            cli.config_overrides,
            vec![
                "server.id=from-cli".to_string(),
                "server.max_body_bytes=2048".to_string()
            ]
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

    fn effective_account(id: &str, models: &[&str]) -> EffectiveAccount {
        EffectiveAccount {
            config: AccountConfig {
                id: id.to_string(),
                models: models.iter().map(|model| model.to_string()).collect(),
                ..AccountConfig::default()
            },
            bearer_token: "upstream".to_string(),
            chatgpt_account_id: None,
            prompt_cache_key_seed: None,
        }
    }
}
