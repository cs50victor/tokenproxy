use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::auth::{ChatGptAuth, parse_chatgpt_auth_json};
use crate::error::TokenproxyError;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub downstream_auth: DownstreamAuthConfig,
    pub timeouts: TimeoutConfig,
    pub retry: RetryConfig,
    pub observability: ObservabilityConfig,
    pub accounts: Vec<AccountConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub id: String,
    pub bind: SocketAddr,
    pub allow_non_loopback: bool,
    pub allow_insecure_upstream: bool,
    pub allow_openai_request_headers: bool,
    pub max_body_bytes: usize,
    pub shutdown_grace_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            id: "tokenproxy-local".to_string(),
            bind: "127.0.0.1:8787".parse().expect("default bind parses"),
            allow_non_loopback: false,
            allow_insecure_upstream: false,
            allow_openai_request_headers: false,
            max_body_bytes: 10 * 1024 * 1024,
            shutdown_grace_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DownstreamAuthConfig {
    pub mode: String,
    pub token_env: String,
}

impl Default for DownstreamAuthConfig {
    fn default() -> Self {
        Self {
            mode: "bearer".to_string(),
            token_env: "TOKENPROXY_CLIENT_KEY".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TimeoutConfig {
    pub connect_ms: u64,
    pub request_header_ms: u64,
    pub stream_idle_ms: u64,
    pub websocket_connect_ms: u64,
    pub websocket_idle_ms: u64,
    pub pool_idle_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_ms: 3_000,
            request_header_ms: 10_000,
            stream_idle_ms: 300_000,
            websocket_connect_ms: 15_000,
            websocket_idle_ms: 300_000,
            pool_idle_ms: 90_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RetryConfig {
    pub max_precommit_retries: u8,
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub honor_retry_after: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_precommit_retries: 1,
            base_backoff_ms: 250,
            max_backoff_ms: 30_000,
            honor_retry_after: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub metrics: bool,
    pub request_body_dumps: bool,
    pub dump_dir: String,
    pub redact_json_pointers: Vec<String>,
    pub account_id_hash_key_env: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics: true,
            request_body_dumps: false,
            dump_dir: String::new(),
            redact_json_pointers: vec![
                "/api_key".to_string(),
                "/authorization".to_string(),
                "/access_token".to_string(),
                "/id_token".to_string(),
                "/password".to_string(),
                "/refresh_token".to_string(),
                "/token".to_string(),
                "/tokens/access_token".to_string(),
                "/tokens/id_token".to_string(),
                "/tokens/refresh_token".to_string(),
            ],
            account_id_hash_key_env: "TOKENPROXY_ACCOUNT_HASH_KEY".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AccountConfig {
    pub id: String,
    pub display_name: Option<String>,
    pub enabled: bool,
    pub kind: AccountKind,
    pub base_url: String,
    pub token_env: Option<String>,
    pub auth_json_path: Option<PathBuf>,
    pub priority: i32,
    pub models: Vec<String>,
    pub supports_chat_completions: bool,
    pub supports_responses: bool,
    pub supports_responses_ws: bool,
    pub supports_incremental_previous_response_id: bool,
    pub supports_compact: bool,
    pub service_tiers: Vec<String>,
    pub prompt_cache_key_seed_env: Option<String>,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            display_name: None,
            enabled: true,
            kind: AccountKind::OpenAiApiKey,
            base_url: "https://api.openai.com/v1".to_string(),
            token_env: None,
            auth_json_path: None,
            priority: 0,
            models: Vec::new(),
            supports_chat_completions: false,
            supports_responses: false,
            supports_responses_ws: false,
            supports_incremental_previous_response_id: true,
            supports_compact: false,
            service_tiers: vec!["auto".to_string(), "default".to_string()],
            prompt_cache_key_seed_env: None,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub enum AccountKind {
    #[serde(rename = "openai_api_key")]
    #[default]
    OpenAiApiKey,
    #[serde(rename = "chatgpt_codex_auth_json")]
    ChatgptCodexAuthJson,
}

#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub config: Config,
    pub downstream_token: String,
    pub account_hash_key: String,
    pub accounts: Vec<EffectiveAccount>,
}

#[derive(Debug, Clone)]
pub struct EffectiveAccount {
    pub config: AccountConfig,
    pub bearer_token: String,
    pub chatgpt_auth: Option<ChatGptAuth>,
    pub prompt_cache_key_seed: Option<String>,
}

pub trait EnvProvider {
    fn get_env(&self, key: &str) -> Option<String>;
}

pub trait FileProvider {
    fn read_to_string(&self, path: &Path) -> std::io::Result<String>;
    fn is_file(&self, path: &Path) -> bool;
}

pub struct ProcessEnv;

impl EnvProvider for ProcessEnv {
    fn get_env(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

pub struct StdFileProvider;

impl FileProvider for StdFileProvider {
    fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }
}

pub fn parse_config(input: &str) -> Result<Config, TokenproxyError> {
    toml::from_str(input).map_err(|error| {
        TokenproxyError::invalid_config(format!("failed to parse tokenproxy config: {error}"))
    })
}

pub fn load_effective_config(
    config: Config,
    env: &impl EnvProvider,
    files: &impl FileProvider,
) -> Result<EffectiveConfig, TokenproxyError> {
    validate_static_config(&config)?;
    let downstream_token = env_value(env, &config.downstream_auth.token_env)?;

    let mut enabled_accounts = Vec::new();
    let mut auth_json_paths = BTreeSet::new();

    for account in config.accounts.iter().filter(|account| account.enabled) {
        let effective = match account.kind {
            AccountKind::OpenAiApiKey => {
                let token_env = account.token_env.as_deref().ok_or_else(|| {
                    TokenproxyError::invalid_config(format!(
                        "enabled account {} missing token_env",
                        account.id
                    ))
                })?;
                EffectiveAccount {
                    config: account.clone(),
                    bearer_token: env_value(env, token_env)?,
                    chatgpt_auth: None,
                    prompt_cache_key_seed: prompt_cache_key_seed(account, env)?,
                }
            }
            AccountKind::ChatgptCodexAuthJson => {
                let path = account.auth_json_path.as_ref().ok_or_else(|| {
                    TokenproxyError::invalid_config(format!(
                        "enabled account {} missing auth_json_path",
                        account.id
                    ))
                })?;

                if !path.is_absolute() {
                    return Err(TokenproxyError::invalid_config(format!(
                        "auth_json_path for {} must be absolute",
                        account.id
                    )));
                }
                if !auth_json_paths.insert(path.clone()) {
                    return Err(TokenproxyError::invalid_config(format!(
                        "auth_json_path reused by enabled account {}",
                        account.id
                    )));
                }
                if !files.is_file(path) {
                    return Err(TokenproxyError::invalid_config(format!(
                        "auth_json_path for {} is not a readable file",
                        account.id
                    )));
                }

                let raw = files.read_to_string(path).map_err(|error| {
                    TokenproxyError::invalid_config(format!(
                        "failed to read auth_json_path for {}: {error}",
                        account.id
                    ))
                })?;
                let chatgpt_auth = parse_chatgpt_auth_json(&raw)?;
                let bearer_token = chatgpt_auth
                    .bearer_token()
                    .expect("parse_chatgpt_auth_json ensures token")
                    .to_string();

                EffectiveAccount {
                    config: account.clone(),
                    bearer_token,
                    chatgpt_auth: Some(chatgpt_auth),
                    prompt_cache_key_seed: prompt_cache_key_seed(account, env)?,
                }
            }
        };

        enabled_accounts.push(effective);
    }

    if enabled_accounts.is_empty() {
        return Err(TokenproxyError::invalid_config(
            "at least one account must be enabled",
        ));
    }

    Ok(EffectiveConfig {
        account_hash_key: account_hash_key(&config, env),
        config,
        downstream_token,
        accounts: enabled_accounts,
    })
}

fn account_hash_key(config: &Config, env: &impl EnvProvider) -> String {
    let env_name = config.observability.account_id_hash_key_env.trim();
    if env_name.is_empty() {
        return config.server.id.clone();
    }
    env.get_env(env_name)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config.server.id.clone())
}

fn prompt_cache_key_seed(
    account: &AccountConfig,
    env: &impl EnvProvider,
) -> Result<Option<String>, TokenproxyError> {
    account
        .prompt_cache_key_seed_env
        .as_deref()
        .map(|env_name| env_value(env, env_name))
        .transpose()
}

fn validate_static_config(config: &Config) -> Result<(), TokenproxyError> {
    if config.server.id.trim().is_empty() {
        return Err(TokenproxyError::invalid_config("server.id must be set"));
    }
    if config.downstream_auth.mode != "bearer" {
        return Err(TokenproxyError::invalid_config(
            "downstream_auth.mode must be bearer",
        ));
    }
    if config.downstream_auth.token_env.trim().is_empty() {
        return Err(TokenproxyError::invalid_config(
            "downstream_auth.token_env must be set",
        ));
    }
    if !config.server.allow_non_loopback && !config.server.bind.ip().is_loopback() {
        return Err(TokenproxyError::invalid_config(
            "non-loopback bind requires server.allow_non_loopback = true",
        ));
    }
    if config.timeouts.connect_ms == 0
        || config.timeouts.request_header_ms == 0
        || config.timeouts.stream_idle_ms == 0
        || config.timeouts.websocket_connect_ms == 0
        || config.timeouts.websocket_idle_ms == 0
        || config.timeouts.pool_idle_ms == 0
    {
        return Err(TokenproxyError::invalid_config(
            "timeout values must be nonzero",
        ));
    }
    if config.retry.max_precommit_retries > 1 {
        return Err(TokenproxyError::invalid_config(
            "max_precommit_retries must be 0 or 1 in stage two",
        ));
    }
    if config.observability.request_body_dumps && config.observability.dump_dir.trim().is_empty() {
        return Err(TokenproxyError::invalid_config(
            "observability.dump_dir must be set when request_body_dumps is true",
        ));
    }
    for pointer in &config.observability.redact_json_pointers {
        if !pointer.starts_with('/') {
            return Err(TokenproxyError::invalid_config(format!(
                "redact_json_pointers entry {pointer:?} must be an absolute JSON pointer",
            )));
        }
    }

    let mut ids = BTreeSet::new();
    for account in &config.accounts {
        if account.id.trim().is_empty() {
            return Err(TokenproxyError::invalid_config("account id must be set"));
        }
        if !ids.insert(account.id.clone()) {
            return Err(TokenproxyError::invalid_config(format!(
                "duplicate account id {}",
                account.id
            )));
        }
    }

    for account in &config.accounts {
        if account.enabled && !account_supports_any_route(account) {
            return Err(TokenproxyError::invalid_config(format!(
                "enabled account {} must support at least one tokenproxy route",
                account.id
            )));
        }
        if account.enabled
            && matches!(account.kind, AccountKind::ChatgptCodexAuthJson)
            && account.supports_chat_completions
        {
            return Err(TokenproxyError::invalid_config(format!(
                "chatgpt_codex_auth_json account {} cannot support chat completions",
                account.id
            )));
        }
        if account.enabled && account_requires_model_allowlist(account) && account.models.is_empty()
        {
            return Err(TokenproxyError::invalid_config(format!(
                "enabled account {} must set models for routed OpenAI-compatible endpoints",
                account.id
            )));
        }
        validate_base_url(config, account)?;
    }

    Ok(())
}

fn account_supports_any_route(account: &AccountConfig) -> bool {
    account.supports_chat_completions
        || account.supports_responses
        || account.supports_responses_ws
        || account.supports_compact
}

fn account_requires_model_allowlist(account: &AccountConfig) -> bool {
    account.supports_chat_completions || account.supports_responses || account.supports_responses_ws
}

fn validate_base_url(config: &Config, account: &AccountConfig) -> Result<(), TokenproxyError> {
    let url = reqwest::Url::parse(&account.base_url).map_err(|error| {
        TokenproxyError::invalid_config(format!("base_url for {} is invalid: {error}", account.id))
    })?;

    match url.scheme() {
        "https" => {}
        "http" if config.server.allow_insecure_upstream => {}
        "http" => {
            return Err(TokenproxyError::invalid_config(format!(
                "base_url for {} must use https",
                account.id
            )));
        }
        _ => {
            return Err(TokenproxyError::invalid_config(format!(
                "base_url for {} must use http or https",
                account.id
            )));
        }
    }

    let host = url.host_str().ok_or_else(|| {
        TokenproxyError::invalid_config(format!("base_url for {} must include a host", account.id))
    })?;

    if host.parse::<IpAddr>().is_ok() {
        return Err(TokenproxyError::invalid_config(format!(
            "base_url for {} must use a hostname, not a raw IP address",
            account.id
        )));
    }

    Ok(())
}

fn env_value(env: &impl EnvProvider, key: &str) -> Result<String, TokenproxyError> {
    env.get_env(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            TokenproxyError::invalid_config(format!("missing environment variable {key}"))
        })
}

impl EnvProvider for BTreeMap<String, String> {
    fn get_env(&self, key: &str) -> Option<String> {
        self.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MemoryFiles(BTreeMap<PathBuf, String>);

    impl FileProvider for MemoryFiles {
        fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
            self.0.get(path).cloned().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "missing test file")
            })
        }

        fn is_file(&self, path: &Path) -> bool {
            self.0.contains_key(path)
        }
    }

    struct UnreadableFile {
        path: PathBuf,
    }

    impl FileProvider for UnreadableFile {
        fn read_to_string(&self, _path: &Path) -> std::io::Result<String> {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            ))
        }

        fn is_file(&self, path: &Path) -> bool {
            path == self.path
        }
    }

    fn env() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("TOKENPROXY_CLIENT_KEY".to_string(), "client".to_string()),
            ("OPENAI_API_KEY".to_string(), "upstream".to_string()),
        ])
    }

    fn valid_openai_account() -> AccountConfig {
        AccountConfig {
            id: "openai-primary".to_string(),
            token_env: Some("OPENAI_API_KEY".to_string()),
            base_url: "https://api.openai.com/v1".to_string(),
            models: vec!["gpt-5.5".to_string()],
            supports_chat_completions: true,
            supports_responses: true,
            supports_responses_ws: true,
            supports_compact: true,
            ..AccountConfig::default()
        }
    }

    #[test]
    fn should_default_redaction_to_common_account_token_fields() {
        let pointers = ObservabilityConfig::default().redact_json_pointers;

        assert!(pointers.contains(&"/token".to_string()));
        assert!(pointers.contains(&"/id_token".to_string()));
        assert!(pointers.contains(&"/access_token".to_string()));
        assert!(pointers.contains(&"/refresh_token".to_string()));
        assert!(pointers.contains(&"/tokens/id_token".to_string()));
        assert!(pointers.contains(&"/tokens/access_token".to_string()));
        assert!(pointers.contains(&"/tokens/refresh_token".to_string()));
    }

    fn config_with_account(account: AccountConfig) -> Config {
        Config {
            accounts: vec![account],
            ..Config::default()
        }
    }

    fn expect_config_error(
        config: Config,
        env: &BTreeMap<String, String>,
        files: &impl FileProvider,
        expected: &str,
    ) {
        let error = load_effective_config(config, env, files).expect_err("config rejected");
        assert!(
            error.message.contains(expected),
            "expected {expected:?} in {:?}",
            error.message
        );
    }

    #[test]
    fn should_load_valid_openai_config() {
        let config = parse_config(
            r#"
            [server]
            bind = "127.0.0.1:8787"
            allow_openai_request_headers = true
            [downstream_auth]
            token_env = "TOKENPROXY_CLIENT_KEY"
            [[accounts]]
            id = "openai-primary"
            kind = "openai_api_key"
            token_env = "OPENAI_API_KEY"
            base_url = "https://api.openai.com/v1"
            models = ["gpt-5.5"]
            supports_chat_completions = true
            supports_responses = true
            supports_responses_ws = true
            supports_compact = true
            "#,
        )
        .unwrap();

        let effective = load_effective_config(config, &env(), &MemoryFiles(BTreeMap::new()))
            .expect("valid config loads");

        assert_eq!(effective.downstream_token, "client");
        assert_eq!(effective.accounts[0].bearer_token, "upstream");
        assert!(effective.config.server.allow_openai_request_headers);
    }

    #[test]
    fn should_load_prompt_cache_seed_from_account_env() {
        let mut account = valid_openai_account();
        account.prompt_cache_key_seed_env = Some("TOKENPROXY_PROMPT_CACHE_SEED".to_string());
        let mut env = env();
        env.insert(
            "TOKENPROXY_PROMPT_CACHE_SEED".to_string(),
            "stable-seed".to_string(),
        );

        let effective = load_effective_config(
            config_with_account(account),
            &env,
            &MemoryFiles(BTreeMap::new()),
        )
        .expect("prompt cache seed loads");

        assert_eq!(
            effective.accounts[0].prompt_cache_key_seed.as_deref(),
            Some("stable-seed")
        );
    }

    #[test]
    fn should_load_websocket_incremental_capability_separately_from_websocket_transport() {
        let config = parse_config(
            r#"
            [downstream_auth]
            token_env = "TOKENPROXY_CLIENT_KEY"
            [[accounts]]
            id = "openai-primary"
            kind = "openai_api_key"
            token_env = "OPENAI_API_KEY"
            base_url = "https://api.openai.com/v1"
            models = ["gpt-5.5"]
            supports_responses = true
            supports_responses_ws = true
            supports_incremental_previous_response_id = false
            "#,
        )
        .unwrap();

        let effective = load_effective_config(config, &env(), &MemoryFiles(BTreeMap::new()))
            .expect("valid config loads");

        assert!(effective.accounts[0].config.supports_responses_ws);
        assert!(
            !effective.accounts[0]
                .config
                .supports_incremental_previous_response_id
        );
    }

    #[test]
    fn should_reject_enabled_account_without_any_route_capability() {
        let mut account = valid_openai_account();
        account.supports_chat_completions = false;
        account.supports_responses = false;
        account.supports_responses_ws = false;
        account.supports_compact = false;

        expect_config_error(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "must support at least one tokenproxy route",
        );
    }

    #[test]
    fn should_reject_model_routed_account_without_model_allowlist() {
        let mut account = valid_openai_account();
        account.models.clear();

        expect_config_error(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "must set models for routed OpenAI-compatible endpoints",
        );
    }

    #[test]
    fn should_allow_compact_only_account_without_model_allowlist() {
        let mut account = valid_openai_account();
        account.models.clear();
        account.supports_chat_completions = false;
        account.supports_responses = false;
        account.supports_responses_ws = false;
        account.supports_compact = true;

        load_effective_config(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
        )
        .expect("compact-only account is not model routed");
    }

    #[test]
    fn should_reject_chatgpt_codex_account_with_chat_completions_capability() {
        let auth_path = PathBuf::from("/tmp/tokenproxy-chatgpt-chat-auth.json");
        let mut account = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(auth_path.clone()),
            base_url: "https://chatgpt.com".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_chat_completions: true,
            supports_responses: true,
            supports_responses_ws: true,
            supports_compact: true,
            ..AccountConfig::default()
        };
        account.token_env = None;
        let files = MemoryFiles(BTreeMap::from([(
            auth_path,
            r#"{"tokens":{"access_token":"chatgpt-access"}}"#.to_string(),
        )]));

        expect_config_error(
            config_with_account(account),
            &env(),
            &files,
            "chatgpt_codex_auth_json account chatgpt cannot support chat completions",
        );
    }

    #[test]
    fn should_load_account_hash_key_from_observability_env() {
        let mut config = config_with_account(valid_openai_account());
        config.observability.account_id_hash_key_env = "TOKENPROXY_ACCOUNT_HASH_KEY".to_string();
        let mut env = env();
        env.insert(
            "TOKENPROXY_ACCOUNT_HASH_KEY".to_string(),
            "stable-account-key".to_string(),
        );

        let effective = load_effective_config(config, &env, &MemoryFiles(BTreeMap::new())).unwrap();

        assert_eq!(effective.account_hash_key, "stable-account-key");
    }

    #[test]
    fn should_reject_duplicate_accounts_and_missing_downstream_token() {
        let config = parse_config(
            r#"
            [[accounts]]
            id = "dup"
            token_env = "OPENAI_API_KEY"
            [[accounts]]
            id = "dup"
            token_env = "OPENAI_API_KEY"
            "#,
        )
        .unwrap();

        let error = load_effective_config(config, &env(), &MemoryFiles(BTreeMap::new()))
            .expect_err("duplicate account rejected");

        assert!(error.message.contains("duplicate account id"));
    }

    #[test]
    fn should_reject_missing_required_environment_values() {
        let files = MemoryFiles(BTreeMap::new());

        expect_config_error(
            config_with_account(valid_openai_account()),
            &BTreeMap::new(),
            &files,
            "missing environment variable TOKENPROXY_CLIENT_KEY",
        );

        let env_without_upstream =
            BTreeMap::from([("TOKENPROXY_CLIENT_KEY".to_string(), "client".to_string())]);
        expect_config_error(
            config_with_account(valid_openai_account()),
            &env_without_upstream,
            &files,
            "missing environment variable OPENAI_API_KEY",
        );
    }

    #[test]
    fn should_reject_static_startup_validation_edges() {
        let files = MemoryFiles(BTreeMap::new());

        let mut non_loopback = config_with_account(valid_openai_account());
        non_loopback.server.bind = "0.0.0.0:8787".parse().unwrap();
        expect_config_error(
            non_loopback,
            &env(),
            &files,
            "non-loopback bind requires server.allow_non_loopback",
        );

        let mut missing_server_id = config_with_account(valid_openai_account());
        missing_server_id.server.id = " ".to_string();
        expect_config_error(missing_server_id, &env(), &files, "server.id must be set");

        let mut zero_timeout = config_with_account(valid_openai_account());
        zero_timeout.timeouts.connect_ms = 0;
        expect_config_error(
            zero_timeout,
            &env(),
            &files,
            "timeout values must be nonzero",
        );

        let mut invalid_retry = config_with_account(valid_openai_account());
        invalid_retry.retry.max_precommit_retries = 2;
        expect_config_error(
            invalid_retry,
            &env(),
            &files,
            "max_precommit_retries must be 0 or 1",
        );

        let mut dumps_without_dir = config_with_account(valid_openai_account());
        dumps_without_dir.observability.request_body_dumps = true;
        expect_config_error(
            dumps_without_dir,
            &env(),
            &files,
            "observability.dump_dir must be set",
        );

        let mut invalid_redaction = config_with_account(valid_openai_account());
        invalid_redaction
            .observability
            .redact_json_pointers
            .push("token".to_string());
        expect_config_error(
            invalid_redaction,
            &env(),
            &files,
            "must be an absolute JSON pointer",
        );
    }

    #[test]
    fn should_reject_invalid_or_unsafe_base_urls() {
        let files = MemoryFiles(BTreeMap::new());

        let mut invalid_url = valid_openai_account();
        invalid_url.base_url = "not a url".to_string();
        expect_config_error(
            config_with_account(invalid_url),
            &env(),
            &files,
            "base_url for openai-primary is invalid",
        );

        let mut unsafe_http = valid_openai_account();
        unsafe_http.base_url = "http://127.0.0.1:4010/v1".to_string();
        expect_config_error(
            config_with_account(unsafe_http),
            &env(),
            &files,
            "base_url for openai-primary must use https",
        );

        let mut unsupported_scheme = valid_openai_account();
        unsupported_scheme.base_url = "file:///tmp/openai.sock".to_string();
        let mut config = config_with_account(unsupported_scheme);
        config.server.allow_insecure_upstream = true;
        expect_config_error(
            config,
            &env(),
            &files,
            "base_url for openai-primary must use http or https",
        );

        let mut raw_ip = valid_openai_account();
        raw_ip.base_url = "https://172.66.0.243/v1".to_string();
        expect_config_error(
            config_with_account(raw_ip),
            &env(),
            &files,
            "base_url for openai-primary must use a hostname",
        );
    }

    #[test]
    fn should_reject_raw_ip_base_urls_even_when_insecure_upstreams_are_allowed() {
        let mut raw_ip = valid_openai_account();
        raw_ip.base_url = "http://127.0.0.1:4010/v1".to_string();
        let mut config = config_with_account(raw_ip);
        config.server.allow_insecure_upstream = true;

        expect_config_error(
            config,
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "base_url for openai-primary must use a hostname",
        );
    }

    #[test]
    fn should_allow_http_base_url_only_when_explicitly_opted_in() {
        let mut account = valid_openai_account();
        account.base_url = "http://localhost:4010/v1".to_string();
        let mut config = config_with_account(account);
        config.server.allow_insecure_upstream = true;

        load_effective_config(config, &env(), &MemoryFiles(BTreeMap::new()))
            .expect("insecure local test upstream loads only with opt-in");
    }

    #[test]
    fn should_load_chatgpt_auth_file_from_absolute_path() {
        let path = PathBuf::from("/tmp/tokenproxy-auth.json");
        let config = parse_config(&format!(
            r#"
            [[accounts]]
            id = "chatgpt"
            kind = "chatgpt_codex_auth_json"
            auth_json_path = "{}"
            base_url = "https://chatgpt.com/backend-api/codex"
            models = ["gpt-5.3-codex"]
            supports_responses = true
            supports_responses_ws = true
            "#,
            path.display()
        ))
        .unwrap();
        let files = MemoryFiles(BTreeMap::from([(
            path,
            r#"{"tokens":{"id_token":"chatgpt-token"}}"#.to_string(),
        )]));

        let effective = load_effective_config(config, &env(), &files).unwrap();

        assert_eq!(effective.accounts[0].bearer_token, "chatgpt-token");
    }

    #[test]
    fn should_reject_chatgpt_auth_file_validation_errors() {
        let files = MemoryFiles(BTreeMap::new());

        let missing_path = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        expect_config_error(
            config_with_account(missing_path),
            &env(),
            &files,
            "enabled account chatgpt missing auth_json_path",
        );

        let relative_path = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(PathBuf::from("auth.json")),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        expect_config_error(
            config_with_account(relative_path),
            &env(),
            &files,
            "auth_json_path for chatgpt must be absolute",
        );

        let unreadable_path = PathBuf::from("/tmp/tokenproxy-unreadable-auth.json");
        let unreadable = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(unreadable_path.clone()),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        expect_config_error(
            config_with_account(unreadable),
            &env(),
            &UnreadableFile {
                path: unreadable_path,
            },
            "failed to read auth_json_path for chatgpt",
        );

        let malformed_path = PathBuf::from("/tmp/tokenproxy-malformed-auth.json");
        let malformed = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(malformed_path.clone()),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        expect_config_error(
            config_with_account(malformed),
            &env(),
            &MemoryFiles(BTreeMap::from([(malformed_path, "not json".to_string())])),
            "auth_json_path contains invalid JSON",
        );
    }

    #[test]
    fn should_reject_duplicate_enabled_chatgpt_auth_paths() {
        let path = PathBuf::from("/tmp/tokenproxy-shared-auth.json");
        let account_one = AccountConfig {
            id: "chatgpt-one".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(path.clone()),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        let account_two = AccountConfig {
            id: "chatgpt-two".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(path.clone()),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        };
        let config = Config {
            accounts: vec![account_one, account_two],
            ..Config::default()
        };

        expect_config_error(
            config,
            &env(),
            &MemoryFiles(BTreeMap::from([(
                path,
                r#"{"tokens":{"id_token":"chatgpt-token"}}"#.to_string(),
            )])),
            "auth_json_path reused by enabled account chatgpt-two",
        );
    }
}
