use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use object_store::ObjectStoreExt;
use serde::Deserialize;
use serde::de::Error as SerdeError;
use toml::Value as TomlValue;

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
    pub supports_anthropic_messages: bool,
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
            supports_anthropic_messages: false,
            service_tiers: vec!["auto".to_string(), "default".to_string()],
            prompt_cache_key_seed_env: None,
        }
    }
}

impl AccountConfig {
    fn supports_any_route(&self) -> bool {
        self.supports_chat_completions
            || self.supports_responses
            || self.supports_responses_ws
            || self.supports_compact
            || self.supports_anthropic_messages
    }

    fn requires_model_allowlist(&self) -> bool {
        matches!(self.kind, AccountKind::AnthropicApiKey) && self.supports_anthropic_messages
    }

    fn should_discover_models(&self) -> bool {
        self.enabled
            && matches!(
                self.kind,
                AccountKind::OpenAiApiKey | AccountKind::ChatgptCodexAuthJson
            )
            && (self.supports_chat_completions
                || self.supports_responses
                || self.supports_responses_ws)
    }
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub enum AccountKind {
    #[serde(rename = "openai_api_key")]
    #[default]
    OpenAiApiKey,
    #[serde(rename = "anthropic_api_key")]
    AnthropicApiKey,
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
    pub chatgpt_account_id: Option<String>,
    pub prompt_cache_key_seed: Option<String>,
}

pub trait EnvProvider {
    fn get_env(&self, key: &str) -> Option<String>;
}

pub trait FileProvider {
    fn read_to_string(&self, path: &Path) -> std::io::Result<String>;
    fn is_file(&self, path: &Path) -> bool;
    fn read_s3_uri_to_string(
        &self,
        uri: &str,
        timeout: Duration,
    ) -> Result<String, TokenproxyError> {
        let _ = timeout;
        Err(TokenproxyError::invalid_config(format!(
            "S3 auth_json_path {uri} cannot be read by this provider"
        )))
    }
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

    fn read_s3_uri_to_string(
        &self,
        uri: &str,
        timeout: Duration,
    ) -> Result<String, TokenproxyError> {
        read_s3_uri_to_string_blocking(uri.to_string(), timeout)
    }
}

pub fn parse_config_with_cli_overrides(
    input: Option<&str>,
    overrides: &[String],
) -> Result<Config, TokenproxyError> {
    let mut config = match input {
        Some(input) => parse_config_value(input)?,
        None => TomlValue::Table(toml::map::Map::new()),
    };
    apply_cli_overrides(&mut config, overrides)?;
    config.try_into().map_err(|error: toml::de::Error| {
        TokenproxyError::invalid_config(format!("failed to parse tokenproxy config: {error}"))
    })
}

fn parse_config_value(input: &str) -> Result<TomlValue, TokenproxyError> {
    toml::from_str(input).map_err(|error| {
        TokenproxyError::invalid_config(format!("failed to parse tokenproxy config: {error}"))
    })
}

fn apply_cli_overrides(
    config: &mut TomlValue,
    overrides: &[String],
) -> Result<(), TokenproxyError> {
    for raw_override in overrides {
        let (path, value) = parse_cli_override(raw_override)?;
        apply_single_override(config, &path, value);
    }
    Ok(())
}

fn parse_cli_override(raw: &str) -> Result<(String, TomlValue), TokenproxyError> {
    let mut parts = raw.splitn(2, '=');
    let key = parts.next().unwrap_or_default().trim();
    let value = parts.next().ok_or_else(|| {
        TokenproxyError::invalid_config(format!("invalid -c override (missing '='): {raw}"))
    })?;

    if key.is_empty() {
        return Err(TokenproxyError::invalid_config(format!(
            "empty key in -c override: {raw}"
        )));
    }
    if key.split('.').any(str::is_empty) {
        return Err(TokenproxyError::invalid_config(format!(
            "empty path segment in -c override: {raw}"
        )));
    }

    let value = match parse_toml_value(value.trim()) {
        Ok(value) => value,
        Err(_) => TomlValue::String(
            value
                .trim()
                .trim_matches(|character| character == '"' || character == '\'')
                .to_string(),
        ),
    };

    Ok((key.to_string(), value))
}

fn parse_toml_value(raw: &str) -> Result<TomlValue, toml::de::Error> {
    let wrapped = format!("_x_ = {raw}");
    let table: toml::Table = toml::from_str(&wrapped)?;
    table
        .get("_x_")
        .cloned()
        .ok_or_else(|| SerdeError::custom("missing sentinel key"))
}

fn apply_single_override(root: &mut TomlValue, path: &str, value: TomlValue) {
    let mut current = root;
    let mut parts = path.split('.').peekable();

    while let Some(part) = parts.next() {
        let is_last = parts.peek().is_none();

        if is_last {
            match current {
                TomlValue::Table(table) => {
                    table.insert(part.to_string(), value);
                }
                _ => {
                    let mut table = toml::map::Map::new();
                    table.insert(part.to_string(), value);
                    *current = TomlValue::Table(table);
                }
            }
            return;
        }

        match current {
            TomlValue::Table(table) => {
                current = table
                    .entry(part.to_string())
                    .or_insert_with(|| TomlValue::Table(toml::map::Map::new()));
            }
            _ => {
                *current = TomlValue::Table(toml::map::Map::new());
                if let TomlValue::Table(table) = current {
                    current = table
                        .entry(part.to_string())
                        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()));
                }
            }
        }
    }
}

pub fn load_effective_config(
    config: Config,
    env: &impl EnvProvider,
    files: &impl FileProvider,
) -> Result<EffectiveConfig, TokenproxyError> {
    let config = expand_config_paths(config, env)?;
    validate_static_config(&config)?;
    let downstream_token = env_value(env, &config.downstream_auth.token_env)?;

    let mut enabled_accounts = Vec::new();
    let mut auth_json_locations = BTreeSet::new();
    let auth_json_fetch_timeout = Duration::from_millis(config.timeouts.request_header_ms);

    for account in config.accounts.iter().filter(|account| account.enabled) {
        let effective = match account.kind {
            AccountKind::OpenAiApiKey | AccountKind::AnthropicApiKey => {
                let token_env = account.token_env.as_deref().ok_or_else(|| {
                    TokenproxyError::invalid_config(format!(
                        "enabled account {} missing token_env",
                        account.id
                    ))
                })?;
                EffectiveAccount {
                    config: account.clone(),
                    bearer_token: env_value(env, token_env)?,
                    chatgpt_account_id: None,
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

                let expanded_path = expand_user_path(path, &account.id, env)?;
                let location = auth_json_location(&expanded_path, &account.id)?;
                if !auth_json_locations.insert(location.key.clone()) {
                    return Err(TokenproxyError::invalid_config(format!(
                        "auth_json_path reused by enabled account {}",
                        account.id
                    )));
                }

                let raw = read_auth_json_location(
                    files,
                    &location,
                    &account.id,
                    auth_json_fetch_timeout,
                )?;
                let chatgpt_auth = parse_chatgpt_auth_json(&raw)?;

                EffectiveAccount {
                    config: AccountConfig {
                        auth_json_path: Some(expanded_path),
                        ..account.clone()
                    },
                    bearer_token: chatgpt_auth.bearer_token,
                    chatgpt_account_id: chatgpt_auth.account_id,
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

#[derive(Debug, Deserialize)]
struct UpstreamModelList {
    data: Vec<UpstreamModel>,
}

#[derive(Debug, Deserialize)]
struct UpstreamModel {
    id: String,
}

pub async fn discover_account_models(
    mut effective: EffectiveConfig,
) -> Result<EffectiveConfig, TokenproxyError> {
    if !effective
        .accounts
        .iter()
        .any(|account| account.config.should_discover_models())
    {
        return Ok(effective);
    }

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(effective.config.timeouts.connect_ms))
        .build()
        .map_err(|error| {
            TokenproxyError::invalid_config(format!(
                "failed to build model discovery HTTP client: {error}"
            ))
        })?;
    let timeout = Duration::from_millis(effective.config.timeouts.request_header_ms);

    for account in &mut effective.accounts {
        if !account.config.should_discover_models() {
            continue;
        }
        let discovered = fetch_account_models(&client, account, timeout).await?;
        account.config.models = filter_discovered_models(discovered, &account.config.models);
    }

    Ok(effective)
}

async fn fetch_account_models(
    client: &reqwest::Client,
    account: &EffectiveAccount,
    timeout: Duration,
) -> Result<Vec<String>, TokenproxyError> {
    let url = model_discovery_url(account)?;
    let mut request = client.get(url).bearer_auth(&account.bearer_token);
    if let Some(account_id) = account.chatgpt_account_id.as_deref() {
        request = request.header("chatgpt-account-id", account_id);
    }

    let response = tokio::time::timeout(timeout, request.send())
        .await
        .map_err(|_| {
            TokenproxyError::invalid_config(format!(
                "model discovery for account {} timed out",
                account.config.id
            ))
        })?
        .map_err(|error| {
            TokenproxyError::invalid_config(format!(
                "model discovery for account {} failed: {error}",
                account.config.id
            ))
        })?;

    let status = response.status();
    if !status.is_success() {
        return Err(TokenproxyError::invalid_config(format!(
            "model discovery for account {} failed with HTTP {status}",
            account.config.id
        )));
    }

    let list = response
        .json::<UpstreamModelList>()
        .await
        .map_err(|error| {
            TokenproxyError::invalid_config(format!(
                "model discovery for account {} returned invalid JSON: {error}",
                account.config.id
            ))
        })?;
    let models = list
        .data
        .into_iter()
        .map(|model| model.id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    if models.is_empty() {
        return Err(TokenproxyError::invalid_config(format!(
            "model discovery for account {} returned no models",
            account.config.id
        )));
    }

    Ok(models)
}

fn filter_discovered_models(discovered: Vec<String>, configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        return discovered;
    }

    let allowlist = configured
        .iter()
        .map(|model| model.trim().to_ascii_lowercase())
        .filter(|model| !model.is_empty())
        .collect::<BTreeSet<_>>();
    if allowlist.is_empty() {
        return discovered;
    }

    let filtered = discovered
        .iter()
        .filter(|model| allowlist.contains(&model.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        discovered
    } else {
        filtered
    }
}

fn model_discovery_url(account: &EffectiveAccount) -> Result<reqwest::Url, TokenproxyError> {
    let mut url = reqwest::Url::parse(&account.config.base_url).map_err(|error| {
        TokenproxyError::invalid_config(format!(
            "base_url for {} is invalid: {error}",
            account.config.id
        ))
    })?;
    let base_path = url.path().trim_end_matches('/');
    url.set_path(&format!("{base_path}/models"));
    Ok(url)
}

struct AuthJsonLocation {
    key: String,
    source: AuthJsonSource,
}

enum AuthJsonSource {
    Local(PathBuf),
    S3(String),
}

fn expand_user_path(
    path: &Path,
    account_id: &str,
    env: &impl EnvProvider,
) -> Result<PathBuf, TokenproxyError> {
    let raw = path.to_string_lossy();
    let expanded = expand_user_path_value(
        raw.as_ref(),
        &format!("auth_json_path for {account_id}"),
        env,
    )?;
    Ok(PathBuf::from(expanded))
}

fn expand_config_paths(
    mut config: Config,
    env: &impl EnvProvider,
) -> Result<Config, TokenproxyError> {
    config.observability.dump_dir = expand_user_path_value(
        &config.observability.dump_dir,
        "observability.dump_dir",
        env,
    )?;
    Ok(config)
}

fn expand_user_path_value(
    raw: &str,
    field: &str,
    env: &impl EnvProvider,
) -> Result<String, TokenproxyError> {
    let home = env.get_env("HOME").filter(|value| !value.trim().is_empty());
    if home.is_none() && uses_home_shortcut(raw) {
        return Err(TokenproxyError::invalid_config(format!(
            "{field} uses ~ but HOME is not set"
        )));
    }

    Ok(shellexpand::tilde_with_context(raw, || home.as_deref()).into_owned())
}

fn uses_home_shortcut(path: &str) -> bool {
    path == "~" || path.starts_with("~/")
}

fn auth_json_location(path: &Path, account_id: &str) -> Result<AuthJsonLocation, TokenproxyError> {
    let raw = path.to_string_lossy();
    if raw.starts_with("s3://") {
        let uri = normalize_s3_auth_json_uri(&raw, account_id)?;
        return Ok(AuthJsonLocation {
            key: uri.clone(),
            source: AuthJsonSource::S3(uri),
        });
    }

    if !path.is_absolute() {
        return Err(TokenproxyError::invalid_config(format!(
            "auth_json_path for {account_id} must be absolute or use s3://"
        )));
    }

    Ok(AuthJsonLocation {
        key: format!("file:{}", path.display()),
        source: AuthJsonSource::Local(path.to_path_buf()),
    })
}

fn normalize_s3_auth_json_uri(uri: &str, account_id: &str) -> Result<String, TokenproxyError> {
    let url = reqwest::Url::parse(uri).map_err(|error| {
        TokenproxyError::invalid_config(format!(
            "auth_json_path for {account_id} is not a valid s3:// URI: {error}"
        ))
    })?;
    if url.scheme() != "s3" {
        return Err(TokenproxyError::invalid_config(format!(
            "auth_json_path for {account_id} must be absolute or use s3://"
        )));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(TokenproxyError::invalid_config(format!(
            "auth_json_path for {account_id} must not include query or fragment data"
        )));
    }
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| {
            TokenproxyError::invalid_config(format!(
                "auth_json_path for {account_id} must include an S3 bucket"
            ))
        })?;
    let key = url.path().trim_start_matches('/');
    if key.is_empty() {
        return Err(TokenproxyError::invalid_config(format!(
            "auth_json_path for {account_id} must include an S3 object key"
        )));
    }

    Ok(format!("s3://{bucket}/{key}"))
}

fn read_auth_json_location(
    files: &impl FileProvider,
    location: &AuthJsonLocation,
    account_id: &str,
    timeout: Duration,
) -> Result<String, TokenproxyError> {
    match &location.source {
        AuthJsonSource::Local(path) => {
            if !files.is_file(path) {
                return Err(TokenproxyError::invalid_config(format!(
                    "auth_json_path for {account_id} is not a readable file"
                )));
            }
            files.read_to_string(path).map_err(|error| {
                TokenproxyError::invalid_config(format!(
                    "failed to read auth_json_path for {account_id}: {error}"
                ))
            })
        }
        AuthJsonSource::S3(uri) => files.read_s3_uri_to_string(uri, timeout).map_err(|error| {
            TokenproxyError::invalid_config(format!(
                "failed to read auth_json_path for {account_id}: {}",
                error.message
            ))
        }),
    }
}

fn read_s3_uri_to_string_blocking(
    uri: String,
    timeout: Duration,
) -> Result<String, TokenproxyError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                TokenproxyError::invalid_config(format!(
                    "failed to initialize S3 auth_json_path runtime: {error}"
                ))
            })
            .and_then(|runtime| runtime.block_on(read_s3_uri_to_string(&uri, timeout)));
        let _ = sender.send(result);
    });
    receiver.recv().map_err(|error| {
        TokenproxyError::invalid_config(format!("failed to read S3 auth_json_path: {error}"))
    })?
}

async fn read_s3_uri_to_string(uri: &str, timeout: Duration) -> Result<String, TokenproxyError> {
    let url = reqwest::Url::parse(uri).map_err(|error| {
        TokenproxyError::invalid_config(format!("invalid S3 auth_json_path URI: {error}"))
    })?;
    let (store, path) = object_store::parse_url_opts(&url, std::env::vars()).map_err(|error| {
        TokenproxyError::invalid_config(format!("failed to configure S3 auth_json_path: {error}"))
    })?;
    let bytes = tokio::time::timeout(timeout, async {
        let object = store.get(&path).await?;
        object.bytes().await
    })
    .await
    .map_err(|_| TokenproxyError::invalid_config("S3 auth_json_path fetch timed out"))?
    .map_err(|error| {
        TokenproxyError::invalid_config(format!(
            "failed to fetch S3 auth_json_path object: {error}"
        ))
    })?;

    String::from_utf8(bytes.to_vec()).map_err(|error| {
        TokenproxyError::invalid_config(format!("S3 auth_json_path object is not UTF-8: {error}"))
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
        if account.enabled && !account.supports_any_route() {
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
        if account.enabled
            && account.supports_anthropic_messages
            && !matches!(account.kind, AccountKind::AnthropicApiKey)
        {
            return Err(TokenproxyError::invalid_config(format!(
                "account {} must use kind anthropic_api_key to support Anthropic messages",
                account.id
            )));
        }
        if account.enabled
            && matches!(account.kind, AccountKind::AnthropicApiKey)
            && (account.supports_chat_completions
                || account.supports_responses
                || account.supports_responses_ws
                || account.supports_compact)
        {
            return Err(TokenproxyError::invalid_config(format!(
                "anthropic_api_key account {} cannot support OpenAI routes",
                account.id
            )));
        }
        if account.enabled && account.requires_model_allowlist() && account.models.is_empty() {
            return Err(TokenproxyError::invalid_config(format!(
                "enabled account {} must set models for routed generation endpoints",
                account.id
            )));
        }
        validate_base_url(config, account)?;
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub fn parse_config(input: &str) -> Result<Config, TokenproxyError> {
        parse_config_value(input)?
            .try_into()
            .map_err(|error: toml::de::Error| {
                TokenproxyError::invalid_config(format!(
                    "failed to parse tokenproxy config: {error}"
                ))
            })
    }

    impl EnvProvider for BTreeMap<String, String> {
        fn get_env(&self, key: &str) -> Option<String> {
            self.get(key).cloned()
        }
    }

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

    #[derive(Default)]
    struct MemorySources {
        files: BTreeMap<PathBuf, String>,
        s3: BTreeMap<String, String>,
        s3_error: Option<String>,
    }

    impl FileProvider for MemorySources {
        fn read_to_string(&self, path: &Path) -> std::io::Result<String> {
            self.files.get(path).cloned().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "missing test file")
            })
        }

        fn is_file(&self, path: &Path) -> bool {
            self.files.contains_key(path)
        }

        fn read_s3_uri_to_string(
            &self,
            uri: &str,
            _timeout: Duration,
        ) -> Result<String, TokenproxyError> {
            if let Some(error) = &self.s3_error {
                return Err(TokenproxyError::invalid_config(error.clone()));
            }
            self.s3
                .get(uri)
                .cloned()
                .ok_or_else(|| TokenproxyError::invalid_config("missing test S3 auth object"))
        }
    }

    fn env() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("TOKENPROXY_CLIENT_KEY".to_string(), "client".to_string()),
            ("OPENAI_API_KEY".to_string(), "upstream".to_string()),
            (
                "ANTHROPIC_API_KEY".to_string(),
                "anthropic-upstream".to_string(),
            ),
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

    fn valid_anthropic_account() -> AccountConfig {
        AccountConfig {
            id: "anthropic-primary".to_string(),
            kind: AccountKind::AnthropicApiKey,
            token_env: Some("ANTHROPIC_API_KEY".to_string()),
            base_url: "https://api.anthropic.com".to_string(),
            models: vec!["claude-sonnet-4.5".to_string()],
            supports_anthropic_messages: true,
            service_tiers: Vec::new(),
            ..AccountConfig::default()
        }
    }

    fn valid_chatgpt_account(id: &str, auth_json_path: impl Into<PathBuf>) -> AccountConfig {
        AccountConfig {
            id: id.to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(auth_json_path.into()),
            base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        }
    }

    fn chatgpt_config(auth_json_path: &str) -> Config {
        parse_config(&format!(
            r#"
            [[accounts]]
            id = "chatgpt"
            kind = "chatgpt_codex_auth_json"
            auth_json_path = "{auth_json_path}"
            base_url = "https://chatgpt.com/backend-api/codex"
            models = ["gpt-5.3-codex"]
            supports_responses = true
            supports_responses_ws = true
            "#
        ))
        .unwrap()
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

    async fn model_fixture_base_url(
        base_path: &'static str,
        expected_path: &'static str,
        expected_auth: &'static str,
        expected_chatgpt_account_id: Option<&'static str>,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(
                request.starts_with(&format!("GET {expected_path} HTTP/1.1")),
                "{request}"
            );
            assert!(request.contains(expected_auth), "{request}");
            if let Some(account_id) = expected_chatgpt_account_id {
                assert!(
                    request.contains(&format!("chatgpt-account-id: {account_id}")),
                    "{request}"
                );
            }
            let body =
                r#"{"object":"list","data":[{"id":"gpt-5.5"},{"id":"gpt-5.5"},{"id":"gpt-5.4"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });
        format!("http://localhost:{port}{base_path}")
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
    fn should_allow_chatgpt_account_without_static_models() {
        let path = PathBuf::from("/tmp/tokenproxy-chatgpt-model-discovery-auth.json");
        let mut account = valid_chatgpt_account("chatgpt", path.clone());
        account.models.clear();
        let files = MemoryFiles(BTreeMap::from([(
            path,
            r#"{"tokens":{"access_token":"chatgpt-access"}}"#.to_string(),
        )]));

        let effective = load_effective_config(config_with_account(account), &env(), &files)
            .expect("ChatGPT models can be discovered after config loading");

        assert!(effective.accounts[0].config.models.is_empty());
    }

    #[test]
    fn should_still_reject_anthropic_account_without_model_allowlist() {
        let mut account = valid_anthropic_account();
        account.models.clear();

        expect_config_error(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "must set models for routed generation endpoints",
        );
    }

    #[tokio::test]
    async fn should_discover_chatgpt_models_when_models_are_omitted() {
        let base_url = model_fixture_base_url(
            "/backend-api/codex",
            "/backend-api/codex/models",
            "authorization: Bearer chatgpt-access",
            Some("acct_123"),
        )
        .await;
        let path = PathBuf::from("/tmp/tokenproxy-chatgpt-discover-auth.json");
        let mut account = valid_chatgpt_account("chatgpt", path.clone());
        account.base_url = base_url;
        account.models.clear();
        let files = MemoryFiles(BTreeMap::from([(
            path,
            r#"{"tokens":{"access_token":"chatgpt-access","account_id":"acct_123"}}"#.to_string(),
        )]));
        let mut config = config_with_account(account);
        config.server.allow_insecure_upstream = true;

        let effective = load_effective_config(config, &env(), &files).unwrap();
        let effective = discover_account_models(effective).await.unwrap();

        assert_eq!(
            effective.accounts[0].config.models,
            vec!["gpt-5.4", "gpt-5.5"]
        );
    }

    #[tokio::test]
    async fn should_filter_discovered_openai_models_by_valid_configured_models() {
        let base_url =
            model_fixture_base_url("/v1", "/v1/models", "authorization: Bearer upstream", None)
                .await;
        let mut config = config_with_account(AccountConfig {
            base_url,
            models: vec!["gpt-5.5".to_string(), "missing-model".to_string()],
            ..valid_openai_account()
        });
        config.server.allow_insecure_upstream = true;

        let effective =
            load_effective_config(config, &env(), &MemoryFiles(BTreeMap::new())).unwrap();
        let effective = discover_account_models(effective).await.unwrap();

        assert_eq!(effective.accounts[0].config.models, vec!["gpt-5.5"]);
    }

    #[test]
    fn should_ignore_configured_openai_models_when_none_are_valid() {
        assert_eq!(
            filter_discovered_models(
                vec!["gpt-5.4".to_string(), "gpt-5.5".to_string()],
                &["missing-model".to_string()],
            ),
            vec!["gpt-5.4", "gpt-5.5"]
        );
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
    fn should_apply_codex_style_cli_config_overrides() {
        let config = parse_config_with_cli_overrides(
            Some(
                r#"
                [server]
                id = "from-file"
                max_body_bytes = 1024
                [downstream_auth]
                token_env = "TOKENPROXY_CLIENT_KEY"
                [[accounts]]
                id = "openai-primary"
                kind = "openai_api_key"
                token_env = "OPENAI_API_KEY"
                base_url = "https://api.openai.com/v1"
                models = ["gpt-5.5"]
                supports_responses = true
                "#,
            ),
            &[
                "server.id=from-cli".to_string(),
                "server.max_body_bytes=2048".to_string(),
                "accounts=[{id=\"override\", kind=\"openai_api_key\", token_env=\"OPENAI_API_KEY\", base_url=\"https://api.openai.com/v1\", models=[\"gpt-5.4\"], supports_chat_completions=true}]".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(config.server.id, "from-cli");
        assert_eq!(config.server.max_body_bytes, 2048);
        assert_eq!(config.accounts[0].id, "override");
        assert!(config.accounts[0].supports_chat_completions);
    }

    #[test]
    fn should_parse_cli_override_values_like_codex() {
        let config = parse_config_with_cli_overrides(
            None,
            &[
                "downstream_auth.token_env=TOKENPROXY_CLIENT_KEY".to_string(),
                "server.allow_openai_request_headers=true".to_string(),
                "accounts=[{id=\"openai-primary\", kind=\"openai_api_key\", token_env=\"OPENAI_API_KEY\", base_url=\"https://api.openai.com/v1\", models=[\"gpt-5.5\"], supports_responses=true}]".to_string(),
            ],
        )
        .unwrap();

        assert_eq!(
            config.downstream_auth.token_env.as_str(),
            "TOKENPROXY_CLIENT_KEY"
        );
        assert!(config.server.allow_openai_request_headers);
        assert_eq!(config.accounts[0].models, vec!["gpt-5.5"]);
    }

    #[test]
    fn should_reject_cli_override_paths_with_empty_segments() {
        let error = parse_config_with_cli_overrides(None, &["server..id=tokenproxy".to_string()])
            .unwrap_err();

        assert_eq!(error.code.as_str(), "invalid_config");
        assert!(error.message.contains("empty path segment"));
    }

    #[test]
    fn should_fallback_malformed_cli_override_values_to_strings() {
        let config = parse_config_with_cli_overrides(
            None,
            &["downstream_auth.token_env='TOKENPROXY_CLIENT_KEY".to_string()],
        )
        .unwrap();

        assert_eq!(
            config.downstream_auth.token_env.as_str(),
            "TOKENPROXY_CLIENT_KEY"
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
    fn should_allow_openai_model_routed_account_without_model_allowlist() {
        let mut account = valid_openai_account();
        account.models.clear();

        load_effective_config(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
        )
        .expect("OpenAI accounts can discover models after config loading");
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
    fn should_reject_chatgpt_codex_account_with_anthropic_messages_capability() {
        let auth_path = PathBuf::from("/tmp/tokenproxy-chatgpt-messages-auth.json");
        let mut account = AccountConfig {
            id: "chatgpt".to_string(),
            kind: AccountKind::ChatgptCodexAuthJson,
            auth_json_path: Some(auth_path.clone()),
            base_url: "https://chatgpt.com".to_string(),
            models: vec!["gpt-5.3-codex".to_string()],
            supports_responses: true,
            supports_responses_ws: true,
            supports_anthropic_messages: true,
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
            "account chatgpt must use kind anthropic_api_key to support Anthropic messages",
        );
    }

    #[test]
    fn should_load_anthropic_messages_account() {
        let effective = load_effective_config(
            config_with_account(valid_anthropic_account()),
            &env(),
            &MemoryFiles(BTreeMap::new()),
        )
        .unwrap();

        assert_eq!(effective.accounts.len(), 1);
        assert_eq!(
            effective.accounts[0].config.kind,
            AccountKind::AnthropicApiKey
        );
        assert_eq!(effective.accounts[0].bearer_token, "anthropic-upstream");
        assert!(effective.accounts[0].config.supports_anthropic_messages);
    }

    #[test]
    fn should_reject_openai_account_with_anthropic_messages_capability() {
        let mut account = valid_openai_account();
        account.supports_anthropic_messages = true;

        expect_config_error(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "account openai-primary must use kind anthropic_api_key to support Anthropic messages",
        );
    }

    #[test]
    fn should_reject_anthropic_account_with_openai_route_capability() {
        let mut account = valid_anthropic_account();
        account.supports_responses = true;

        expect_config_error(
            config_with_account(account),
            &env(),
            &MemoryFiles(BTreeMap::new()),
            "anthropic_api_key account anthropic-primary cannot support OpenAI routes",
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

        let mut dumps_with_home_dir = config_with_account(valid_openai_account());
        dumps_with_home_dir.observability.request_body_dumps = true;
        dumps_with_home_dir.observability.dump_dir = "~/.cache/tokenproxy/dumps".to_string();
        let mut env_with_home = env();
        env_with_home.insert("HOME".to_string(), "/Users/tokenproxy".to_string());
        let effective = load_effective_config(dumps_with_home_dir, &env_with_home, &files).unwrap();
        assert_eq!(
            effective.config.observability.dump_dir,
            "/Users/tokenproxy/.cache/tokenproxy/dumps"
        );

        let mut dumps_with_missing_home = config_with_account(valid_openai_account());
        dumps_with_missing_home.observability.request_body_dumps = true;
        dumps_with_missing_home.observability.dump_dir = "~/.cache/tokenproxy/dumps".to_string();
        expect_config_error(
            dumps_with_missing_home,
            &env(),
            &files,
            "observability.dump_dir uses ~ but HOME is not set",
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
        let config = chatgpt_config(&path.display().to_string());
        let files = MemoryFiles(BTreeMap::from([(
            path,
            r#"{"tokens":{"id_token":"chatgpt-token"}}"#.to_string(),
        )]));

        let effective = load_effective_config(config, &env(), &files).unwrap();

        assert_eq!(effective.accounts[0].bearer_token, "chatgpt-token");
    }

    #[test]
    fn should_expand_home_relative_chatgpt_auth_path() {
        let expanded = PathBuf::from("/Users/tokenproxy/.config/tokenproxy/auth.json");
        let config = chatgpt_config("~/.config/tokenproxy/auth.json");
        let mut env = env();
        env.insert("HOME".to_string(), "/Users/tokenproxy".to_string());
        let files = MemoryFiles(BTreeMap::from([(
            expanded.clone(),
            r#"{"tokens":{"access_token":"chatgpt-access"}}"#.to_string(),
        )]));

        let effective = load_effective_config(config, &env, &files).unwrap();

        assert_eq!(effective.accounts[0].bearer_token, "chatgpt-access");
        assert_eq!(
            effective.accounts[0].config.auth_json_path.as_deref(),
            Some(expanded.as_path())
        );
    }

    #[test]
    fn should_load_chatgpt_auth_json_from_s3_uri() {
        let uri = "s3://tokenproxy-auth/chatgpt/auth.json";
        let sources = MemorySources {
            s3: BTreeMap::from([(
                uri.to_string(),
                r#"{"tokens":{"access_token":"chatgpt-access","account_id":"acct_123"}}"#
                    .to_string(),
            )]),
            ..MemorySources::default()
        };

        let effective = load_effective_config(chatgpt_config(uri), &env(), &sources).unwrap();

        assert_eq!(effective.accounts[0].bearer_token, "chatgpt-access");
        assert_eq!(
            effective.accounts[0].chatgpt_account_id.as_deref(),
            Some("acct_123")
        );
    }

    #[test]
    fn should_reject_chatgpt_auth_file_validation_errors() {
        let files = MemoryFiles(BTreeMap::new());

        let mut missing_path = valid_chatgpt_account("chatgpt", "/tmp/ignored-auth.json");
        missing_path.auth_json_path = None;
        expect_config_error(
            config_with_account(missing_path),
            &env(),
            &files,
            "enabled account chatgpt missing auth_json_path",
        );

        let relative_path = valid_chatgpt_account("chatgpt", "auth.json");
        expect_config_error(
            config_with_account(relative_path),
            &env(),
            &files,
            "auth_json_path for chatgpt must be absolute or use s3://",
        );

        expect_config_error(
            chatgpt_config("~/missing-auth.json"),
            &env(),
            &files,
            "auth_json_path for chatgpt uses ~ but HOME is not set",
        );

        let unreadable_path = PathBuf::from("/tmp/tokenproxy-unreadable-auth.json");
        let unreadable = valid_chatgpt_account("chatgpt", unreadable_path.clone());
        expect_config_error(
            config_with_account(unreadable),
            &env(),
            &UnreadableFile {
                path: unreadable_path,
            },
            "failed to read auth_json_path for chatgpt",
        );

        let malformed_path = PathBuf::from("/tmp/tokenproxy-malformed-auth.json");
        let malformed = valid_chatgpt_account("chatgpt", malformed_path.clone());
        expect_config_error(
            config_with_account(malformed),
            &env(),
            &MemoryFiles(BTreeMap::from([(malformed_path, "not json".to_string())])),
            "auth_json_path contains invalid JSON",
        );
    }

    #[test]
    fn should_reject_invalid_chatgpt_auth_s3_uris() {
        expect_config_error(
            chatgpt_config("s3://tokenproxy-auth"),
            &env(),
            &MemorySources::default(),
            "must include an S3 object key",
        );

        expect_config_error(
            chatgpt_config("s3://tokenproxy-auth/auth.json?token=secret"),
            &env(),
            &MemorySources::default(),
            "must not include query or fragment data",
        );
    }

    #[test]
    fn should_surface_chatgpt_auth_s3_fetch_errors() {
        let sources = MemorySources {
            s3_error: Some("S3 object not found".to_string()),
            ..MemorySources::default()
        };

        expect_config_error(
            chatgpt_config("s3://tokenproxy-auth/missing.json"),
            &env(),
            &sources,
            "failed to read auth_json_path for chatgpt: S3 object not found",
        );
    }

    #[test]
    fn should_reject_duplicate_enabled_chatgpt_auth_paths() {
        let path = PathBuf::from("/tmp/tokenproxy-shared-auth.json");
        let config = Config {
            accounts: vec![
                valid_chatgpt_account("chatgpt-one", path.clone()),
                valid_chatgpt_account("chatgpt-two", path.clone()),
            ],
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

    #[test]
    fn should_reject_duplicate_enabled_chatgpt_auth_s3_uris() {
        let uri = "s3://tokenproxy-auth/shared/auth.json";
        let config = Config {
            accounts: vec![
                valid_chatgpt_account("chatgpt-one", uri),
                valid_chatgpt_account("chatgpt-two", uri),
            ],
            ..Config::default()
        };

        expect_config_error(
            config,
            &env(),
            &MemorySources {
                s3: BTreeMap::from([(
                    uri.to_string(),
                    r#"{"tokens":{"id_token":"chatgpt-token"}}"#.to_string(),
                )]),
                ..MemorySources::default()
            },
            "auth_json_path reused by enabled account chatgpt-two",
        );
    }
}

use crate::time_parse::parse_rfc3339;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatGptAuth {
    pub bearer_token: String,
    pub account_id: Option<String>,
}

pub fn parse_chatgpt_auth_json(input: &str) -> Result<ChatGptAuth, TokenproxyError> {
    let value: Value = serde_json::from_str(input).map_err(|error| {
        TokenproxyError::invalid_config(format!("auth_json_path contains invalid JSON: {error}"))
    })?;

    if let Some(last_refresh) = string_field(&value, "last_refresh")
        && parse_rfc3339(&last_refresh).is_none()
    {
        return Err(TokenproxyError::invalid_config(
            "auth_json_path field last_refresh must be RFC3339",
        ));
    }

    let tokens = value.get("tokens").unwrap_or(&Value::Null);
    // Codex sends the OAuth access_token as the upstream bearer; the OIDC
    // id_token is an identity assertion, not an API credential. CLIProxyAPI
    // stores the same token fields at the top level.
    let bearer_token = string_field(tokens, "access_token")
        .or_else(|| string_field(&value, "access_token"))
        .or_else(|| string_field(&value, "OPENAI_API_KEY"))
        .or_else(|| string_field(tokens, "id_token"))
        .or_else(|| string_field(&value, "id_token"))
        .ok_or_else(|| {
            TokenproxyError::invalid_config("auth_json_path lacks ChatGPT token data")
        })?;

    Ok(ChatGptAuth {
        bearer_token,
        account_id: string_field(tokens, "account_id")
            .or_else(|| string_field(&value, "account_id")),
    })
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod auth_tests {
    use super::*;

    #[test]
    fn should_use_access_token_as_upstream_bearer_for_codex_auth_json() {
        let auth = parse_chatgpt_auth_json(
            r#"{"auth_mode":"chatgpt","last_refresh":"2026-05-27T11:24:18Z","tokens":{"id_token":"id","access_token":"access","refresh_token":"refresh","account_id":"acct"}}"#,
        )
        .unwrap();

        assert_eq!(auth.bearer_token, "access");
        assert_eq!(auth.account_id.as_deref(), Some("acct"));
    }

    #[test]
    fn should_use_top_level_access_token_for_cli_proxy_codex_auth_json() {
        let auth = parse_chatgpt_auth_json(
            r#"{"type":"codex","email":"user@example.com","last_refresh":"2026-05-27T11:24:18Z","access_token":"access","id_token":"id","refresh_token":"refresh","account_id":"acct"}"#,
        )
        .unwrap();

        assert_eq!(auth.bearer_token, "access");
        assert_eq!(auth.account_id.as_deref(), Some("acct"));
    }

    #[test]
    fn should_reject_invalid_auth_json_last_refresh() {
        let error =
            parse_chatgpt_auth_json(r#"{"last_refresh":"not-a-time","tokens":{"id_token":"id"}}"#)
                .unwrap_err();

        assert!(error.message.contains("last_refresh"));
    }

    #[test]
    fn should_reject_auth_json_without_token_data() {
        let error = parse_chatgpt_auth_json(r#"{"tokens":{}}"#).unwrap_err();

        assert!(error.message.contains("lacks ChatGPT token data"));
    }

    #[test]
    fn should_not_use_refresh_token_as_upstream_bearer() {
        let error =
            parse_chatgpt_auth_json(r#"{"tokens":{"refresh_token":"refresh"}}"#).unwrap_err();

        assert!(error.message.contains("lacks ChatGPT token data"));
    }
}
