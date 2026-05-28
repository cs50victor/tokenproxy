use serde_json::Value;

use crate::error::TokenproxyError;
use crate::time_parse::parse_rfc3339;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatGptAuth {
    pub id_token: Option<String>,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub account_id: Option<String>,
    pub last_refresh: Option<chrono::DateTime<chrono::Utc>>,
}

impl ChatGptAuth {
    pub fn bearer_token(&self) -> Option<&str> {
        self.id_token.as_deref().or(self.access_token.as_deref())
    }
}

pub fn parse_chatgpt_auth_json(input: &str) -> Result<ChatGptAuth, TokenproxyError> {
    let value: Value = serde_json::from_str(input).map_err(|error| {
        TokenproxyError::invalid_config(format!("auth_json_path contains invalid JSON: {error}"))
    })?;

    let tokens = value.get("tokens").unwrap_or(&Value::Null);
    let auth = ChatGptAuth {
        id_token: string_field(tokens, "id_token"),
        access_token: string_field(tokens, "access_token")
            .or_else(|| string_field(&value, "OPENAI_API_KEY")),
        refresh_token: string_field(tokens, "refresh_token"),
        account_id: string_field(tokens, "account_id"),
        last_refresh: utc_datetime_field(&value, "last_refresh")?,
    };

    if auth.bearer_token().is_none() {
        return Err(TokenproxyError::invalid_config(
            "auth_json_path lacks ChatGPT token data",
        ));
    }

    Ok(auth)
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn utc_datetime_field(
    value: &Value,
    field: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, TokenproxyError> {
    let Some(raw) = string_field(value, field) else {
        return Ok(None);
    };
    let Some(parsed) = parse_rfc3339(&raw) else {
        return Err(TokenproxyError::invalid_config(format!(
            "auth_json_path field {field} must be RFC3339"
        )));
    };
    Ok(Some(parsed.to_utc()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_codex_auth_json_token_shape() {
        let auth = parse_chatgpt_auth_json(
            r#"{"auth_mode":"chatgpt","last_refresh":"2026-05-27T11:24:18Z","tokens":{"id_token":"id","access_token":"access","refresh_token":"refresh","account_id":"acct"}}"#,
        )
        .unwrap();

        assert_eq!(auth.bearer_token(), Some("id"));
        assert_eq!(auth.refresh_token.as_deref(), Some("refresh"));
        assert_eq!(auth.account_id.as_deref(), Some("acct"));
        assert_eq!(
            auth.last_refresh.as_ref().map(chrono::DateTime::to_rfc3339),
            Some("2026-05-27T11:24:18+00:00".to_string())
        );
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
