use std::collections::BTreeSet;

use serde::Serialize;

use crate::config::EffectiveAccount;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ModelList {
    pub object: &'static str,
    pub data: Vec<ModelEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ModelEntry {
    pub id: String,
    pub object: &'static str,
}

pub fn model_list(accounts: &[EffectiveAccount]) -> ModelList {
    let mut ids = BTreeSet::new();
    for account in accounts {
        let config = &account.config;
        let supports_generation_model = config.supports_chat_completions
            || config.supports_responses
            || config.supports_responses_ws
            || config.supports_anthropic_messages;
        if !config.enabled || !supports_generation_model {
            continue;
        }
        ids.extend(config.models.iter().cloned());
    }

    ModelList {
        object: "list",
        data: ids
            .into_iter()
            .map(|id| ModelEntry {
                id,
                object: "model",
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AccountConfig;

    fn account(id: &str, models: &[&str]) -> EffectiveAccount {
        EffectiveAccount {
            config: AccountConfig {
                id: id.to_string(),
                models: models.iter().map(|model| model.to_string()).collect(),
                supports_responses: true,
                ..AccountConfig::default()
            },
            bearer_token: "token".to_string(),
            chatgpt_account_id: None,
            prompt_cache_key_seed: None,
        }
    }

    #[test]
    fn should_return_sorted_unique_models_from_enabled_generation_capable_accounts() {
        let mut disabled = account("disabled", &["gpt-disabled"]);
        disabled.config.enabled = false;

        let mut no_generation_capability = account("no-generation", &["gpt-hidden"]);
        no_generation_capability.config.supports_responses = false;

        let list = model_list(&[
            account("primary", &["gpt-5.5", "gpt-5.4"]),
            account("secondary", &["gpt-5.4", "gpt-5.3-codex"]),
            disabled,
            no_generation_capability,
        ]);

        assert_eq!(
            list.data,
            vec![
                ModelEntry {
                    id: "gpt-5.3-codex".to_string(),
                    object: "model",
                },
                ModelEntry {
                    id: "gpt-5.4".to_string(),
                    object: "model",
                },
                ModelEntry {
                    id: "gpt-5.5".to_string(),
                    object: "model",
                },
            ]
        );
    }
}
