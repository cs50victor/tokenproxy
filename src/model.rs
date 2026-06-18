use std::borrow::Cow;

pub fn model_family_label(model: &str) -> String {
    let model = model.trim();
    let model = if model.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(model.to_ascii_lowercase())
    } else {
        Cow::Borrowed(model)
    };
    if model.is_empty() || model == "unknown" {
        return "unknown".to_string();
    }

    let mut parts = model.split('-');
    let Some(prefix) = parts.next().filter(|part| !part.is_empty()) else {
        return "unknown".to_string();
    };

    let Some(version) = parts.next().filter(|part| !part.is_empty()) else {
        return prefix.to_string();
    };
    let major = version.split('.').next().unwrap_or(version);
    if major.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}-{major}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_label_major_model_family() {
        assert_eq!(model_family_label("gpt-5.5"), "gpt-5");
        assert_eq!(model_family_label("GPT-5.5"), "gpt-5");
        assert_eq!(model_family_label("gpt-4o-mini"), "gpt-4o");
        assert_eq!(model_family_label("o3-mini"), "o3-mini");
        assert_eq!(model_family_label("unknown"), "unknown");
        assert_eq!(model_family_label("  "), "unknown");
    }
}
