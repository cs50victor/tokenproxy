pub fn version_text() -> String {
    format!("tokenproxy {}", env!("CARGO_PKG_VERSION"))
}

pub fn os() -> &'static str {
    env!("TOKENPROXY_OS")
}

pub fn kernel() -> &'static str {
    env!("TOKENPROXY_KERNEL")
}

pub fn cpu() -> &'static str {
    env!("TOKENPROXY_CPU")
}

pub fn git_sha() -> &'static str {
    env!("TOKENPROXY_GIT_SHA")
}

pub fn rustc_version() -> &'static str {
    env!("TOKENPROXY_RUSTC_VERSION")
}

pub fn curl_version() -> &'static str {
    env!("TOKENPROXY_CURL_VERSION")
}

pub fn submodule_status() -> &'static str {
    env!("TOKENPROXY_SUBMODULE_STATUS")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_render_normal_cargo_package_version() {
        let text = version_text();

        assert_eq!(text, format!("tokenproxy {}", env!("CARGO_PKG_VERSION")));
    }
}
