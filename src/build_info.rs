pub fn version_text() -> String {
    format!(
        "tokenproxy version={} os={} kernel={} cpu={} git_sha={} rustc={} curl={} submodules={}",
        env!("CARGO_PKG_VERSION"),
        os(),
        kernel(),
        cpu(),
        git_sha(),
        rustc_version(),
        curl_version(),
        submodule_status()
    )
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
    fn should_render_version_with_git_rust_and_submodule_snapshot() {
        let text = version_text();

        assert!(text.contains("tokenproxy version="));
        assert!(text.contains("os="));
        assert!(text.contains("kernel="));
        assert!(text.contains("cpu="));
        assert!(text.contains("git_sha="));
        assert!(text.contains("rustc="));
        assert!(text.contains("curl="));
        assert!(text.contains("submodules="));
    }
}
