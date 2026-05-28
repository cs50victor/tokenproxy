use std::fs;
use std::process::Command;

fn main() {
    print_git_rerun_inputs();
    println!("cargo:rerun-if-changed=.gitmodules");
    println!("cargo:rustc-env=TOKENPROXY_OS={}", std::env::consts::OS);
    println!(
        "cargo:rustc-env=TOKENPROXY_KERNEL={}",
        command_output("uname", &["-sr"])
    );
    println!("cargo:rustc-env=TOKENPROXY_CPU={}", cpu_name());
    println!(
        "cargo:rustc-env=TOKENPROXY_GIT_SHA={}",
        command_output("git", &["rev-parse", "--short=12", "HEAD"])
    );
    println!(
        "cargo:rustc-env=TOKENPROXY_RUSTC_VERSION={}",
        command_output("rustc", &["--version"])
    );
    println!("cargo:rustc-env=TOKENPROXY_CURL_VERSION={}", curl_version());
    println!(
        "cargo:rustc-env=TOKENPROXY_SUBMODULE_STATUS={}",
        command_output("git", &["submodule", "status", "--recursive"])
            .lines()
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("; ")
    );
}

fn print_git_rerun_inputs() {
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/logs/HEAD");

    let Ok(head) = fs::read_to_string(".git/HEAD") else {
        return;
    };
    let Some(ref_name) = head.trim().strip_prefix("ref: ") else {
        return;
    };
    println!("cargo:rerun-if-changed=.git/{ref_name}");
    println!("cargo:rerun-if-changed=.git/logs/{ref_name}");
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().replace('\n', " "))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unavailable".to_string())
}

fn cpu_name() -> String {
    let sysctl = command_output("sysctl", &["-n", "machdep.cpu.brand_string"]);
    if sysctl != "unavailable" {
        return sysctl;
    }
    command_output("uname", &["-m"])
}

fn curl_version() -> String {
    command_output("curl", &["--version"])
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ")
}
