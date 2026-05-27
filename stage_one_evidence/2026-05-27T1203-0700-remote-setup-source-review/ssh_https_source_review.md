# Remote setup source review

Timestamp: 2026-05-27 12:03 PM PDT / 2026-05-27 19:03 UTC.

Question: should stage-two tokenproxy support remote Codex access through normal `https://`/`wss://`, SSH local forwarding, or both?

Commands and sources reviewed:

- `parallel-cli search "Find primary-source benchmark data and source evidence about SSH tunnel overhead for HTTP/WebSocket traffic versus direct HTTPS/WSS reverse proxy, with emphasis on OpenSSH benchmarks, GitHub repositories, and official docs. Need numbers, commands, and conclusions relevant to choosing one deployment mode for a low-latency proxy." -q "OpenSSH tunnel benchmark HTTP WebSocket latency" -q "SSH port forwarding benchmark latency throughput github" -q "direct HTTPS vs SSH tunnel WebSocket benchmark" --json --max-results 10 --excerpt-max-chars-total 30000 -o "/tmp/ssh-tunnel-websocket-benchmarks.json"`
- `gh api repos/alexeygrigorev/ssh-auto-forward/contents/benchmarks/README.md -H 'Accept: application/vnd.github.raw'`
- `gh api repos/alexeygrigorev/ssh-auto-forward/contents/benchmarks/results_html_stress.json -H 'Accept: application/vnd.github.raw'`
- `gh api repos/alexeygrigorev/ssh-auto-forward/contents/benchmarks/results_large_files.json -H 'Accept: application/vnd.github.raw'`
- `gh api repos/erebe/wstunnel/contents/README.md -H 'Accept: application/vnd.github.raw'`
- OpenBSD `ssh_config(5)`: https://man.openbsd.org/ssh_config.5
- OpenBSD `ssh(1)`: https://man.openbsd.org/ssh.1
- NGINX WebSocket proxying: https://nginx.org/en/docs/http/websocket.html
- Caddy `reverse_proxy`: https://caddyserver.com/docs/caddyfile/directives/reverse_proxy
- Cloudflare WebSockets: https://developers.cloudflare.com/network/websockets/
- Allan Jude, "SSH Performance": https://papers.freebsd.org/2017/bsdcan/jude-ssh_performance.files/Paper_-_SSH_Performance.pdf

Findings:

- OpenSSH local forwarding is a generic TCP channel over an existing SSH client process. It solves private reachability but requires a separate SSH session, local port binding, lifecycle handling, and failure handling outside the OpenAI-compatible client.
- OpenSSH's documented defaults include encrypted ciphers and local/remote forwarding controls. Allan Jude's FreeBSD SSH performance paper shows modern SSH can be high-throughput with AES-GCM on server hardware, so SSH is not automatically too slow. The paper does not show a latency advantage over direct HTTPS/WSS for application traffic.
- `ssh-auto-forward` benchmarks compare a Python Paramiko helper with native `ssh -L`, not direct HTTPS/WSS. Native SSH handled small HTML requests at about 1.4-2.2 ms p50 and 2.0-3.9 ms p99 on the Windows/Hetzner rows; the helper added higher p99 tails. Bulk native SSH reached 220.7 MB/s for a 10 GB stream on the Windows/Hetzner row. This proves native SSH forwarding can work, but not that it is faster or simpler than direct HTTPS/WSS.
- Caddy, NGINX, and Cloudflare document normal WebSocket proxy support. A normal HTTPS/WSS endpoint is the native remote shape for OpenAI-compatible HTTP/SSE/WebSocket clients and avoids a second user-managed tunnel command.
- `wstunnel` exists because some networks block arbitrary protocols and allow WebSocket/HTTP traffic. Its README mirrors the same tradeoff: tunneling is useful when reachability is blocked, but it adds protocol, authentication, TLS/SNI, keepalive, and restriction configuration.

Decision:

Stage-two tokenproxy should support one remote client setup: normal `https://` for HTTP/SSE and `wss://` for Responses WebSocket at the configured tokenproxy origin, protected by downstream client authentication. SSH local forwarding should not be a tokenproxy-supported setup path or CLI-managed feature. Operators can still use SSH manually as an out-of-band debug or private-admin workaround, but the implementation spec should not require tests, diagrams, client commands, or product behavior for SSH tunneling.

No local SSH-vs-direct tokenproxy benchmark was run because stage-two product code does not exist and this checkout has no remote tokenproxy/SSH benchmark target. No performance claim for SSH versus direct HTTPS/WSS is accepted from this review.
