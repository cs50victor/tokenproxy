# Executive Summary

For a stage-one, low-latency, OpenAI-compatible Rust proxy, the recommended design prioritizes simplicity, reliability, and pragmatic performance optimizations. The core goal is to create a minimal proxy that forwards requests to `api.openai.com` or an alternative like Cloudflare AI Gateway. The proxy must support key OpenAI endpoints: the legacy `POST /v1/chat/completions`, the modern `POST /v1/responses` including its Server-Sent Events (SSE) streaming, and the `wss://api.openai.com/v1/realtime` WebSocket API for low-latency, bi-directional communication. The discontinued Codex endpoints should not be implemented. The recommended technical foundation is Rust's `hyper` and `tokio` ecosystem, which provides a production-grade, minimal stack. The architecture should be a simple reverse proxy with three distinct logic paths: one for standard JSON requests, one for non-buffered SSE stream pass-through, and one for WebSocket connection tunneling. A critical finding is that `api.openai.com` is served via Cloudflare's Anycast network; therefore, the proxy must rely on DNS resolution of the hostname for routing. Attempting to connect via direct IP addresses is unreliable, will likely cause TLS errors, and is actively blocked by Cloudflare. For performance, the design should enable HTTP/2 for upstream connections, prefer TLS 1.3 for faster handshakes, and implement connection pooling. For a stage-one implementation, a 'server mode' (listening reverse proxy) is the most practical approach, centralizing concerns like authentication and observability.

# Proxy Core Design Recommendations

## Recommended Framework

Tokio, as it provides the asynchronous runtime for writing reliable, high-performance network applications in Rust.

## Core Library

Hyper, as it is a minimal, production-grade, and fast HTTP implementation that serves as the foundation for both the client and server components of the proxy.

## Architecture Principle

Keep it small and simple. The proxy should be a minimal reverse proxy with only three essential code paths: one for standard JSON POST requests, one for non-buffered Server-Sent Events (SSE) pass-through, and one for WebSocket upgrade and tunneling. Heavy frameworks should be avoided.

## Performance Goal

To achieve the best-effort low latency and high reliability by forwarding requests to OpenAI's API. This is accomplished through pragmatic optimizations like using HTTP/2 upstream, preferring TLS 1.3, reusing connections, and leveraging Linux kernel features like BBR where appropriate.


# Openai Endpoint Analysis

OpenAI's API landscape has evolved, with a clear transition from older endpoints to more modern, capable ones. The Codex API, once used for code generation, was officially discontinued in March 2023, and any related features like 'fast mode' are now considered legacy. The `v1/chat/completions` endpoint, while still operational, is primarily for backward compatibility. OpenAI's official recommendation for new projects is to use the `v1/responses` API, which offers advanced features like stateful interactions and built-in tools, with streaming handled via Server-Sent Events (SSE). For the lowest-latency, bi-directional communication, such as streaming audio, the `v1/realtime` WebSocket API is provided. This server-to-server interface offers the most direct access to the models.

# Chat Completions Api Status

## Endpoint Path

POST /v1/chat/completions

## Current Status

Operational, but primarily for backward compatibility.

## Official Recommendation

For new projects, it is recommended to use the Responses API to take advantage of the latest platform features.


# Responses Api Details

## Endpoint Path

POST /v1/responses

## Key Features

Allows for the creation of stateful interactions with the model and extends the model's capabilities with built-in tools.

## Streaming Mechanism

Server-Sent Events (SSE). When the 'stream=true' parameter is used, the server emits a stream of events with names like 'response.created' and 'response.completed'.

## Replaces Api

Chat Completions API. OpenAI recommends the Responses API for all new projects.


# Realtime Api Websocket Details

## Endpoint Url

wss://api.openai.com/v1/realtime?model=gpt-realtime-2

## Protocol

WebSocket, over which bi-directional JSON-serialized events are sent and received as text strings.

## Authentication

A standard API key is used, passed in the 'Authorization: Bearer <API key>' header during the connection upgrade.

## Primary Use Case

Designed for server-to-server integrations requiring the lowest-level interface, including handling real-time data like base64 encoded audio chunks.


# Codex Api Status And Behavior

## Status

Discontinued

## Deprecation Date

March 2023

## Implication

Features associated with the legacy Codex API, such as a 'fast mode service tier' or WebSocket-based remote control tools, are obsolete and not applicable to the current public OpenAI API.


# Network Reliability And Routing

The recommended network routing strategy for connecting to OpenAI services is to exclusively use hostnames such as `api.openai.com` and `chatgpt.com` and rely on standard DNS resolution. These domains are served via Cloudflare's Anycast network, which automatically routes client requests to the nearest and healthiest data center in its global network. This approach provides significant benefits in terms of speed, resilience, and high availability. Conversely, attempting to connect directly to the raw IP addresses of these services is an inappropriate and unreliable strategy. Direct-IP connections are prone to failure due to both technical and policy-based reasons. The underlying infrastructure requires the client to specify the hostname during the TLS handshake via SNI to receive the correct certificate, and the service provider, Cloudflare, frequently blocks direct-IP access as a security policy, leading to connection errors. Therefore, for reliable and performant connections, traffic should always be steered by DNS and Anycast, and any form of IP pinning should be avoided.

# Anycast For Reliability And Latency

## Provider

Cloudflare

## Mechanism

Anycast routing works by advertising a single IP address from multiple geographically dispersed data centers (Cloudflare's Points of Presence, or POPs). When a client sends a request to this IP address, the network routes the traffic to the topologically nearest healthy data center. This means a request from Europe will be handled by a European server, while a request from Asia will be handled by an Asian server, all using the same destination IP address.

## Primary Benefits

The main advantages are improved speed and resilience. Speed is enhanced by reducing latency, as user traffic travels a shorter distance to the nearest server. Resilience and high availability are achieved because if one data center or DNS resolver goes offline, the network can automatically reroute traffic to other healthy locations, preventing service interruptions.

## Affected Domains

The key OpenAI domains routed via the Cloudflare Anycast network include `api.openai.com`, `chatgpt.com`, `chat.com`, and the parent `openai.com`.


# Unreliability Of Direct Ip Connections

## Viability

Direct-IP connections are not a viable or appropriate strategy for connecting to OpenAI services. The documentation strongly concludes that one must always use hostnames and avoid any form of IP pinning for reliable access.

## Reason For Failure

The primary reasons for failure are a combination of TLS handshake errors and explicit policy blocks by the CDN provider, Cloudflare. Connections fail because they cannot complete the security handshake correctly and because they violate the provider's terms of access.

## Underlying Technology

The specific technology that causes the connection failure is Server Name Indication (SNI), which is a critical extension to the Transport Layer Security (TLS) protocol.

## Technical Explanation

When a client attempts to connect directly to a raw IP address, it does not include the target hostname (e.g., `api.openai.com`) in the SNI field of the initial TLS handshake message. Cloudflare's servers are multi-tenant, meaning a single IP address hosts services for many different domains. They rely on the SNI information to identify which domain the client is trying to reach and present the corresponding TLS certificate. Without SNI, the server cannot select the correct certificate, which results in a 'common name mismatch' error and a failed handshake. Furthermore, Cloudflare's security policies often identify direct-IP access as suspicious or improper and will actively block the connection, returning an HTTP error like 'Error 1003: Direct IP Access Not Allowed'.


# Transport Protocol Choices

The choice of transport protocol between the end-user/client and the proxy depends on the specific OpenAI API being accessed. For the stage-one design of a minimal proxy, the following options are recommended:

- **Plain HTTPS (HTTP/1.1 or HTTP/2)**: This is the recommended default for most interactions, including calls to the `/v1/chat/completions` and `/v1/responses` endpoints. It is simple, widely supported, and easy to debug. For streaming responses (when `stream=true` is used with the Responses API), Server-Sent Events (SSE) are delivered over a standard, long-lived HTTP connection. HTTP/2 can offer benefits like multiplexing for concurrent requests over a single connection, but HTTP/1.1 is sufficient.

- **WebSockets**: This protocol should be used exclusively when the client needs to access the OpenAI Realtime API, which requires a bi-directional, low-latency communication channel. The proxy's role is to handle the WebSocket upgrade request and transparently tunnel the connection to the upstream endpoint (e.g., `wss://api.openai.com/v1/realtime`). It is advised to avoid tunneling other API calls, like SSE streams, over a WebSocket in the initial design to maintain simplicity.

- **gRPC and HTTP/3**: These are considered for future implementation but deferred for the initial stage. While gRPC offers benefits like strong schemas and efficient multiplexing over HTTP/2, bridging it with SSE can be awkward and it adds a client-side SDK dependency. HTTP/3 is promising for improving performance on lossy or mobile networks but is deferred due to its implementation complexity and more limited client-side support.

# Server Side Optimization Strategy

To minimize latency and maximize throughput for the Linux server hosting the proxy, a multi-layered optimization strategy is recommended, focusing on the TCP stack, TLS handshakes, and connection management for both standard HTTP traffic and persistent connections like WebSockets and Server-Sent Events (SSE).

1.  **TCP Stack Tuning**: The TCP congestion control algorithm should be set to BBR (Bottleneck Bandwidth and Round-trip propagation time), which is particularly effective for improving throughput on high-bandwidth, high-latency network paths. This requires setting the kernel's default queueing discipline to `fq` (Fair Queue). For interactive messages, such as WebSocket control frames, the `TCP_NODELAY` socket option can be enabled to disable Nagle's algorithm and reduce latency, though this may increase the number of packets.

2.  **TLS Performance**: Prefer TLS 1.3 for all connections. Its primary performance benefit is a faster handshake process, which reduces the number of round-trips required to establish a secure connection compared to older TLS versions. This directly lowers initial connection latency.

3.  **Socket and Runtime Configuration**: To scale across multiple CPU cores, the `SO_REUSEPORT` socket option should be used on the listening socket. This allows multiple threads or processes to bind to the same port and accept incoming connections, which pairs well with a multi-threaded async runtime like Tokio. System-level `sysctl` parameters for network buffers and connection tracking should also be tuned according to enterprise server best practices.

4.  **Connection Management for SSE/WebSockets**: For long-lived connections like SSE and WebSockets, it's crucial to tune keep-alive and idle timeout settings to prevent premature disconnection by intermediate network devices. For WebSockets, the proxy should implement periodic pings to maintain connection health. When handling SSE, the proxy must pass the stream through without buffering to ensure the client receives events as soon as they are sent from the upstream server.

5.  **Upstream HTTP Client Pooling**: The proxy's HTTP client should reuse upstream connections to `api.openai.com` to avoid the overhead of repeated TCP and TLS handshakes. HTTP/2 should be enabled for the upstream connection pool to allow for multiplexing multiple requests over a single connection, reducing head-of-line blocking under concurrent loads.

# Linux Tcp Stack Tuning

## Congestion Control

BBR (Bottleneck Bandwidth and Round-trip propagation time) is the recommended TCP congestion control algorithm. It is designed to deliver significant performance improvements on high-bandwidth, high-latency connections (high BDP paths).

## Queueing Discipline

The `fq` (Fair Queue) queueing discipline is required to work with the BBR congestion control algorithm. It must be set as the default queueing discipline.

## Socket Options

Recommended socket options include `SO_REUSEPORT` to allow for scaling the server's listening socket across multiple threads on multi-core systems, and `TCP_NODELAY` to disable Nagle's algorithm, which is beneficial for reducing latency on small, interactive messages like WebSocket control frames.

## Kernel Parameters

To apply the recommended tuning via sysctl, the following parameters should be set in a configuration file like `/etc/sysctl.conf` and applied:
`net.core.default_qdisc=fq`
`net.ipv4.tcp_congestion_control=bbr`


# Tls Performance Optimization

## Recommended Version

TLS 1.3

## Key Performance Feature

The primary performance feature of TLS 1.3 is a faster handshake process. It streamlines the negotiation between the client and server.

## Latency Impact

TLS 1.3 speeds up connection establishment by reducing the number of round-trips required for the handshake compared to previous versions like TLS 1.2. This directly lowers the initial latency when a new secure connection is created.


# Websocket And Sse Tuning

## Sse Optimization Technique

A key tuning consideration for Server-Sent Events (SSE) is managing the long-lived HTTP connection. The proxy must have generous idle timeouts and properly configured TCP keep-alives to prevent the connection from being prematurely terminated by intermediate network devices or the server itself. Additionally, the proxy should disable any response buffering for SSE streams to ensure events are passed through to the client with minimal delay.

## Websocket Optimization Technique

A performance tradeoff to consider for WebSockets is the use of the `permessage-deflate` extension for compression. While compression can reduce bandwidth usage, it comes at the cost of increased CPU load on both the client and server. The recommendation is to disable WebSocket compression by default and only enable it if the application is heavily bandwidth-bound and sufficient CPU resources are available.


# Utility Of Server Client Modes

For a small, stage-one proxy design, the 'server mode' is the most useful and practical implementation. In this mode, the proxy acts as a listening reverse proxy, accepting connections from clients and forwarding them to the upstream OpenAI API. This architecture is highly beneficial as it allows for the centralization of critical functionalities such as authentication (e.g., injecting API keys), observability (logging and metrics), and rate-limiting. It can also be strategically placed close to the network egress point with good peering to optimize connections. In contrast, a 'client/sidecar mode,' where the proxy acts as a local forwarder or library, is less relevant for an initial design. While a sidecar could potentially reduce LAN latency and pre-establish upstream connections, it significantly increases deployment complexity for end-users. Therefore, the recommendation is to focus exclusively on building the server mode for stage-one, with the client/sidecar mode being a potential future addition if a specific need arises.

# Evidence Backed Tradeoffs For Small Proxy

## Technology Stack Tradeoff

The primary tradeoff is between using a minimal, low-level stack like `hyper` and `tokio` versus a heavier, more feature-rich framework. The evidence points towards choosing the minimal stack. While a larger framework might offer more out-of-the-box features, a small proxy's core logic is simple (forwarding requests). Using `hyper` directly allows for maximum performance, fine-grained control, and a very small binary size, with examples showing a functional reverse proxy can be built in under 1,000 lines of code. The tradeoff is accepting more boilerplate code in exchange for performance, control, and minimal dependencies.

## Protocol Selection Tradeoff

The tradeoff lies between the simplicity of Server-Sent Events (SSE) and the bi-directional capability of WebSockets. The OpenAI API uses both for different purposes. For the Responses API (`/v1/responses`), which involves the server streaming data to the client, SSE is the specified protocol. It is simple, text-based, and works over standard HTTP, making it easy to proxy without special handling. For the Realtime API, which requires true bi-directional communication, WebSockets are necessary. The pragmatic tradeoff for a stage-one proxy is to implement pass-through for both protocols in their native contexts and avoid the complexity of converting SSE streams into a WebSocket channel, which would add no functional value and increase overhead.

## Reliability Strategy Tradeoff

This is a critical tradeoff between attempting complex, unreliable routing logic versus leveraging standard, robust internet infrastructure. Evidence shows that `api.openai.com` is fronted by Cloudflare's Anycast network, which automatically routes traffic to the nearest and healthiest data center. The alternative, attempting to connect directly to OpenAI's servers via a pinned IP address, is highly unreliable. Evidence from Cloudflare's documentation confirms that direct-IP access is often blocked (resulting in Error 1003) and that TLS handshakes will fail due to Server Name Indication (SNI) mismatches. Therefore, the correct and reliable strategy is to always use the DNS hostname (`api.openai.com`) and let Anycast manage routing and failover, trading perceived manual control for superior, built-in resilience.
