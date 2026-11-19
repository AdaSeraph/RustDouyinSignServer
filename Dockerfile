FROM rust:1-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/douyin-sign-server-rust /app/
COPY scripts ./scripts
COPY static ./static
COPY config.example.toml LICENSE README.md ./
LABEL org.opencontainers.image.source="https://github.com/AdaSeraph/RustDouyinSignServer"
LABEL org.opencontainers.image.description="For learning & non-commercial research only. See LICENSE."
LABEL org.opencontainers.image.licenses="LicenseRef-DouyinSignServerRust-NC"
EXPOSE 5289
CMD ["/app/douyin-sign-server-rust"]
