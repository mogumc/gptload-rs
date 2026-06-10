FROM alpine:3.20

ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates tzdata libssl3

WORKDIR /app

COPY gptload-rs-${TARGETOS}-${TARGETARCH} /app/gptload-rs
RUN chmod +x /app/gptload-rs

RUN mkdir -p /app/data

EXPOSE 8080

VOLUME ["/app/data"]

ENTRYPOINT ["/app/gptload-rs"]
CMD ["--config", "/app/config.toml"]
