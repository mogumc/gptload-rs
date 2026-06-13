FROM alpine:3.20

ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates tzdata libssl3

WORKDIR /app

COPY aequi-${TARGETOS}-${TARGETARCH} /app/aequi
RUN chmod +x /app/aequi

RUN mkdir -p /app/data

EXPOSE 8080

VOLUME ["/app/data"]

ENTRYPOINT ["/app/aequi"]
CMD ["--config", "/app/config.toml"]
