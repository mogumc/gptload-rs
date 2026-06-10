FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# 从构建上下文复制预编译的二进制文件
COPY gptload-rs .
RUN chmod +x gptload-rs

# 创建数据目录
RUN mkdir -p /app/data

EXPOSE 8080

VOLUME ["/app/data"]

ENTRYPOINT ["/app/gptload-rs"]
CMD ["--config", "/app/config.toml"]
