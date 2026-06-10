#!/bin/bash
# 构建 Docker 镜像的脚本
# 宿主机编译静态二进制，Dockerfile 只做 COPY

set -e

BINARY_NAME="gptload-rs"

# 检测当前架构
ARCH=$(uname -m)
case $ARCH in
    x86_64)  DOCKER_ARCH="amd64"; RUST_TARGET="x86_64-unknown-linux-gnu" ;;
    aarch64) DOCKER_ARCH="arm64"; RUST_TARGET="aarch64-unknown-linux-gnu" ;;
    *)       echo "不支持的架构: $ARCH"; exit 1 ;;
esac

echo "==> 架构: $ARCH -> Docker: $DOCKER_ARCH, Rust target: $RUST_TARGET"

echo "==> 编译二进制..."
cargo build --release --target "$RUST_TARGET"

echo "==> 复制二进制文件到当前目录..."
cp "target/$RUST_TARGET/release/$BINARY_NAME" "$BINARY_NAME-linux-$DOCKER_ARCH"

echo "==> 构建 Docker 镜像..."
docker build --platform linux/$DOCKER_ARCH -t $BINARY_NAME .

echo "==> 清理二进制文件..."
rm -f "$BINARY_NAME-linux-$DOCKER_ARCH"

echo "==> 完成! 镜像: $BINARY_NAME"
echo "    运行: docker run -d -p 8080:8080 -v \$(pwd)/config.toml:/app/config.toml:ro $BINARY_NAME"
