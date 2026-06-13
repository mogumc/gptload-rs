#!/bin/bash
# 构建 Docker 镜像的脚本

set -e

BINARY_NAME="aequi"

# 检测当前架构
ARCH=$(uname -m)
case $ARCH in
    x86_64)  DOCKER_ARCH="amd64"; RUST_TARGET="x86_64-unknown-linux-musl" ;;
    aarch64) DOCKER_ARCH="arm64"; RUST_TARGET="aarch64-unknown-linux-musl" ;;
    *)       echo "不支持的架构: $ARCH"; exit 1 ;;
esac

echo "==> 架构: $ARCH -> Docker: $DOCKER_ARCH, Rust target: $RUST_TARGET"

# 首选使用 cross 进行可靠交叉编译；若未安装则使用 cargo 直编
if command -v cross &> /dev/null; then
    echo "==> 使用 cross 编译二进制..."
    cross build --release --target "$RUST_TARGET"
else
    echo "==> cross 未安装，使用 cargo 编译..."
    # 确保 musl target 已安装
    rustup target add "$RUST_TARGET"

    # 设置 musl-gcc 交叉编译器
    if command -v musl-gcc &> /dev/null; then
        export CC_x86_64_unknown_linux_musl=musl-gcc
        echo "    使用 musl-gcc 作为链接器"
    else
        echo "    警告: musl-gcc 未找到，可能需要安装 musl-tools"
        echo "    安装方法: sudo apt-get install musl-tools"
    fi

    cargo build --release --target "$RUST_TARGET"
fi

echo "==> 复制二进制文件到当前目录..."
cp "target/$RUST_TARGET/release/$BINARY_NAME" "$BINARY_NAME-linux-$DOCKER_ARCH"

echo "==> 构建 Docker 镜像..."
docker build --platform linux/$DOCKER_ARCH -t $BINARY_NAME .

echo "==> 清理二进制文件..."
rm -f "$BINARY_NAME-linux-$DOCKER_ARCH"

echo "==> 完成! 镜像: $BINARY_NAME"
echo "    运行: docker run -d -p 8080:8080 -v \$(pwd)/config.toml:/app/config.toml:ro $BINARY_NAME"
