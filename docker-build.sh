#!/bin/bash

set -e

BINARY_NAME="gptload-rs"
TARGET_DIR="target/release"

echo "==> 检查二进制文件..."
if [ ! -f "$TARGET_DIR/$BINARY_NAME" ]; then
    echo "==> 二进制文件不存在，开始编译..."
    cargo build --release
fi

echo "==> 复制二进制文件到当前目录..."
cp "$TARGET_DIR/$BINARY_NAME" .

echo "==> 构建 Docker 镜像..."
docker build -t $BINARY_NAME .

echo "==> 清理..."
rm -f $BINARY_NAME

echo "==> 完成! 镜像: $BINARY_NAME"
echo "    运行: docker run -d -p 8080:8080 -v \$(pwd)/config.toml:/app/config.toml:ro $BINARY_NAME"
