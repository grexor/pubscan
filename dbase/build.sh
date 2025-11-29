#!/usr/bin/env bash

set -e

IMAGE_NAME="dbase_expressrna"
TAG="latest"

echo "Building Docker image: ${IMAGE_NAME}:${TAG}"

docker build -t "${IMAGE_NAME}:${TAG}" .

echo ""
echo "Done!"
echo "Run it with:"
echo "  docker run -p 8080:80 ${IMAGE_NAME}:${TAG}"

