#!/usr/bin/env bash

set -e

IMAGE_NAME="pubscan"
TAG="latest"

echo "Building Docker image: ${IMAGE_NAME}:${TAG}"

docker build -t "${IMAGE_NAME}:${TAG}" .

echo ""
echo "Done!"
