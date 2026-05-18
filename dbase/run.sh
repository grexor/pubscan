docker rm -f pubscan3 2>/dev/null || true

rm -rf logs run
mkdir -p logs run

docker run \
    -d \
    --name pubscan3 \
    --restart unless-stopped \
    --user $(id -u):$(id -g) \
    --init \
    --sig-proxy=true \
    -p 8008:80 \
    -v $(pwd)/..:/var/www/site \
    pubscan3:latest

