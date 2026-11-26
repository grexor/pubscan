docker rm -f pubscan3 2>/dev/null || true

rm -rf logs run
mkdir -p logs run

# the container runs on the time zone of the host, so that the times in its logs (search.log, access.log) are local
# time; without this it runs on UTC
HOST_TZ=$(cat /etc/timezone 2>/dev/null || timedatectl show -p Timezone --value 2>/dev/null || echo UTC)

# the container only listens on localhost: all traffic comes through the host Apache reverse proxy (TLS, X-Forwarded-For)
docker run \
    -d \
    --name pubscan3 \
    --restart unless-stopped \
    --user $(id -u):$(id -g) \
    --init \
    --sig-proxy=true \
    -e TZ="${HOST_TZ}" \
    -p 127.0.0.1:8008:80 \
    -v $(pwd)/..:/var/www/site \
    pubscan3:latest

