docker rm -f expressrna 2>/dev/null || true

rm -rf logs run
mkdir -p logs run

docker run \
    --name expressrna \
    --user $(id -u):$(id -g) \
    --init \
    --sig-proxy=true \
    -p 8007:80 \
    -v $(pwd)/..:/var/www/site \
    dbase_expressrna:latest

