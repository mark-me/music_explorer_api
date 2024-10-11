# Default entrypoint as per https://github.com/nginxinc/docker-nginx/blob/master/stable/alpine/Dockerfile
# bash docker-entrypoint.sh nginx -g "daemon off;"

until curl --silent http://kna-historie:5000/health; do
    echo "App not yet healthy. Waiting ...";
    sleep 1s;
done

curl -v http://kna-historie:5000/health
echo "App healthy. Starting nginx...";

while true; do
    sleep 6h & wait ${!};
    nginx -s reload;
done & \
bash docker-entrypoint.sh nginx -g "daemon off;"
