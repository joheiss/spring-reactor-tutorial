# create docker image
docker build -t ext-service:1.0 .

# run docker container
docker run -d -p 7070:7070 ext-service:1.0