sudo docker login -u abhinavabcd

DOCKER_EXP="DOCKER_BUILD_KIT=1 DOCKER_CLI_EXPERIMENTAL=enabled"

sudo $DOCKER_EXP docker buildx create --name python3_blaster_builder
sudo $DOCKER_EXP docker buildx use python3_blaster_builder
sudo $DOCKER_EXP docker buildx inspect --bootstrap
sudo $DOCKER_EXP docker buildx build --platform linux/amd64,linux/arm64,linux/arm/v7 -t abhinavabcd/blaster-python:v1 --push .

