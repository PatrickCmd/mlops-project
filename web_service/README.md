# Deploying a model as web-service with docker

## Packaging the app to docker

```
docker build -t ride-duration-prediction-service:v1 .
```

## Running the docker container service with logs

Copy the `.env_exampple` file to `.env` and enter the corresponding correct values

```
cp .env_example .env
```


```
docker run --env-file .env -it \
    --rm -v $(pwd):/app \
    -p 9696:9696  ride-duration-prediction-service:v1
```

## Running the docker container service in detached mode

```
docker run --env-file .env -d -it \
    --rm -v $(pwd):/app \
    -p 9696:9696  ride-duration-prediction-service:v1
```

## Test the web-service

```
python test.py
```

## Run all the above steps in one command

```
make build_webservice
```