# zenoh-eval


## MQTT
### To run the mqtt program first build the python image (on the mqtt folder)

`docker image build -t python_runner .`

### Then run the docker compose (on the mqtt folder)

`docker compose up`


# Zenoh + Zenoh-Flow
### To run the zenoh+zenoh-flow program first build the images:

```
cd zenoh-flow/zenoh-flow-runtime
./build-image.sh
cd ../house
./build-image.sh
```

### Then run the docker compose:

```
cd zenoh-flow
docker compose up
```
