
#!/bin/bash
sg docker -c "docker build ./ -f ./Dockerfile -t gabrik91/zenoh-flow-runtime --no-cache" --oom-kill-disable