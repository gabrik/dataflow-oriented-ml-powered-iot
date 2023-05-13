
#!/bin/bash
sg docker -c "docker build ./ -f ./Dockerfile -t gabrik91/house --no-cache" --oom-kill-disable