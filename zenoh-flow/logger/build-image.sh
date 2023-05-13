
#!/bin/bash
sg docker -c "docker build ./ -f ./Dockerfile -t gabrik91/logger --no-cache" --oom-kill-disable