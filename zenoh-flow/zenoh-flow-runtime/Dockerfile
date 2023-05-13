FROM ubuntu:jammy as build

# first let's build the runtime, the python binding and the python api
ENV DEBIAN_FRONTEND=noninteractive


RUN apt update && apt install git python3-venv python3-pip equivs musl-tools build-essential devscripts debhelper pkg-config dpkg-dev equivs libpython3-dev -y

# install rust
RUN curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y


# clone zenoh-flow runtime and zenoh-flow-python repos
RUN git clone https://github.com/gabrik/zenoh-flow-runtime /root/zenoh-flow-runtime
RUN git clone https://github.com/eclipse-zenoh/zenoh-flow-python /root/zenoh-flow-python

# -b feat-typed-inputs-outputs

# build zenoh-flow-runtime
RUN bash -c "source /root/.cargo/env && cd /root/zenoh-flow-runtime && cargo build --release --all-targets"

# build zenoh-flow-python
RUN bash -c "source /root/.cargo/env && cd /root/zenoh-flow-python && cargo build --release --all-targets"

# python api build
FROM quay.io/pypa/manylinux2014_x86_64 as manylinuxbuild

RUN curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain 1.66.0 -y

RUN git clone https://github.com/eclipse-zenoh/zenoh-flow-python /root/zenoh-flow-python
#-b feat-typed-inputs-outputs

# build zenoh-flow-python apis
RUN bash -c "source /root/.cargo/env && cd /root/zenoh-flow-python && python3.7 -m venv venv && source venv/bin/activate && cd zenoh-flow-python && pip3 install -r requirements-dev.txt && maturin build --release --compatibility manylinux2014"


FROM ubuntu:jammy as zenoh-flow-runtime
LABEL authors="Gabriele Baldoni"

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install python3 python3-distutils curl libpython3-dev -y

RUN curl --proto "=https" --tlsv1.2 -sSf https://bootstrap.pypa.io/get-pip.py | python3


# where python wrapper will reside
RUN mkdir -p /var/zenoh-flow/python/

VOLUME ["/var/zenoh-flow/flows"]
VOLUME ["/var/zenoh-flow/nodes"]
VOLUME ["/etc/zenoh-flow"]

# copy the runtime into bin
COPY --from=build /root/zenoh-flow-runtime/target/release/runtime /usr/local/bin

# copy python wrappers
COPY --from=build /root/zenoh-flow-python/target/release/lib*.so /var/zenoh-flow/python/

# copy python api
COPY --from=manylinuxbuild /root/zenoh-flow-python/target/wheels/*.whl /root/

RUN bash -c "pip3 install /root/*.whl"

RUN bash -c "rm /root/*.whl"

COPY ./requirements.txt .
RUN pip install -r requirements.txt

COPY start.sh /usr/bin/start.sh
RUN chmod +x /usr/bin/start.sh

# clean up
# RUN  bash -c "source /root/.cargo/env && rustup toolchain remove stable && rm -rf /root/.cargo"
# RUN  bash -c "rm -rf /root/zenoh-flow-python/venv"
RUN  DEBIAN_FRONTEND=noninteractive apt clean && apt autoclean

CMD "/usr/bin/start.sh"