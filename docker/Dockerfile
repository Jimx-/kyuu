FROM debian:buster

RUN apt-get update && \
    apt-get install -y curl git

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

RUN curl -sSL https://get.haskellstack.org/ | sh

ENV PATH="/root/.cargo/bin/:/root/.local/bin/:${PATH}"

RUN git clone https://github.com/Jimx-/kyuu.git && \
    cd kyuu && \
    git submodule update --init && \
    cd lib/suziQ && \
    cargo build --release && \
    cd ../.. && \
    stack build

WORKDIR /kyuu

CMD ["stack", "run"]