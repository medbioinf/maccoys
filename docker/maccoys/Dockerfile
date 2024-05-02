FROM rust:1.77

WORKDIR /usr/src/maccoys-repo/maccoys
COPY ./maccoys .
COPY ./Readme.md ..

RUN apt update \
    && apt install -y libhdf5-dev libssl-dev jq \
    && apt clean

RUN cargo install --path .

CMD ["maccoys"]