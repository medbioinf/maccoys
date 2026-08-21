FROM rust:1.95 AS build-stage

WORKDIR /usr/src/maccoys
COPY . .

RUN cargo build -r --package maccoys



FROM debian:trixie-slim

COPY --from=build-stage /usr/src/maccoys/target/release/maccoys /usr/bin/maccoys

ENTRYPOINT [ "/usr/bin/maccoys" ]
