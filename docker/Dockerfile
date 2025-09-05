FROM rust:1.89 AS build-stage

WORKDIR /usr/src/maccoys
COPY . .

RUN cargo build -r



FROM debian:trixie-slim

COPY --from=build-stage /usr/src/maccoys/target/release/maccoys /usr/bin/maccoys

ENTRYPOINT [ "/usr/bin/maccoys" ]
