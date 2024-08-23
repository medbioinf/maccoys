FROM python:3.11-slim-bookworm

WORKDIR /usr/src/maccoys
COPY . .
ENV PATH=/usr/local/bin:/root/.cargo/bin:/usr/local/bin:$PATH

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libssl-dev \
        libhdf5-dev \
        pkg-config \
    && apt-get clean -y \
    # Install external tools
    && curl -L -o /usr/local/bin/comet https://github.com/UWPR/Comet/releases/download/v2024.01.1/comet.linux.exe \
    && chmod +x /usr/local/bin/comet \
    # Install rustup and compile maccoys
    && curl https://sh.rustup.rs -sSf | bash -s -- -y \
    && cargo install --path . --root /usr/local \
    # Install Python package
    && pip install . \
    # Cleanup
    && cargo clean \
    && apt-get purge -y \
        build-essential \
        curl \
        pkg-config \
    && apt-get autoremove -y \
    && rustup self uninstall -y 

ENTRYPOINT [ "maccoys" ]

