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
    && rm -rf /var/lib/apt/lists/* \
    # Install rustup and compile maccoys
    && curl https://sh.rustup.rs -sSf | bash -s -- -y \
    && cargo install --path . --root /usr/local \
    # Install Python package
    && pip install . \
    # Cleanup python
    && pip cache purge \
    # Cleanup rust
    && cargo clean -r \
    && cargo clean \
    && rustup self uninstall -y \
    # Cleanup apt
    && apt-get purge -y \
    build-essential \
    curl \
    pkg-config \
    && apt-get autoclean -y \
    && apt-get autoremove -y --purge \
    && rm -rf ./*

ENV PATH=/usr/local/bin:/usr/local/bin:$PATH

ENTRYPOINT [ "maccoys" ]
