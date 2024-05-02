FROM mambaorg/micromamba:1.5.1-jammy

WORKDIR /home/mambauser
COPY --chown=mambauser:mambauser py_src/ py_src/
COPY --chown=mambauser:mambauser pyproject.toml .
COPY --chown=mambauser:mambauser environment.yml .

USER mambauser
ENV HOME /home/mambauser
ENV ENV_NAME=maccoys

RUN echo 'show_banner: false' > ~/.mambarc

RUN micromamba env create -y -f environment.yml \
    && micromamba clean --all --yes


USER root
# First is necessary for base_image to actvate the conda environment second is entrypoint
# which adds the python file to PATH
ENTRYPOINT [ "/usr/local/bin/_entrypoint.sh"]