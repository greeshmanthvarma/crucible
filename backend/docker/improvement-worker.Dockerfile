# Build only under separate concrete operator authorization. Supply digest-pinned
# PYTHON_IMAGE and UV_IMAGE, then admit the resulting image digest/platform.
ARG PYTHON_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv
FROM ${PYTHON_IMAGE}
USER root
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/runtime
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
RUN uv sync --frozen --no-dev --no-install-project
ENV TIKTOKEN_CACHE_DIR=/opt/tokenizer-cache
RUN /opt/venv/bin/python -c "import tiktoken; tiktoken.get_encoding('o200k_base')" \
    && chmod -R a+rX /opt/tokenizer-cache
RUN /opt/venv/bin/python -c "import sys; assert sys.version_info[:2] == (3, 13)"
RUN git --version
COPY src/crucible/improvement/worker_relay.py /opt/relay.py
USER 65534:65534
