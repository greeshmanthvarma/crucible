# Build from repository root. Both build inputs must be digest-pinned; pass the
# resulting worker image by digest to CRUCIBLE_REPAIR_WORKER_IMAGE.
ARG BASE_IMAGE
ARG UV_IMAGE
FROM ${UV_IMAGE} AS uv
FROM ${BASE_IMAGE}
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin \
    PYTHONDONTWRITEBYTECODE=1
COPY backend/pyproject.toml backend/uv.lock /build/
RUN uv sync --project /build --frozen --no-dev --no-install-project \
    && rm -rf /root/.cache /build /usr/local/bin/uv
USER 65532:65532
WORKDIR /workspace
ENTRYPOINT []
