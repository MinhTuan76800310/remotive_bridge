FROM python:3.12-slim AS base

RUN pip install uv==0.9.2

CMD ["placeholder_override", "--allow-root"]


FROM base AS model
ARG MODEL_PATH

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=models/${MODEL_PATH}/uv.lock,target=/uv.lock,readonly \
    --mount=type=bind,source=models/${MODEL_PATH}/pyproject.toml,target=/pyproject.toml,readonly \
    uv sync --locked --no-install-project --no-dev

COPY models/${MODEL_PATH}/ .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

ENV PATH="/.venv/bin:$PATH"

CMD ["placeholder_override", "--allow-root"]
