FROM python:3.12-slim

WORKDIR /app

# Copy only what the build backend needs, so a change to tests or docs does not
# invalidate the dependency layer.
COPY pyproject.toml README.md ./
COPY src/ src/

RUN pip install --no-cache-dir .

# Ship the worked example so the image is runnable with no repo checkout and no
# mount — that is what `run_remotive_vss_bridge.sh` falls back to. It is NOT the
# default config path: nothing reads it unless a caller names it, so an operator
# who forgets to mount their mapping gets a startup error, not a silent run
# against someone else's signal names.
COPY mapping.example.yaml /usr/share/kx-vss-bridge/mapping.example.yaml

# Unprivileged. The bridge opens two outbound gRPC connections and one inbound
# HTTP port; nothing it does needs root, and it must never be able to touch the
# Docker socket even if one were mounted by mistake.
USER 65532:65532

# The health server binds this. Publishing it is the operator's choice.
EXPOSE 8090

ENTRYPOINT ["kx-vss-bridge"]
CMD ["--config", "/config/mapping.yaml"]
