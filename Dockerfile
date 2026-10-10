FROM python:3.12.15-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /srv

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

RUN useradd --system --create-home --uid 10001 runner
USER runner

# Overridden by compose. On its own the image is the CLI.
ENTRYPOINT ["python", "-m", "device_control.cli"]
