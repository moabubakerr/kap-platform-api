# syntax=docker/dockerfile:1
# KAP platform layer: one image, four roles (migrate | api | gateway | worker).
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLATFORM_MIGRATIONS_DIR=/app/migrations \
    PLATFORM_TENANTS_FILE=/app/config/tenants.yaml
COPY --from=build /install /usr/local
WORKDIR /app
COPY migrations ./migrations
RUN useradd --system --uid 10001 platform
USER 10001
ENTRYPOINT ["python", "-m", "kap_platform"]
CMD ["api"]
