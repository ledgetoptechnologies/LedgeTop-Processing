FROM python:3.13.16-slim-trixie@sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLCONFIGDIR=/tmp/treedar-matplotlib \
    HOME=/home/treedar

# Native numeric/geospatial wheels need C++, OpenMP and Expat runtimes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libstdc++6 libgomp1 libexpat1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/treedar
COPY requirements-test.lock ./
RUN python -m pip install --only-binary=:all: -r requirements-test.lock

COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN python -m pip install --no-deps --no-build-isolation . \
    && groupadd --gid 10001 treedar \
    && useradd --uid 10001 --gid treedar --create-home treedar \
    && mkdir -p /data /runs \
    && chown treedar:treedar /runs

USER 10001:10001
WORKDIR /runs
ENTRYPOINT ["treedar"]
CMD ["--help"]
