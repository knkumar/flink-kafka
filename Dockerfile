FROM python:3.12-slim

WORKDIR /workspace
COPY . /workspace

ENV PYTHONPATH=/workspace/src
CMD ["./scripts/run-local-check.sh"]
