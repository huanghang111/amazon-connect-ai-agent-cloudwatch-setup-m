FROM public.ecr.aws/docker/library/python:3.11-slim
WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN useradd -m -u 1000 bedrock_agentcore
USER bedrock_agentcore

EXPOSE 8000

COPY . .

# AgentCore Observability: opentelemetry-instrument auto-loads aws-opentelemetry-distro
# (ADOT) so Strands' built-in OTEL emits spans/metrics/logs to the AgentCore-managed
# OTLP endpoint. Runtime injects the OTEL_EXPORTER_OTLP_* env vars; we only need
# the wrapper here. To opt out for a debug run, override CMD or set
# DISABLE_ADOT_OBSERVABILITY=true on the Runtime.
CMD ["opentelemetry-instrument", "python", "-m", "mcp_server"]
