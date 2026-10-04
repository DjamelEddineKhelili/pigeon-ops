# The Data Engineering team's container: Python + our pipeline package + the API server.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies FIRST: Docker caches this layer, so code edits don't reinstall everything.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline ./pipeline

EXPOSE 8000
CMD ["uvicorn", "pipeline.app:app", "--host", "0.0.0.0", "--port", "8000"]
