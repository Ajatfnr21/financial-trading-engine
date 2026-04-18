FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY engine/ ./engine/
COPY tests/ ./tests/

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "engine.main:app", "--host", "0.0.0.0", "--port", "8000"]
