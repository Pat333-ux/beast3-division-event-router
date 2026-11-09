FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY router.py .

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "router:app", "--host", "0.0.0.0", "--port", "8080"]
