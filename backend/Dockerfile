
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY migrations ./migrations

ENV DB_HOST=db
ENV DB_PORT=5432
ENV DB_USER=adpulse
ENV DB_PASSWORD=adpulse
ENV DB_NAME=adpulse
ENV APP_PORT=8080

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
