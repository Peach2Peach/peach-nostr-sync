FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

RUN chmod +x /app/entrypoint.sh

RUN mkdir -p /app/log
RUN touch /app/log/app.log && chmod 0644 /app/log/app.log

ENTRYPOINT ["/app/entrypoint.sh"]
