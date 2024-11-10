FROM python:3.12.3-slim

COPY requirements.txt /requirements.txt
COPY requirements.lock /requirements.lock

RUN pip install --no-cache-dir -r /requirements.txt -c /requirements.lock

COPY app /app

EXPOSE 8080
ENTRYPOINT [ "python", "-u", "/app/main.py" ]
