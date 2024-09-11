FROM python:3.8-slim

COPY requirements.txt .

RUN pip install -r requirements.txt --no-cache-dir && \
    mkdir "/app"

WORKDIR /app

#ADD /src /app/src

ENTRYPOINT ["tail", "-f", "/dev/null"]