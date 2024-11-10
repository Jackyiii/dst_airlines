FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /log

COPY . .

COPY start.sh /start.sh
RUN chmod +x /start.sh

RUN pip install --no-cache-dir -r requirements.txt

CMD ["/start.sh"]
#ENTRYPOINT ["python", "main.py"]