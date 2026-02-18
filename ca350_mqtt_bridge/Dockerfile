ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache \
    python3 \
    py3-virtualenv

WORKDIR /app

RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY ca350.py /app/ca350.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

CMD [ "/app/run.sh" ]