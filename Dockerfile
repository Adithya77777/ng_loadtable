FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

ENV FUNCTION_TARGET=read_streams_config
ENV FUNCTION_SIGNATURE_TYPE=http

RUN pip install functions-framework

EXPOSE 8080

CMD ["functions-framework", "--target=read_streams_config", "--port=8080"]

