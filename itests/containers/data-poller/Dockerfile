FROM ubuntu:22.04
RUN \
  apt update && \
  apt install -yf python3 python3-pip && \
  pip3 install boto3 botocore retry confluent-kafka

ADD app /app
VOLUME /output
CMD ["sh", "/app/entrypoint.sh"]
