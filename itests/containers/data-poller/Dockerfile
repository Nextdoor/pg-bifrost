FROM alpine:3.8
RUN \
  apk update && \
  apk add python py-pip && \
  pip install --upgrade pip && \
  pip install boto3 botocore retry

ADD app /app
VOLUME /output
CMD ["sh", "/app/entrypoint.sh"]
