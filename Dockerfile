ARG ALPINE_VERSION=3.18.4

FROM alpine:${ALPINE_VERSION} AS base

USER root

ARG PYTHON_VERSION=3.11.8-r0
ARG JQ_VERSION=1.6

# System modules
RUN apk update && apk upgrade
RUN apk add --no-cache \
    openssl \
    expat \
    sqlite \
    bash \
    curl \
    sudo \
    krb5 \
    python3=${PYTHON_VERSION} \
    py3-pip-23.1.2-r0

# Python libraries
RUN pip3 install --upgrade pip && pip3 install --no-cache-dir \
    boto3 \
    prometheus-client

# Additional binaries
RUN curl -Lo /usr/local/bin/jq https://github.com/jqlang/jq/releases/download/jq-${JQ_VERSION}/jq-linux64 \
    && chmod +x /usr/local/bin/jq

# Create user
RUN apk add --no-cache tzdata \
    && cp /usr/share/zoneinfo/Asia/Singapore /etc/localtime

RUN adduser -D -u 2001 adak8s wheel
RUN echo "" >> /etc/sudoers
RUN echo "### Removing sudo password for adak8s" >> /etc/sudoers
RUN echo "adak8s ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Change user
USER adak8s
WORKDIR /opt

COPY --chown=adak8s:adak8s src /opt/src
# COPY --chown=adak8s:adak8s data /opt/data
CMD ["python", "src/app.py"]
