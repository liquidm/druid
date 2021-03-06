# Based on https://github.com/druid-io/docker-druid

FROM ubuntu:bionic

RUN apt-get update \
      && apt-get install -y openjdk-8-jdk-headless \
                            postgresql-client \
                            supervisor \
                            git \
                            netcat \
                            curl \
                            wget \
                            python3-pip \
                            maven \
      && pip3 install wheel \
      && pip3 install pyyaml \
      && apt-get clean


WORKDIR /tmp/druid

COPY . .

RUN update-java-alternatives  -s java-1.8.0-openjdk-amd64 \
      && mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

CMD ./cmd.sh