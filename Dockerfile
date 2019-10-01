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

RUN java -version
RUN mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

CMD ./cmd.sh