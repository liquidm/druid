FROM registry.build.lqm.io/java-mvn:8

RUN mkdir -p /usr/local/druid/lib \
      && mkdir -p /opt/druid/distribution

WORKDIR /tmp/druid

COPY . .

RUN mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet \
      && cp services/target/druid-services-*-selfcontained.jar /usr/local/druid/lib \
      && cp distribution/target/*.tar.gz /opt/druid/distribution \
      && cp -r distribution/target/extensions /usr/local/druid/ \
      && cp -r distribution/target/hadoop-dependencies /usr/local/druid/ \
      && apt-get purge --auto-remove -y git \
      && apt-get clean \
      && rm -rf /var/tmp/* \
                /usr/local/apache-maven-* \
                /usr/local/apache-maven \
                /root/.m2

WORKDIR /var/lib/druid
ADD . .

CMD ./cmd.sh