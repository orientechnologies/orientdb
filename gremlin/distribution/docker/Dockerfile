############################################################
# Dockerfile to run an OrientDB (Graph) Container
# for current developed version
############################################################

FROM azul/zulu-openjdk:8

MAINTAINER OrientDB LTD (info@orientdb.com)

RUN mkdir /orientdb

COPY orientdb-tp3-*.tar.gz /orientdb-tp3.tar.gz

RUN tar -xvzf orientdb-tp3.tar.gz -C /orientdb --strip-components=1\
      && rm orientdb-tp3.tar.gz

#overwrite gremlin server config
ADD gremlin-server.yaml /orientdb/config/

ENV PATH /orientdb/bin:$PATH

VOLUME ["/orientdb/backup", "/orientdb/databases", "/orientdb/config"]

WORKDIR /orientdb

#OrientDb binary
EXPOSE 2424

#OrientDb http
EXPOSE 2480

#gremlin
EXPOSE 8182

# Default command start the server
CMD ["server.sh"]

