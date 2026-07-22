############################################################
# Dockerfile to run an OrientDB (Graph) Container
# for current developed version
############################################################

FROM azul/zulu-openjdk:8

MAINTAINER OrientDB LTD (info@orientdb.com)

RUN mkdir /orientdb

COPY orientdb-community-*.tar.gz /orientdb-community.tar.gz

RUN tar -xvzf orientdb-community.tar.gz -C /orientdb --strip-components=1\
      && rm orientdb-community.tar.gz

ENV PATH /orientdb/bin:$PATH

VOLUME ["/orientdb/backup", "/orientdb/databases", "/orientdb/config"]

WORKDIR /orientdb

#OrientDb binary
EXPOSE 2424

#OrientDb http
EXPOSE 2480

# Default command start the server
CMD ["server.sh"]

