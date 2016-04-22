############################################################
# Dockerfile to run an OrientDB (Graph) Container
############################################################

FROM java:8-jdk

MAINTAINER OrientDB LTD (info@orientdb.com)

ENV ORIENTDB_VERSION 3.0.0-SNAPSHOT


#download distribution tar, untar and delete databases
RUN mkdir /orientdb

ADD target/orientdb-community-${ORIENTDB_VERSION}-distribution.dir/orientdb-community-${ORIENTDB_VERSION} /orientdb/

ENV PATH /orientdb/bin:$PATH

VOLUME ["/orientdb/backup", "/orientdb/databases", "/orientdb/config"]

WORKDIR /orientdb

#OrientDb binary
EXPOSE 2424

#OrientDb http
EXPOSE 2480

# Default command start the server
CMD ["server.sh"]
