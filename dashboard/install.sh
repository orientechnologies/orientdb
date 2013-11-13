export MAVEN_OPTS=-Xmx1G
mvn clean
cd src/site/dashboard
grunt clean --force
grunt build --force
cd ../../..
mvn install
