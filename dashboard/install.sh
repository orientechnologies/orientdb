export MAVEN_OPTS=-Xmx3G
mvn clean
cd src/site/dashboard
grunt build
cd ../../..
mvn install
