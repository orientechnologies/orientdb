export MAVEN_OPTS=-Xmx1G
mvn clean
cd src/site/dashboard
bower install
grunt clean --force
grunt build --force
cd ../../..
mvn install
