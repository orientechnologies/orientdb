#!groovy
node("master") {
    milestone()
    lock(resource: "${env.BRANCH_NAME}", inversePrecedence: true) {
        ansiColor('xterm') {
            milestone()
            def mvnHome = tool 'mvn'
            def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

            stage('Source checkout') {
                checkout scm
            }

            try {

                stage('Run tests on Java8') {
                    docker.image("${mvnJdk8Image}").inside("${env.VOLUMES}") {
                        try {
                            //skip integration test for now
                            sh "${mvnHome}/bin/mvn -V  -fae clean install   -Dsurefire.useFile=false -DskipITs"
                            //clean distribution to enable recreation of databases
                            sh "${mvnHome}/bin/mvn -f distribution/pom.xml clean"
                            sh "${mvnHome}/bin/mvn -f distribution-tp2/pom.xml clean"
                            sh "${mvnHome}/bin/mvn deploy -DskipTests -DskipITs"
                        } finally {
                            junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                        }
                    }
                }

                stage('Run QA/Integration tests on Java8') {
                    docker.image("${mvnJdk8Image}").inside("${env.VOLUMES}") {
                        try {
                            sh "${mvnHome}/bin/mvn -f distribution/pom.xml clean install -Pqa"
                        } finally {
                            junit allowEmptyResults: true, testResults: '**/target/failsafe-reports/TEST-*.xml'

                        }
                    }
                }

                stage('Publish Javadoc') {
                    docker.image("${mvnJdk8Image}").inside("${env.VOLUMES}") {
                        sh "${mvnHome}/bin/mvn  javadoc:aggregate"
                        sh "rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e ${env.RSYNC_JAVADOC}/${env.BRANCH_NAME}/"
                    }
                }

                stage("Downstream projects") {

                    build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}", wait: false
                    //excluded: too long
                    //build job: "orientdb-enterprise-multibranch/${env.BRANCH_NAME}", wait: false

                    build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-teleporter-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "spring-data-orientdb-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "/develop/orientdb-gremlin-develop", wait: false
                }

                slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

            } catch (e) {
                currentBuild.result = 'FAILURE'
                slackSend(channel: '#jenkins-failures', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                throw e;
            }
        }
    }
}
