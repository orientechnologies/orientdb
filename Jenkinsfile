#!groovy
node("master") {
    properties([[$class  : 'BuildDiscarderProperty',
                 strategy: [$class              : 'LogRotator', artifactDaysToKeepStr: '',
                            artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '10']]])

    milestone()
    lock(resource: "${env.BRANCH_NAME}", inversePrecedence: true) {
        ansiColor('xterm') {
            milestone()
            def mvnHome = tool 'mvn'
            def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"
            def mvnJdk7Image = "orientdb/mvn-gradle-zulu-jdk-7"

            stage('Source checkout') {
                checkout scm
            }

            try {
                stage('Run tests on Java7') {
                    docker.image("${mvnJdk7Image}")
                            .inside("--memory=4g ${env.VOLUMES}") {
                        try {
                            //skip integration test for now
                            sh "${mvnHome}/bin/mvn -V  -fae clean   install   -Dsurefire.useFile=false -DskipITs"
                            sh "${mvnHome}/bin/mvn -f distribution/pom.xml clean"
                            sh "${mvnHome}/bin/mvn  --batch-mode -V deploy -DskipTests -DskipITs"
                        } finally {
                            junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                        }
                    }
                }

                stage('Publish Javadoc') {
                    docker.image("${mvnJdk8Image}")
                            .inside("--memory=1g ${env.VOLUMES}") {
                        sh "${mvnHome}/bin/mvn  javadoc:aggregate"
                        sh "rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e ${env.RSYNC_JAVADOC}/${env.BRANCH_NAME}/"
                    }
                }

                stage("Downstream projects") {
                    build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-teleporter-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "spring-data-orientdb-multibranch/${env.BRANCH_NAME}", wait: false
                }

                slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

            } catch (e) {
                currentBuild.result = 'FAILURE'
                slackSend(channel: '#jenkins-failures', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) ${e}")
                throw e;
            }
        }

    }
}
