#!groovy
node("master") {
    ansiColor('xterm') {

        def mvnHome = tool 'mvn'
        def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

        stage('Source checkout') {

            checkout scm
        }

        stage('Run tests on Java8') {
            docker.image("${mvnJdk8Image}").inside("--memory=4g ${env.VOLUMES}") {
                try {

                    sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install -Dsurefire.useFile=false"
                    slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

                } catch (e) {
                    currentBuild.result = 'FAILURE'
                    slackSend(channel: '#jenkins-failures', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
                    throw e;
                } finally {
                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
                }
            }
        }
    }
}
