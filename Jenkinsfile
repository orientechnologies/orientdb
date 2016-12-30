#!groovy
node("master") {
    def mvnHome = tool 'mvn'
    def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

    stage('Source checkout') {

        checkout scm
    }

    stage('Run tests on Java8') {
        docker.image("${mvnJdk8Image}").inside("${env.VOLUMES}") {
            try {

                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"

            } catch (e) {
                currentBuild.result = 'FAILURE'
                if (currentBuild.previousBuild == null || currentBuild.previousBuild.result != currentBuild.result) {
                    slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                }
                throw e;
            }

        }
    }
}
