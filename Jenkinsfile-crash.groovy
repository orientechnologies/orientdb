#!groovy
node("master") {
    def mvnHome = tool 'mvn'
    def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

    stage('Source checkout') {

        checkout scm
    }

    stage('Run crash tests on java8') {

        try {
            timeout(time: 240, unit: 'MINUTES') {
                docker.image("${mvnJdk8Image}")
                        .inside("${env.VOLUMES}") {
                    sh "${mvnHome}/bin/mvn -f ./server/pom.xml  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
                }
            }

            if (currentBuild.previousBuild == null || currentBuild.previousBuild.result != currentBuild.result) {
                slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }

        } catch (e) {
            currentBuild.result = 'FAILURE'
            if (currentBuild.previousBuild == null || currentBuild.previousBuild.result != currentBuild.result) {
                slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }
            throw e;
        }
    }

}

