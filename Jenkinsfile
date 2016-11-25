#!groovy
node("master") {
    def mvnHome = tool 'mvn'
    def mvnJdk8Image = "orientdb/mvn-zulu-jdk-8:20161124"

    stage('Source checkout') {

        checkout scm
    }

    try {
        stage('Run tests on Java8') {
            docker.image("${mvnJdk8Image}")
                    .inside("${env.VOLUMES}") {

                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
            }
        }

        slackSend(color: 'good', message: "SUCCESSFUL: Job '${env.JOB_NAME}-${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

    } catch (e) {
        slackSend(color: 'bad', message: "FAILED: Job '${env.JOB_NAME}-${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }

}

