#!groovy
node("master") {
    def mvnHome = tool 'mvn'
    def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

    stage('Source checkout') {

        checkout scm
    }

    stage('Run distributed test on Java8') {

        try {
            timeout(time: 180, unit: 'MINUTES') {
                docker.image("${mvnJdk8Image}")
                        .inside("${env.VOLUMES}") {
                    sh "${mvnHome}/bin/mvn -f ./distributed/pom.xml --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean package  -Dsurefire.useFile=false -DskipTests=false"

                }
            }
        } catch (e) {
            currentBuild.result = 'FAILURE'

            slackSend(color: 'bad', message: "FAILED distributed tests: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        } finally {
            junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

        }

    }

}

