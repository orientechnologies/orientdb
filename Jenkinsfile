#!groovy
node("master") {

    stage('Source checkout') {

        checkout scm
//        stash name: 'source', excludes: 'target/', includes: '**'
    }

    stage('Run tests on Java8') {
        docker.image("orientdb/mvn-zulu-jdk-8:20161124")
                .inside("${env.VOLUMES}") {

            try {

//            sh "rm -rf *"
//            unstash 'source'

                def mvnHome = tool 'mvn'
                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                slackSend color: 'good', message: """TEST::: ${env.JOB_NAME} build ${env.BUILD_NUMBER} """
            } catch (e) {

                slackSend color: 'bad', message: """TEST::: ${env.JOB_NAME} build ${env.BUILD_NUMBER} """
            }
        }

    }
}

