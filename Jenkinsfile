#!groovy
node("openjdk-8-slave") {
    stage 'Source checkout'

    checkout scm
    stash name: 'source', excludes: 'target/', includes: '**'

    stage 'Run tests on Java8'
    docker.image("orientdb/jenkins-slave-zulu-jdk-8:20160510").inside() {
        sh "rm -rf *"
        unstash 'source'

        def mvnHome = tool 'mvn'
        sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])

        dir('distribution') {
            stash name: 'orientdb-tgz', includes: 'target/orientdb-community-*.tar.gz'
        }


    }
}

