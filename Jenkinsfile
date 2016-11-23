#!groovy
node("master") {
    stage 'Source checkout'

    checkout scm
    stash name: 'source', excludes: 'target/', includes: '**'

    stage 'Run tests on Java8'
    docker.image("orientdb/jenkins-slave-zulu-jdk-8:20160510")
            .inside("-v /home/orient/jenkins/workspace:/home/jenkins/workspace:rw\n" +
            "-v /home/orient/.m2:/home/jenkins/.m2:rw\n" +
            "-v /home/orient/.ssh:/home/jenkins/.ssh:ro") {
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

