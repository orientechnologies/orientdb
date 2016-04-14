#!groovy
stage 'Source checkout'
node("openjdk-8-slave") {

    checkout scm
    stash name: 'source', excludes: 'target/', includes: '**'
}


stage 'Compile on Java7 and run tests on Java8'
parallel(
        java8: {
            node("openjdk-8-slave") {
                sh "rm -rf *"
                unstash 'source'

                def mvnHome = tool 'mvn'
                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])

                dir('distribution') {
                    stash name: 'orientdb-tgz', includes: 'target/orientdb-community-*.tar.gz'
                }
            }
        },
        java7: {
            node("openjdk-7-slave") {
                sh "rm -rf *"
                unstash 'source'
                def mvnHome = tool 'mvn'

                sh "${mvnHome}/bin/mvn --batch-mode -V -U clean compile  -Dmaven.test.failure.ignore=true"
            }
        }
)

stage 'Build docker container'
node("master") {
    sh "rm -rf *"

    dir("source") {
        unstash 'source'
    }
    unstash 'orientdb-tgz'
    sh "cp target/orientdb-community-*.tar.gz source/distribution/docker/"

    docker.build("orientdb/orientdb-${env.BRANCH_NAME}:latest", "source/distribution/docker")

}

stage("Run JsClient integration tests")
node("master") {

    def odbImg = docker.image("orientdb/orientdb-${env.BRANCH_NAME}:latest")

    def jsBuildImg = docker.image("orientdb/jenkins-slave-node-0.10:20160112")

    def branchName = $ { env.BRANCH_NAME }

    odbImg.withRun("-e ORIENTDB_ROOT_PASSWORD=root") { odb ->
        jsBuildImg.inside("-v /home/orient/.npm:/home/jenkins/.npm:rw -v /home/orient/node_modules:/home/jenkins/node_modules:rw --link=${odb.id}:odb -e ORIENTDB_HOST=odb -e ORIENTDB_BIN_PORT=2424 -e ORIENTDB_HTTP_PORT=2480") {
            git url: 'https://github.com/orientechnologies/orientjs.git', branch: $ { branchName }
            sh "npm install"
            sh "npm test"
        }
    }

}


stage 'Run CI profile, crash tests and distributed tests on java8'
parallel(
        ci: {
            timeout(time: 180, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    sh "${mvnHome}/bin/mvn  - --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean package -Dsurefire.useFile=false"
                    step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                }
            }
        },
        crash: {
            timeout(time: 60, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    dir('server') {
                        sh "${mvnHome}/bin/mvn   --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                    }
                }
            }
        },
        distributed: {
            timeout(time: 60, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    dir('distributed') {
                        sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean package  -Dsurefire.useFile=false"
                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                    }
                }
            }
        }
)


