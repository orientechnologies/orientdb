#!groovy
stage 'Source checkout'
node("openjdk-8-slave") {

    checkout scm
    stash name: 'source', excludes: 'target/', includes: '**'
}


stage 'Run tests on Java8'
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

stage 'Javadoc'
node("master") {
    sh "rm -rf *"
    unstash 'source'
    def mvnHome = tool 'mvn'
    sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e javadoc:aggregate"

    sh "rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e \"ssh -p 59965\" root@144.76.167.241:/vz/private/103/var/www/wordpress/javadoc/2.2.x/  "
}


stage 'Run CI profile, crash tests and distributed tests on java8'
parallel(
        ci: {
            timeout(time: 180, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean package -Dsurefire.useFile=false"
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


