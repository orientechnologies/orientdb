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
                sh "${mvnHome}/bin/mvn  -B  clean package  -Dmaven.test.failure.ignore=true"
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

                sh "${mvnHome}/bin/mvn -B clean compile  -Dmaven.test.failure.ignore=true"
            }
        }
)

stage 'Build docker container wit OrientDB inside'
node("master") {
    sh "rm -rf *"

    dir("source") {
        unstash 'source'
    }
    unstash 'orientdb-tgz'
    sh "cp target/orientdb-community-*.tar.gz source/distribution/docker/"

    def odb = docker.build("orientdb/orientdb-develop:" + env.BUILD_ID, "source/distribution/docker")

}

stage 'Run CI profile, crash tests and distributed tests on java8'
parallel(
        ci: {
            timeout(time: 180, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    sh "${mvnHome}/bin/mvn  -B -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean install"
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
                        sh "${mvnHome}/bin/mvn  -B -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test"
                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                    }
                }
            }
        }
        distributed: {
            timeout(time: 60, unit: 'MINUTES') {
                node("openjdk-8-slave") {
                    sh "rm -rf *"
                    unstash 'source'
                    def mvnHome = tool 'mvn'
                    dir('distributed') {
                        sh "${mvnHome}/bin/mvn  -B -Dmaven.test.failure.ignore=true  clean install "
                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                    }
                }
            }
        }
)

stage("Run JsClient test")
node("node-0.10-slave") {
    git url: 'https://github.com/orientechnologies/orientjs.git', branch: '2.2.x'
    sh "npm install"
    sh "npm test"
}
