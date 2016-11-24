#!groovy
def mvnHome = tool 'mvn'
def mvnJdk8Image = "orientdb/mvn-zulu-jdk-8:20161124"
node("master") {

    stage('Source checkout') {

        checkout scm
    }

    try {
        stage('Run tests on Java8') {
            docker.image($ { mvnJdk8Image })
                    .inside("${env.VOLUMES}") {

                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                slackSend(color: '#00FF00', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }

        }


        stage('Run CI profile and crash tests on java8') {
            parallel(
                    ci: {
                        timeout(time: 180, unit: 'MINUTES') {
                            docker.image($ { mvnJdk8Image })
                                    .inside("${env.VOLUMES}") {
                                sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean package -Dsurefire.useFile=false"
                                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                            }
                        }
                    },
                    crash: {
                        timeout(time: 60, unit: 'MINUTES') {
                            docker.image($ { mvnJdk8Image })
                                    .inside("${env.VOLUMES}") {
                                dir('server') {
                                    sh "${mvnHome}/bin/mvn   --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
                                    step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                                }
                            }
                        }
                    }
            )
        }

//        stage('distributed test on Java8') {
//
//            timeout(time: 60, unit: 'MINUTES') {
//                docker.image($ { mvnJdk8Image })
//                        .inside("${env.VOLUMES}") {
//                    dir('distributed') {
//                        sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean package  -Dsurefire.useFile=false"
//                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
//                    }
//                }
//            }
//
//        }
    } catch (e) {
        slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }

}

