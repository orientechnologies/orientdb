#!groovy
node("master") {
    def mvnHome = tool 'mvn'
    def mvnJdk8Image = "orientdb/mvn-zulu-jdk-8:20161124"
    def mvnIBMJdkImage = "orientdb/jenkins-slave-ibm-jdk-8:20161027"

    stage('Source checkout') {

        checkout scm
    }

    try {
        stage('Run tests on Java8') {
            docker.image("${mvnJdk8Image}")
                    .inside("${env.VOLUMES}") {

                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  deploy -DskipTests"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
            }
        }


        stage('Run tests on IBM Java8') {
            docker.image("${mvnIBMJdkImage}")
                    .inside("${env.VOLUMES}") {

                sh "${mvnHome}/bin/mvn  --batch-mode -V -U  clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
            }
        }


        stage('Run CI tests on java8') {
            timeout(time: 180, unit: 'MINUTES') {
                docker.image("${mvnJdk8Image}")
                        .inside("${env.VOLUMES}") {
                    sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean package -Dsurefire.useFile=false"
                    step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                }
            }
        }

        stage('Publish Javadoc') {
            docker.image("${mvnJdk8Image}")
                    .inside("${env.VOLUMES}") {
                sh "${mvnHome}/bin/mvn  javadoc:aggregate"
                sh "rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e ${env.RSYNC_JAVADOC}/${env.BRANCH_NAME}/"
            }
        }



        stage('Run crash tests on java8') {

            timeout(time: 60, unit: 'MINUTES') {
                docker.image("${mvnJdk8Image}")
                        .inside("${env.VOLUMES}") {
                    dir('server') {
                        sh "${mvnHome}/bin/mvn   --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
                        step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                    }
                }
            }
        }

        stage('Run distributed test on Java8') {

            timeout(time: 60, unit: 'MINUTES') {
                docker.image($ { mvnJdk8Image })
                        .inside("${env.VOLUMES}") {
                    dir('distributed') {
                        try {
                            sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean package  -Dsurefire.useFile=false"
                            step([$class: 'JUnitResultArchiver', testResults: '**/target/surefire-reports/TEST-*.xml'])
                        } catch (e) {
                            slackSend(color: 'bad', message: "FAILED Distributed tests: Job '${env.JOB_NAME}-${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                        }
                    }
                }
            }

        }

        slackSend(color: 'good', message: "SUCCESSFUL: Job '${env.JOB_NAME}-${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

    } catch (e) {
        slackSend(color: 'bad', message: "FAILED: Job '${env.JOB_NAME}-${env.BRANCH_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
    }

}

