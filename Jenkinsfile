#!groovy
node("master") {
    lock("${env.BRANCH_NAME}") {
        def mvnHome = tool 'mvn'
        def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8:20161125"
        def mvnIBMJdkImage = "orientdb/jenkins-slave-ibm-jdk-8:20161027"

        stage('Source checkout') {
            checkout scm
        }


        try {
            stage("Run downstream projects") {

//                build job: 'orientdb-develop-ibm-jdk8'
                    build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}"
//                    build job: 'orientdb-enterprise-multibranch/${env.BRANCH_NAME}'
//                    build job: 'orientdb-security-multibranch/${env.BRANCH_NAME}'
//                    build job: 'orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}'
//                    build job: 'orientdb-teleporter-multibranch/${env.BRANCH_NAME}'
//                    build job: 'spring-data-orientdb-multibranch/${env.BRANCH_NAME}'
            }

            stage('Run tests on Java8') {
                docker.image("${mvnJdk8Image}")
                        .inside("${env.VOLUMES}") {
                    try {
                        sh "${mvnHome}/bin/mvn  --batch-mode -V clean install  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                        sh "${mvnHome}/bin/mvn  --batch-mode -V deploy -DskipTests"
                    } finally {
                        junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                    }
                }
            }


            stage('Run tests on IBM Java8') {
                docker.image("${mvnIBMJdkImage}")
                        .inside("${env.VOLUMES}") {
                    try {
                        sh "${mvnHome}/bin/mvn  --batch-mode -V  test  -Dmaven.test.failure.ignore=true -Dsurefire.useFile=false"
                    } finally {
                        junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                    }
                }
            }


            stage('Run CI tests on java8') {
                timeout(time: 180, unit: 'MINUTES') {
                    docker.image("${mvnJdk8Image}")
                            .inside("${env.VOLUMES}") {
                        sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  -Dstorage.diskCache.bufferSize=4096 -Dorientdb.test.env=ci clean package -Dsurefire.useFile=false"
                        junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
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

                try {
                    timeout(time: 240, unit: 'MINUTES') {
                        docker.image("${mvnJdk8Image}")
                                .inside("${env.VOLUMES}") {
                            sh "${mvnHome}/bin/mvn -f ./server/pom.xml  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
                        }
                    }

                } catch (e) {
                    currentBuild.result = 'FAILURE'

                    slackSend(color: 'bad', message: "FAILED crash tests: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
                } finally {
                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                }
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


        } catch (e) {
            currentBuild.result = 'FAILURE'
            slackSend(color: 'bad', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
    }
}
