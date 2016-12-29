#!groovy
node("master") {
    milestone()
    lock(resource: "${env.BRANCH_NAME}", inversePrecedence: true) {
        milestone()
        def mvnHome = tool 'mvn'
        def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"
        def mvnIBMJdkImage = "orientdb/jenkins-slave-ibm-jdk-8"

        stage('Source checkout') {
            checkout scm
        }


        try {

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

            stage("Run downstream projects") {

                build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-enterprise-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-teleporter-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "spring-data-orientdb-multibranch/${env.BRANCH_NAME}", wait: false
            }

            stage('Run CI tests on java8') {
                timeout(time: 300, unit: 'MINUTES') {
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

//            stage('Run crash tests on java8') {
//
//                try {
//                    timeout(time: 240, unit: 'MINUTES') {
//                        docker.image("${mvnJdk8Image}")
//                                .inside("${env.VOLUMES}") {
//                            sh "${mvnHome}/bin/mvn -f ./server/pom.xml  --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean test-compile failsafe:integration-test -Dsurefire.useFile=false"
//                        }
//                    }
//
//                } catch (e) {
//                    currentBuild.result = 'FAILURE'
//
//                    slackSend(color: 'bad', message: "FAILED crash tests: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
//                } finally {
//                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
//
//                }
//            }
//
//            stage('Run distributed test on Java8') {
//
//                try {
//                    timeout(time: 180, unit: 'MINUTES') {
//                        docker.image("${mvnJdk8Image}")
//                                .inside("${env.VOLUMES}") {
//                            sh "${mvnHome}/bin/mvn -f ./distributed/pom.xml --batch-mode -V -U -e -Dmaven.test.failure.ignore=true  clean package  -Dsurefire.useFile=false -DskipTests=false"
//
//                        }
//                    }
//                } catch (e) {
//                    currentBuild.result = 'FAILURE'
//
//                    slackSend(color: 'bad', message: "FAILED distributed tests: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
//                } finally {
//                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
//
//                }
//
//            }

            if (currentBuild.previousBuild == null || currentBuild.previousBuild.result != currentBuild.result) {
                slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }

        } catch (e) {
            currentBuild.result = 'FAILURE'
            if (currentBuild.previousBuild == null || currentBuild.previousBuild.result != currentBuild.result) {
                slackSend(color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
            }
            throw e;
        }
    }

}
