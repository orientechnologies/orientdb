#!groovy
node("master") {
    properties([[$class  : 'BuildDiscarderProperty',
                 strategy: [$class              : 'LogRotator', artifactDaysToKeepStr: '',
                            artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '10']]])

    milestone()
    lock(resource: "${env.BRANCH_NAME}", inversePrecedence: true) {
        ansiColor('xterm') {
            milestone()
            def mvnHome = tool 'mvn'
            def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"

            def containerName = env.JOB_NAME.replaceAll(/\//, "_") +
                    "_build_${currentBuild.number}"

            def appNameLabel = "docker_ci";
            def taskLabel = env.JOB_NAME.replaceAll(/\//, "_")


            stage('Source checkout') {
                checkout scm
            }

            try {

                stage('Run tests on Java8') {
                    lock("label": "memory", "quantity": 6) {
                        docker.image("${mvnJdk8Image}").inside("--label collectd_docker_app=${appNameLabel} --label collectd_docker_task=${taskLabel} "
                                + "--name ${containerName} --memory=6g ${env.VOLUMES} --cap-add=SYS_PTRACE") {
                            try {
                                //skip integration test for now
                                sh "${mvnHome}/bin/mvn -V  -fae clean install   -Dsurefire.useFile=false -DskipITs"
                                //clean distribution to enable recreation of databases
                                sh "${mvnHome}/bin/mvn -f distribution/pom.xml clean"
                                sh "${mvnHome}/bin/mvn -f distribution-tp2/pom.xml clean"
                                sh "${mvnHome}/bin/mvn clean deploy -DskipTests -DskipITs"
                            } finally {
                                junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'

                            }
                        }
                    }
                }

                stage('Run QA/Integration tests on Java8') {
                    lock("label": "memory", "quantity": 5) {
                        docker.image("${mvnJdk8Image}").inside("--label collectd_docker_app=${appNameLabel} --label collectd_docker_task=${taskLabel} " +
                                "--name ${containerName} --memory=5g ${env.VOLUMES}") {
                            try {
                                sh "${mvnHome}/bin/mvn -f distribution/pom.xml clean install -Pqa"
                            } finally {
                                junit allowEmptyResults: true, testResults: '**/target/failsafe-reports/TEST-*.xml'

                            }
                        }
                    }
                }

                stage('Publish Javadoc') {
                    lock("label": "memory", "quantity": 2) {
                        docker.image("${mvnJdk8Image}").inside("--label collectd_docker_app=${appNameLabel} --label collectd_docker_task=${taskLabel} " +
                                "--name ${containerName} --memory=2g ${env.VOLUMES}") {
                            sh "${mvnHome}/bin/mvn  javadoc:aggregate"
                            sh "rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e ${env.RSYNC_JAVADOC}/${env.BRANCH_NAME}/"
                        }
                    }
                }

                stage("Downstream projects") {
                    build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}", wait: false
                    //excluded: too long
                    //build job: "orientdb-enterprise-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-teleporter-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "spring-data-orientdb-multibranch/${env.BRANCH_NAME}", wait: false
                }

                slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

            } catch (e) {
                currentBuild.result = 'FAILURE'
                slackSend(channel: '#jenkins-failures', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
                throw e;
            }
        }
    }
}
