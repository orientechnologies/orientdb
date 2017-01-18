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
                        sh "${mvnHome}/bin/mvn  --batch-mode -V clean install   -Dsurefire.useFile=false"
                        sh "${mvnHome}/bin/mvn  --batch-mode -V deploy -DskipTests"
                    } finally {
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

            stage("Run downstream projects") {

                build job: "orientdb-spatial-multibranch/${env.BRANCH_NAME}", wait: false
                //excluded: too long
                //build job: "orientdb-enterprise-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-neo4j-importer-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "orientdb-teleporter-multibranch/${env.BRANCH_NAME}", wait: false
                build job: "spring-data-orientdb-multibranch/${env.BRANCH_NAME}", wait: false
            }


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
