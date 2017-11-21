#!groovy
node("master") {
    
    properties([
            pipelineTriggers([cron('H H * * H(6-7)')]),
            [$class: 'BuildDiscarderProperty', 
                 strategy: [$class: 'LogRotator', artifactDaysToKeepStr: '', 
                            artifactNumToKeepStr: '', daysToKeepStr: '', numToKeepStr: '10']]
    ])

    
    ansiColor('xterm') {

        def mvnHome = tool 'mvn'
        def mvnJdk8Image = "orientdb/mvn-gradle-zulu-jdk-8"
        
        def containerName = env.JOB_NAME.replaceAll(/\//, "_") + 
            "_build_${currentBuild.number}"
			
        def appNameLabel = "docker_ci";
        def taskLabel = env.JOB_NAME.replaceAll(/\//, "_")


        stage('Source checkout') {

            checkout scm
        }

        stage('Run tests on Java8') {
            docker.image("${mvnJdk8Image}").inside("--label collectd_docker_app=${appNameLabel} --label collectd_docker_task=${taskLabel} " + 
                                                   "--name ${containerName} --memory=4g ${env.VOLUMES}") {
                try {
                    sh "${mvnHome}/bin/mvn  --batch-mode -V -U -e -fae clean jacoco:prepare-agent deploy -Dsurefire.useFile=false"

                    slackSend(color: '#00FF00', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")

                } catch (e) {
                    currentBuild.result = 'FAILURE'
                    slackSend(channel: '#jenkins-failures', color: '#FF0000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
                    throw e;
                } finally {
                    junit allowEmptyResults: true, testResults: '**/target/surefire-reports/TEST-*.xml'
                    jacoco(execPattern: '**/*.exec')

                }
            }
        }
    }

}


