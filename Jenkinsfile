@Library(['piper-lib', 'piper-lib-os']) _

properties([[$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', numToKeepStr: '4']]]);

node {


    stage('build')   {
        sh "rm -rf *"
        sh "cp /var/jenkins_home/uploadedContent/settings.xml ."

        dockerExecute(
        dockerImage:'ldellaquila/maven-gradle-node-zulu-openjdk8:1.1.0',
          dockerWorkspace: '/orientdb-${env.BRANCH_NAME}'
          ) {

          try{
              sh "rm -rf orientdb-studio"
              sh "rm -rf orientdb"

            // needed after the release and tag change, otherwise Studio is not found on Sonatype
              checkout(
                    [$class: 'GitSCM', branches: [[name: "develop"]],
                    doGenerateSubmoduleConfigurations: false,
                    extensions: [],
                    submoduleCfg: [],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'orientdb-studio']],
                    userRemoteConfigs: [[url: 'https://github.com/orientechnologies/orientdb-studio']]])

                checkout(
                    [$class: 'GitSCM', branches: [[name: env.BRANCH_NAME]],
                    doGenerateSubmoduleConfigurations: false,
                    extensions: [],
                    submoduleCfg: [],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: 'orientdb']],
                    userRemoteConfigs: [[url: 'https://github.com/orientechnologies/orientdb']]])


              withMaven(globalMavenSettingsFilePath: 'settings.xml') {
                  sh "cd orientdb-studio && mvn clean install -DskipTests"
                  sh "cd orientdb && mvn clean deploy"

                  //TODO publish javadoc
                  //sh "cd orientdb && mvn  javadoc:aggregate"
                  //sh "cd orientdb && rsync -ra --stats ${WORKSPACE}/target/site/apidocs/ -e ${env.RSYNC_JAVADOC}/${env.BRANCH_NAME}/"
              }

              try{
                  if (!env.BRANCH_NAME.contains("dritter")){ // TODO: add "!env.BRANCH_NAME.startsWith("WIP") &&"
                    build job: "orientdb-gremlin-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-security-multibranch/${env.BRANCH_NAME}", wait: false
                    build job: "orientdb-sap-enterprise-multibranch/${env.BRANCH_NAME}", wait: false
                  }
              } catch (ex){
                  slackSend(color: '#FFAAAA', channel: '#jenkins-failures', message: "Error scheduling downstream builds: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${ex}")
              }

          }catch(e){
              slackSend(color: '#FF0000', channel: '#jenkins-failures', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
              throw e
          }
          slackSend(color: '#00FF00', channel: '#jenkins', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
        }
    }

}
