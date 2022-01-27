@Library(['piper-lib', 'piper-lib-os']) _

properties([[$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', numToKeepStr: '3']]]);

node {


	stage('build')   {
		sh "rm -rf *"

		dockerExecute(
				dockerImage:'ldellaquila/maven-gradle-node-zulu-openjdk8:1.1.0',
				dockerWorkspace: '/orientdb-enterprise-${env.BRANCH_NAME}'
		) {

			try{
				sh "rm -rf orientdb-studio"
				sh "rm -rf orientdb"
				sh "rm -rf teleporter"
				sh "rm -rf orientdb-neo4j-importer-plugin"
				sh "rm -rf orientdb-security"
				sh "rm -rf orientdb-gremlin"
				sh "rm -rf orientdb-enterprise-agent"


				fetch("orientdb-studio")
				fetch("orientdb")
				fetch("orientdb-neo4j-importer-plugin")
				fetch("teleporter")
				fetch("orientdb-security")
				fetch("orientdb-gremlin")
				fetchInternal("orientdb-enterprise-agent")


				sh "cd orientdb-studio && mvn clean install -DskipTests"
				sh "cd orientdb && mvn clean install -DskipTests"
				sh "cd orientdb-neo4j-importer-plugin && mvn clean install -DskipTests"
				sh "cd teleporter && mvn clean install -DskipTests"
				sh "cd orientdb-gremlin && mvn clean install -DskipTests"
				sh "cd orientdb-security && mvn clean install -DskipTests"
				sh "cd orientdb-enterprise-agent && mvn clean jacoco:prepare-agent install jacoco:report -Dstorage.wal.allowDirectIO=false"


			}catch(e){
				slackSend(color: '#FF0000', channel: '#jenkins-failures', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})\n${e}")
				throw e
			}
			slackSend(color: '#00FF00', channel: '#jenkins', message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})")
		}
	}

}

def fetch(module) {
	echo "checkout for $module"
	checkout([$class: 'GitSCM', branches: [[name: env.BRANCH_NAME]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: "${module}"]], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '38719b3f-e300-4fe5-9d50-cfe2c10ce1f0', url: "https://github.com/orientechnologies/${module}.git"]]])
	sh "git -C $module checkout ${env.BRANCH_NAME}"
}

def fetchInternal(module) {
    echo "checkout for $module"
    checkout([$class: 'GitSCM', branches: [[name: env.BRANCH_NAME]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: "${module}"]], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '38719b3f-e300-4fe5-9d50-cfe2c10ce1f0', url: "https://github.com/SAP/${module}.git"]]])
    sh "git -C $module checkout ${env.BRANCH_NAME}"
}